// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "replication/primarybackup.h"

#include "base/assert.h"
#include "base/stlutil.h"
#include "io/buffer.h"
#include "io/message.h"

using std::string;
using std::vector;

namespace replication {

// TODO: The extra entries are wasted when we are running as a backup. Does this matter? It makes
// the primary more efficient, which is the bottleneck.
struct PrimaryBackupReplica::PendingLogEntry {
    PendingLogEntry() : callback_arg_(NULL) {}

    void clear() {
        entry_.clear();
        acks_.clear();
        callback_arg_ = NULL;
    }

    string entry_;
    vector<bool> acks_;
    void* callback_arg_;
};

// Constructor and destructor must be here because of definition of PendingLogEntry.
PrimaryBackupReplica::PrimaryBackupReplica(PrimaryBackupCommunication* communication) :
        communication_(communication), next_uncommitted_(0), next_unacknowledged_(0),
        primary_next_uncommitted_(0), primary_next_unacknowledged_(0),
        num_replicas_(0), is_primary_(false), serialization_buffer_(new io::FIFOBuffer()) {
    assert(communication_ != NULL);
}

PrimaryBackupReplica::~PrimaryBackupReplica() {
    delete serialization_buffer_;
}

void PrimaryBackupReplica::setPrimary(int num_replicas) {
    assert(num_replicas > 1);
    assert(num_replicas_ == 0);
    assert(!is_primary_);

    num_replicas_ = num_replicas;
    is_primary_ = true;
}

void PrimaryBackupReplica::setBackup() {
    assert(num_replicas_ == 0);

    num_replicas_ = 1;
    is_primary_ = false;
}

void PrimaryBackupReplica::flush() {
    communication_->flush();
}

int PrimaryBackupReplica::submit(const io::Message& entry, void* callback_arg) {
    assert(is_primary_);
    assert(num_replicas_ > 1);

    // record the entry and a bit vector for recording acknowledgements
    size_t sequence = log_.nextIndex();
    PendingLogEntry* pending = log_.add();
    // TODO: Serialize directly to an io::FIFOBuffer? Requires changes on the nextLogEntry end.
    assert(serialization_buffer_->available() == 0);
    entry.serialize(serialization_buffer_);
    assert(serialization_buffer_->available() > 0);
    serialization_buffer_->copyOut(&pending->entry_, serialization_buffer_->available());
    assert(pending->acks_.empty());
    pending->acks_.resize(num_replicas_ - 1);  // do not count ourselves
    pending->callback_arg_ = callback_arg;

    // send the entry to all replicas
    Entry msg;
    msg.sequence_ = sequence;
    msg.entry_ = pending->entry_;
    assert(!msg.entry_.empty());
    msg.next_uncommitted_sequence_ = next_uncommitted_;
    msg.next_unacknowledged_sequence_ = next_unacknowledged_;

    for (int i = 1; i < num_replicas_; ++i) {
        communication_->send(i, msg);
    }

    return (int) sequence;
}

void PrimaryBackupReplica::setCallbackArg(int sequence, void* callback_arg) {
    PendingLogEntry* pending = log_.mutableAt(sequence);
    pending->callback_arg_ = callback_arg;
}

bool PrimaryBackupReplica::isPrimary() const {
    assert(num_replicas_ > 0);
    return is_primary_;
}

void PrimaryBackupReplica::receive(int replica_index, const Ack& ack) {
    assert(is_primary_);
    // We cannot received acks for entries we have not sent yet
    assert(0 <= ack.sequence_ && ack.sequence_ < log_.nextIndex());
    assert(1 <= replica_index && replica_index < num_replicas_);

    if (ack.sequence_ < next_unacknowledged_) {
        // ignore this: it is an old ack
        return;
    }

    size_t sequence = static_cast<size_t>(ack.sequence_);
    PendingLogEntry* pending = log_.mutableAt(sequence);
    if (pending->acks_[replica_index-1]) {
        // this message was already acknowledged: ignore
        return;
    }
    pending->acks_[replica_index-1] = true;

    // if this entry is the next uncommitted, the next unacknowledged, or both, search forward
    // from that point, committing entries or forgetting entries, as desired.
    bool did_something = true;
    while (did_something &&
            (sequence == next_uncommitted_ || sequence == next_unacknowledged_)
            && sequence < log_.nextIndex()) {
        did_something = false;

        // Count the number of acks
        const PendingLogEntry& next_entry = log_.at(sequence);

        // TODO: Use a real bit set with a bit count operation?
        int acks = 0;
        for (size_t i = 0; i < next_entry.acks_.size(); ++i) {
            if (next_entry.acks_[i]) {
                acks += 1;
            }
        }
        assert(0 <= acks && acks <= num_replicas_-1);

        // TODO: Policy choice: ack all or majority?
        if (sequence == next_uncommitted_ && acks == num_replicas_-1) {
            // commit this sequence
            assert(next_uncommitted_ < log_.nextIndex());
            
            nextLogEntryCallback(
                    (int) sequence, next_entry.entry_, next_entry.callback_arg_);
            next_uncommitted_ += 1;
            did_something = true;
        }

        if (sequence == next_unacknowledged_ && acks == num_replicas_-1) {
            // all replicas have received this entry: it can be forgotten
            assert(next_unacknowledged_ < next_uncommitted_);
            assert(next_unacknowledged_ == log_.firstIndex());
            log_.pop_front();
            next_unacknowledged_ += 1;
            did_something = true;
        }

        // Check the next entry
        sequence += 1;
    }
}


void PrimaryBackupReplica::receive(int replica_index, const Entry& entry) {
    assert(!is_primary_ && num_replicas_ == 1);
    assert(replica_index == 0);
    assert(entry.next_uncommitted_sequence_ >= 0);
    assert(entry.next_unacknowledged_sequence_ >= 0);

    // unacknowledged <= committed <= sequence
    assert(entry.next_unacknowledged_sequence_ <= entry.next_uncommitted_sequence_);

    // if we acked something, we must have it in our log
    assert(entry.next_unacknowledged_sequence_ <= log_.nextIndex());
    assert(entry.next_unacknowledged_sequence_ == 0 ||
            ((log_.empty() && entry.next_unacknowledged_sequence_ == log_.nextIndex())
            || entry.next_unacknowledged_sequence_-1 < log_.firstIndex()
            || !log_.at((size_t) entry.next_unacknowledged_sequence_-1).entry_.empty()));

    if (entry.sequence_ != Entry::INVALID_SEQUENCE) {
        // unacknowledged *must* be <= sequence, otherwise it wouldn't get sent!
        assert(entry.next_unacknowledged_sequence_ <= entry.sequence_);

        assert(!entry.entry_.empty());
        if (entry.sequence_ < log_.firstIndex()) {
            // this is must be an old retransmission: we've already acked this sequence! drop it
            return;
        }

        // Add empty entries before this sequence, if this is a "future" entry
        while (entry.sequence_ > log_.nextIndex()) {
            PendingLogEntry* pending = log_.add();
            ASSERT(pending->entry_.empty());
        }

        if (log_.nextIndex() == entry.sequence_) {
            PendingLogEntry* pending = log_.add();
            assert(pending->entry_.empty());
            pending->entry_ = entry.entry_;
        } else {
            // Must either be filling a hole or is a retransmission
            assert(log_.at((size_t) entry.sequence_).entry_.empty() ||
                    log_.at((size_t) entry.sequence_).entry_ == entry.entry_);
            log_.mutableAt((size_t) entry.sequence_)->entry_ = entry.entry_;
        }

        // ack receipt
        Ack msg;
        msg.sequence_ = entry.sequence_;
        communication_->send(0, msg);
    } else {
        assert(entry.entry_.empty());
    }

    // Record the uncommitted/unacked counters for processing on idle.
    // TODO: Are there better policies for when to process records? We want to send acks with
    // high priority.
    primary_next_uncommitted_ =
            std::max(primary_next_uncommitted_, entry.next_uncommitted_sequence_);
    primary_next_unacknowledged_ =
            std::max(primary_next_unacknowledged_, entry.next_unacknowledged_sequence_);
}

void PrimaryBackupReplica::idle() {
    if (isPrimary()) return;

    // update the uncommitted counter. we may not have received some entries that have
    // committed (quorums), so only commit things we have received
    while (next_uncommitted_ < primary_next_uncommitted_
            && next_uncommitted_ < log_.nextIndex()) {
        assert(log_.firstIndex() <= next_uncommitted_ && next_uncommitted_ < log_.nextIndex());
        if (log_.at((size_t) next_uncommitted_).entry_.empty()) {
            break;
        }

        nextLogEntryCallback((int) next_uncommitted_, log_.at((size_t) next_uncommitted_).entry_, NULL);
        next_uncommitted_ += 1;
    }

    // update the unacked counter.
    while (next_unacknowledged_ < primary_next_unacknowledged_) {
        // we MUST have received this entry: all backups have acked it
        assert(!log_.empty() && log_.firstIndex() == next_unacknowledged_);
        log_.pop_front();
        next_unacknowledged_ += 1;
    }
}

}
