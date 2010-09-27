// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef REPLICATION_PRIMARYBACKUP_H__
#define REPLICATION_PRIMARYBACKUP_H__

#include <vector>

#include "io/message.h"
#include "replication/cachedslidingarray.h"
#include "replication/faulttolerantlog.h"
#include "serialization.h"

namespace io {
class FIFOBuffer;
}

namespace replication {

// Message sent from the primary to the backups
struct Entry : public io::Message {
    Entry() : sequence_(INVALID_SEQUENCE), next_uncommitted_sequence_(INVALID_SEQUENCE), next_unacknowledged_sequence_(INVALID_SEQUENCE) {}
    virtual ~Entry() {}

    virtual void serialize(io::FIFOBuffer* out) const {
        serialization::serialize(sequence_, out);
        serialization::serialize(entry_, out);
        serialization::serialize(next_uncommitted_sequence_, out);
        serialization::serialize(next_unacknowledged_sequence_, out);
    }

    const char* parseFromString(const char* start, const char* end) {
        const char* next = serialization::deserialize(&sequence_, start, end);
        next = serialization::deserialize(&entry_, next, end);
        next = serialization::deserialize(&next_uncommitted_sequence_, next, end);
        next = serialization::deserialize(&next_unacknowledged_sequence_, next, end);
        return next;
    }

    static int32_t typeCode() { return 0; }

    // An invalid sequence number
    static const int64_t INVALID_SEQUENCE = -1;

    // If sequence is invalid, there is no entry.
    int64_t sequence_;

    // The contents of the log.
    std::string entry_;

    // All sequence < next_uncommitted_sequence_ have been committed.
    int64_t next_uncommitted_sequence_;

    // All sequence < next_unacknowledged_sequence_ have been received everywhere.
    int64_t next_unacknowledged_sequence_;
};

// Message sent from the backup to the primary
struct Ack {
    Ack() : sequence_(Entry::INVALID_SEQUENCE) {}
    virtual ~Ack() {}

    virtual void serialize(io::FIFOBuffer* out) const {
        serialization::serialize(sequence_, out);
    }

    const char* parseFromString(const char* start, const char* end) {
        return serialization::deserialize(&sequence_, start, end);
    }

    static int32_t typeCode() {
        return 1;
    }

    // The received entry
    int64_t sequence_;
};


// Interface for sending messages between replicas.
class PrimaryBackupCommunication {
public:
    PrimaryBackupCommunication() : buffered_(false) {}

    virtual ~PrimaryBackupCommunication() {}

    virtual void send(int replica_index, const Entry& entry) = 0;
    virtual void send(int replica_index, const Ack& entry) = 0;

    // If buffered, this flushes all outgoing messages.
    virtual void flush() = 0;

    // Changes the buffering on communication.
    void buffered(bool buffered) { buffered_ = buffered; }
    bool buffered() const { return buffered_; }

private:
    bool buffered_;
};


// A simple primary-backup implementation of a fault-tolerant log.
class PrimaryBackupReplica : public FaultTolerantLog {
public:
    // does not owns communication
    PrimaryBackupReplica(PrimaryBackupCommunication* communication);
    virtual ~PrimaryBackupReplica();

    void setPrimary(int num_replicas_);
    void setBackup();

    // Flushes any buffered messages.
    virtual void flush();

    // Implementation of FaultTolerantLog.
    virtual int submit(const io::Message& operation, void* callback_arg);
    virtual void setCallbackArg(int sequence, void* callback_arg);
    virtual bool isPrimary() const;

    void receive(int replica_index, const Entry& entry);
    void receive(int replica_index, const Ack& ack);

    // Invoked from the event loop, after handling all read and write events. This is used to
    // invoke the log entry callbacks on the backup.
    void idle();

    // Returns the number of log entries stored in this replica. Used for unit testing.
    size_t logSize() const { return log_.size(); }

private:
    PrimaryBackupCommunication* communication_;

    struct PendingLogEntry;
    replication::CachedSlidingArray<PendingLogEntry> log_;

    int64_t next_uncommitted_;
    int64_t next_unacknowledged_;

    int64_t primary_next_uncommitted_;
    int64_t primary_next_unacknowledged_;

    int num_replicas_;
    bool is_primary_;

    // Used to temporarily serialize messages. TODO: Remove this to remove a copy.
    io::FIFOBuffer* serialization_buffer_;
};

}
#endif
