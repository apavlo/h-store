// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/stlutil.h"
#include "io/buffer.h"
#include "io/message.h"
#include "replication/primarybackup.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

using namespace replication;


class LogCallback : public FaultTolerantLogCallback {
public:
    LogCallback() : last_sequence_(Entry::INVALID_SEQUENCE), last_arg_(NULL) {}

    virtual void nextLogEntry(int sequence, const string& entry, void* arg) {
        last_sequence_ = sequence;
        last_entry_ = entry;
        last_arg_ = arg;
    }

    int last_sequence_;
    string last_entry_;
    void* last_arg_;
};


class MockPrimaryBackupCommunication : public PrimaryBackupCommunication {
public:
    MockPrimaryBackupCommunication() :
            counter_(0), flush_count_(0), has_entry_(false), has_ack_(false) {}

    virtual void send(int replica_index, const Entry& entry) {
        counter_ += 1;
        destination_ = replica_index;
        has_entry_ = true;
        entry_ = entry;
    }

    virtual void send(int replica_index, const Ack& ack) {
        counter_ += 1;
        destination_ = replica_index;
        has_ack_ = true;
        ack_ = ack;
    }

    virtual void flush() {
        assert(buffered());
        flush_count_ += 1;
    }

    int counter_;
    int destination_;
    int flush_count_;

    bool has_entry_;
    Entry entry_;

    bool has_ack_;
    Ack ack_;
};


class StringMessage : public io::Message {
public:
    StringMessage(const string& data) : data_(data) {}

    virtual void serialize(io::FIFOBuffer* buffer) const {
        buffer->copyIn(data_.data(), (int) data_.size());
    }

    const string& data() { return data_; }

private:
    string data_;
};

// Message that serializes to zero bytes.
class EmptyMessage : public io::Message {
public:
    virtual void serialize(io::FIFOBuffer* buffer) const {}
};


class PrimaryBackupTest : public Test {
public:
    PrimaryBackupTest() : message1_("m1"), message2_("m2"), message3_("m3"), message4_("m4"),
            message5_("m5"), replica_(&communication_) {
        replica_.entry_callback(&log_);
    }

    StringMessage message1_;
    StringMessage message2_;
    StringMessage message3_;
    StringMessage message4_;
    StringMessage message5_;

    MockPrimaryBackupCommunication communication_;
    LogCallback log_;
    PrimaryBackupReplica replica_;
    Ack ack_;
    Entry entry_;
};


TEST_F(PrimaryBackupTest, BadSetPrimary) {
    EXPECT_DEATH(replica_.setPrimary(-1));
    EXPECT_DEATH(replica_.setPrimary(0));
    EXPECT_DEATH(replica_.setPrimary(1));
    replica_.setPrimary(2);
    EXPECT_DEATH(replica_.setPrimary(2));
}

TEST_F(PrimaryBackupTest, BadBackup) {
    replica_.setBackup();
    EXPECT_DEATH(replica_.setBackup());
}

TEST_F(PrimaryBackupTest, SubmitBeforeMembership) {
    // cannot submit operations before membership is set
    EXPECT_DEATH(replica_.submit(message1_, NULL));
    EXPECT_DEATH(replica_.isPrimary());
}

TEST_F(PrimaryBackupTest, SetPrimaryIsPrimary) {
    replica_.setPrimary(3);
    EXPECT_TRUE(replica_.isPrimary());
    EXPECT_DEATH(replica_.setBackup());
    EXPECT_DEATH(replica_.setPrimary(3));
}

TEST_F(PrimaryBackupTest, BadSubmit) {
    replica_.setPrimary(3);
    // Cannot submit empty operations
    EXPECT_DEATH(replica_.submit(EmptyMessage(), NULL));
}

TEST_F(PrimaryBackupTest, BadAcks) {
    // submit an operation
    replica_.setPrimary(3);
    replica_.submit(message1_, NULL);
    EXPECT_EQ(2, communication_.counter_);

    // bad sequence number
    ack_.sequence_ = -1;
    EXPECT_DEATH(replica_.receive(1, ack_));

    // ack for future sequence numbers: not okay
    ack_.sequence_ = 1;
    EXPECT_DEATH(replica_.receive(1, ack_));

    // ack from bad channels
    ack_.sequence_ = 0;
    EXPECT_DEATH(replica_.receive(-1, ack_));
    EXPECT_DEATH(replica_.receive(0, ack_));
    EXPECT_DEATH(replica_.receive(3, ack_));
}

TEST_F(PrimaryBackupTest, PrimaryNoEntry) {
    replica_.setPrimary(3);
    // primaries cannot receive entries
    EXPECT_DEATH(replica_.receive(1, entry_));
}

TEST_F(PrimaryBackupTest, Primary) {
    replica_.setPrimary(3);

    // submit an operation
    void* const CALLBACK_ARGUMENT = (void*) 42;
    int sequence = replica_.submit(message1_, CALLBACK_ARGUMENT);
    EXPECT_EQ(0, sequence);
    EXPECT_EQ(2, communication_.counter_);
    EXPECT_TRUE(communication_.has_entry_);
    EXPECT_EQ(0, communication_.entry_.sequence_);
    EXPECT_EQ(message1_.data(), communication_.entry_.entry_);
    EXPECT_EQ(0, communication_.entry_.next_uncommitted_sequence_);
    EXPECT_EQ(0, communication_.entry_.next_unacknowledged_sequence_);

    ack_.sequence_ = 0;
    replica_.receive(1, ack_);
    EXPECT_EQ(Entry::INVALID_SEQUENCE, log_.last_sequence_);
    // multiple acks: ignored
    replica_.receive(1, ack_);
    EXPECT_EQ(Entry::INVALID_SEQUENCE, log_.last_sequence_);

    // last ack = committed
    replica_.receive(2, ack_);
    EXPECT_EQ(0, log_.last_sequence_);
    EXPECT_EQ(message1_.data(), log_.last_entry_);
    EXPECT_EQ(CALLBACK_ARGUMENT, log_.last_arg_);

    // old ack should be ignored
    replica_.receive(1, ack_);

    // next submits
    sequence = replica_.submit(message2_, NULL);
    EXPECT_EQ(1, sequence);
    EXPECT_EQ(1, communication_.entry_.sequence_);
    EXPECT_EQ(message2_.data(), communication_.entry_.entry_);
    EXPECT_EQ(1, communication_.entry_.next_uncommitted_sequence_);
    EXPECT_EQ(1, communication_.entry_.next_unacknowledged_sequence_);

    sequence = replica_.submit(message3_, NULL);
    EXPECT_EQ(2, sequence);
    EXPECT_EQ(2, communication_.entry_.sequence_);
    EXPECT_EQ(message3_.data(), communication_.entry_.entry_);
    EXPECT_EQ(1, communication_.entry_.next_uncommitted_sequence_);
    EXPECT_EQ(1, communication_.entry_.next_unacknowledged_sequence_);

    // ack sequence 2: no commits
    ack_.sequence_ = 2;
    replica_.receive(1, ack_);
    replica_.receive(2, ack_);
    EXPECT_EQ(0, log_.last_sequence_);

    // No change in next uncommitted and next unacknowledged
    sequence = replica_.submit(message4_, NULL);
    EXPECT_EQ(3, sequence);
    EXPECT_EQ(3, communication_.entry_.sequence_);
    EXPECT_EQ(message4_.data(), communication_.entry_.entry_);
    EXPECT_EQ(1, communication_.entry_.next_uncommitted_sequence_);
    EXPECT_EQ(1, communication_.entry_.next_unacknowledged_sequence_);

    // ack 1
    ack_.sequence_ = 1;
    replica_.receive(1, ack_);
    replica_.receive(2, ack_);

    // Both uncommitted and unacknowledged roll forward
    EXPECT_EQ(2, log_.last_sequence_);
    EXPECT_EQ(message3_.data(), log_.last_entry_);
    sequence = replica_.submit(message5_, NULL);
    EXPECT_EQ(4, sequence);
    EXPECT_EQ(4, communication_.entry_.sequence_);
    EXPECT_EQ(message5_.data(), communication_.entry_.entry_);
    EXPECT_EQ(3, communication_.entry_.next_uncommitted_sequence_);
    EXPECT_EQ(3, communication_.entry_.next_unacknowledged_sequence_);
}

TEST_F(PrimaryBackupTest, PrimaryCleanupLogEntries) {
    replica_.setPrimary(3);

    // submit an operation
    replica_.submit(message1_, NULL);
    EXPECT_EQ(1, replica_.logSize());

    // everyone has acknowledged the entry: it can be removed
    ack_.sequence_ = 0;
    replica_.receive(1, ack_);
    replica_.receive(2, ack_);
    EXPECT_EQ(0, replica_.logSize());
}

TEST_F(PrimaryBackupTest, PrimaryChangeCallback) {
    replica_.setPrimary(3);

    int sequence = replica_.submit(message1_, NULL);
    replica_.setCallbackArg(sequence, this);

    ack_.sequence_ = 0;
    replica_.receive(1, ack_);
    replica_.receive(2, ack_);
    // The argument was changed
    EXPECT_EQ(this, log_.last_arg_);

    // Bad arguments
    EXPECT_DEATH(replica_.setCallbackArg(0, NULL));
    EXPECT_DEATH(replica_.setCallbackArg(-1, NULL));
    EXPECT_DEATH(replica_.setCallbackArg(1, NULL));

    // This is now okay
    replica_.submit(message1_, NULL);
    replica_.setCallbackArg(1, NULL);
}

TEST_F(PrimaryBackupTest, BackupCleanupLogEntries) {
    replica_.setBackup();

    // receive an operation
    entry_.sequence_ = 0;
    entry_.entry_ = message1_.data();
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = 0;
    replica_.receive(0, entry_);
    replica_.idle();
    EXPECT_EQ(1, replica_.logSize());

    // receive another operation with a high unacked sequence
    entry_.sequence_ = 1;
    entry_.next_uncommitted_sequence_ = 1;
    entry_.next_unacknowledged_sequence_ = 1;
    replica_.receive(0, entry_);
    replica_.idle();
    EXPECT_EQ(1, replica_.logSize());

    // receive an empty operation
    entry_.sequence_ = Entry::INVALID_SEQUENCE;
    entry_.entry_.clear();
    entry_.next_uncommitted_sequence_ = 2;
    entry_.next_unacknowledged_sequence_ = 2;
    replica_.receive(0, entry_);
    replica_.idle();
    EXPECT_EQ(0, replica_.logSize());

    // receive an old operation. This kind of inconsistency can happen if this is a really old
    // message. It would be nice if we crashed at this inconsistency, but we just drop the message.
    entry_.sequence_ = 0;
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = 0;
    entry_.entry_ = message2_.data();
    communication_.has_ack_ = false;
    replica_.receive(0, entry_);
    EXPECT_FALSE(communication_.has_ack_);  // no ack for old decided messages
    EXPECT_EQ(0, replica_.logSize());
}

TEST_F(PrimaryBackupTest, BadSetBackupBadOrder) {
    replica_.setBackup();
    EXPECT_DEATH(replica_.setBackup());
    EXPECT_DEATH(replica_.setPrimary(2));
}

TEST_F(PrimaryBackupTest, BackupNoSubmitOrAcks) {
    replica_.setBackup();
    EXPECT_DEATH(replica_.submit(message1_, NULL));
    ack_.sequence_ = 0;
    EXPECT_DEATH(replica_.receive(0, ack_));
}

TEST_F(PrimaryBackupTest, BadEntry) {
    // not running yet
    EXPECT_DEATH(replica_.receive(0, entry_));
    replica_.setBackup();
    EXPECT_DEATH(replica_.submit(message1_, NULL));

    // updating the unacked and uncommitted counters: entry not permitted
    entry_.sequence_ = Entry::INVALID_SEQUENCE;
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = 0;
    entry_.entry_ = message1_.data();
    EXPECT_DEATH(replica_.receive(0, entry_));
    
    // Empty log entries not permitted
    entry_.sequence_ = 0;
    entry_.entry_.clear();
    EXPECT_DEATH(replica_.receive(0, entry_));
    entry_.entry_ = message1_.data();

    // unacked <= sequence.
    entry_.next_uncommitted_sequence_ = 3;
    entry_.next_unacknowledged_sequence_ = 1;
    EXPECT_DEATH(replica_.receive(0, entry_));

    // invalid sequence numbers
    entry_.next_uncommitted_sequence_ = -1;
    entry_.next_unacknowledged_sequence_ = 0;
    EXPECT_DEATH(replica_.receive(0, entry_));
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = -1;
    EXPECT_DEATH(replica_.receive(0, entry_));
    entry_.next_unacknowledged_sequence_ = 0;
    
    // not from the primary
    EXPECT_DEATH(replica_.receive(-1, entry_));
    EXPECT_DEATH(replica_.receive(1, entry_));

    entry_.sequence_ = 2;
    entry_.next_uncommitted_sequence_ = 1;
    entry_.next_unacknowledged_sequence_ = 1;
    // cannot have acknowledged a sequence we haven't received yet!
    EXPECT_DEATH(replica_.receive(0, entry_));

    // Accept a value
    entry_.sequence_ = 0;
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = 0;
    replica_.receive(0, entry_);

    // Same sequence, different entry!
    // TODO: In case of failure, this is actually okay if it comes from a new primary
    entry_.entry_ = message2_.data();
    EXPECT_DEATH(replica_.receive(0, entry_));

    // unacked <= uncommitted
    entry_.entry_.clear();
    entry_.sequence_ = Entry::INVALID_SEQUENCE;
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = 1;
    EXPECT_DEATH(replica_.receive(0, entry_));

    // Move the unacked/uncommitted sequence numbers forward
    entry_.next_uncommitted_sequence_ = 1;
    entry_.next_unacknowledged_sequence_ = 1;
    replica_.receive(0, entry_);

    // Getting a message where the sequences move backwards is okay: message reorderings
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = 0;
    replica_.receive(0, entry_);
}

TEST_F(PrimaryBackupTest, Backup) {
    replica_.setBackup();
    EXPECT_FALSE(replica_.isPrimary());

    // receive out of order is okay
    entry_.sequence_ = 1;
    entry_.next_uncommitted_sequence_ = 0;
    entry_.next_unacknowledged_sequence_ = 0;
    entry_.entry_ = message1_.data();
    replica_.receive(0, entry_);

    EXPECT_TRUE(communication_.has_ack_);
    EXPECT_EQ(1, communication_.ack_.sequence_);
    EXPECT_EQ(Entry::INVALID_SEQUENCE, log_.last_sequence_);

    // receive 0
    entry_.sequence_ = 0;
    replica_.receive(0, entry_);
    EXPECT_EQ(0, communication_.ack_.sequence_);
    EXPECT_EQ(Entry::INVALID_SEQUENCE, log_.last_sequence_);

    // receive an empty entry that updates the committed and unacked counts
    entry_.sequence_ = Entry::INVALID_SEQUENCE;
    entry_.next_uncommitted_sequence_ = 2;
    entry_.next_unacknowledged_sequence_ = 2;
    entry_.entry_.clear();
    communication_.has_ack_ = false;
    replica_.receive(0, entry_);
    EXPECT_FALSE(communication_.has_ack_);

    // Log callback not called until idle
    EXPECT_EQ(-1, log_.last_sequence_);
    replica_.idle();
    EXPECT_EQ(1, log_.last_sequence_);
    EXPECT_EQ(message1_.data(), log_.last_entry_);

    // This is okay: In case of weird failures, the primary may be retransmitting and catching us up
    entry_.sequence_ = 3;
    entry_.next_uncommitted_sequence_ = 50;
    entry_.next_unacknowledged_sequence_ = 2;
    entry_.entry_ = message2_.data();
    replica_.receive(0, entry_);
    EXPECT_EQ(1, log_.last_sequence_);
    entry_.sequence_ = 2;
    // both new sequences are committed: the primary told us so!
    replica_.receive(0, entry_);

    EXPECT_EQ(1, log_.last_sequence_);
    replica_.idle();
    EXPECT_EQ(3, log_.last_sequence_);
}

TEST_F(PrimaryBackupTest, BufferedFlush) {
    replica_.setPrimary(3);
    EXPECT_DEATH(replica_.flush());

    communication_.buffered(true);
    replica_.flush();
    EXPECT_EQ(1, communication_.flush_count_);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
