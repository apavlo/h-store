// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "io/buffer.h"
#include "io/message.h"
//~ #include "mockmessageconnection.h"
//~ #include "net/messageserver.h"
#include "replication/nulllog.h"
#include "stupidunit/stupidunit.h"

using std::string;

class MockLogCallback : public replication::FaultTolerantLogCallback {
public:
    MockLogCallback() : last_sequence_(-1), last_arg_(NULL) {}

    virtual void nextLogEntry(int sequence, const string& entry, void* arg) {
        last_sequence_ = sequence;
        last_entry_ = entry;
        last_arg_ = arg;
    }

    int last_sequence_;
    string last_entry_;
    void* last_arg_;
};

class MockMessage : public io::Message {
public:
    virtual ~MockMessage() {}

    virtual void serialize(io::FIFOBuffer* buffer) const {
        buffer->copyIn(DATA);
    }

    static const string DATA;
};
const string MockMessage::DATA = "data";

class NullLogTest : public Test {
public:
    NullLogTest() {
        log_.entry_callback(&callback_);
    }

    MockLogCallback callback_;
    replication::NullLog log_;
    MockMessage message_;
};

TEST_F(NullLogTest, Simple) {
    EXPECT_EQ(0, log_.submit(message_, this));
    EXPECT_EQ(0, callback_.last_sequence_);
    EXPECT_EQ(MockMessage::DATA, callback_.last_entry_);
    EXPECT_EQ(this, callback_.last_arg_);

    EXPECT_EQ(1, log_.submit(message_, &message_));
    EXPECT_EQ(1, callback_.last_sequence_);
    EXPECT_EQ(MockMessage::DATA, callback_.last_entry_);
    EXPECT_EQ(&message_, callback_.last_arg_);    
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
