// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "dtxn/ordered/orderedclient.h"
#include "mockmessageconnection.h"
#include "stupidunit/stupidunit.h"

using std::string;

using namespace dtxn;

class OrderedClientTest : public Test, public TransactionCallback {
public:
    OrderedClientTest() :
            sequencer_(new MockMessageConnection()),
            client_(sequencer_),
            transaction_("hello world"),
            callback_(false) {
    }

    virtual void transactionComplete(const FragmentResponse& response) {
        callback_ = true;
        response_ = response;
    }

    void responseReceived(FragmentResponse& r) {
        string buffer;
        r.appendToString(&buffer);
        client_.messageReceived(sequencer_, r.typeCode(), buffer);
    }

protected:
    MockMessageConnection* sequencer_;
    OrderedClient client_;

    string transaction_;
    bool callback_;
    FragmentResponse response_;
    FragmentResponse temp_response_;
};

TEST(OrderedClient, BadCreate) {
    EXPECT_DEATH(OrderedClient(NULL));
};

TEST_F(OrderedClientTest, ExecuteBadCallback) {
    EXPECT_DEATH(client_.execute(transaction_, NULL));
}

TEST_F(OrderedClientTest, ExecuteBadTransaction) {
    transaction_.clear();
    EXPECT_DEATH(client_.execute(transaction_, this));
}

TEST_F(OrderedClientTest, NoRequest) {
    EXPECT_DEATH(responseReceived(response_));
}

TEST_F(OrderedClientTest, ConnectionClosed) {
    // TODO: At some point this needs to be handled
    EXPECT_DEATH(client_.connectionClosed(sequencer_));
}

TEST_F(OrderedClientTest, SetSequencerTarget) {
    EXPECT_EQ(true, sequencer_->target() != NULL);
}

TEST_F(OrderedClientTest, Simple) {
    client_.execute(transaction_, this);

    EXPECT_EQ(false, callback_);
    EXPECT_EQ(true, sequencer_->hasMessage());
    Fragment txn;
    sequencer_->getMessage(&txn);
    EXPECT_EQ(0, txn.id);
    EXPECT_EQ(transaction_, txn.transaction);
    EXPECT_TRUE(txn.last_fragment);
    EXPECT_FALSE(txn.multiple_partitions);

    // Receive a response
    temp_response_.status = ExecutionEngine::ABORT_USER;
    temp_response_.result = "foo";
    responseReceived(temp_response_);

    EXPECT_EQ(true, callback_);
    EXPECT_EQ(temp_response_, response_);
}

TEST_F(OrderedClientTest, BadResponse) {
    // send 2 requests
    // TODO: out of order responses?
    client_.execute(transaction_, this);
    client_.execute(transaction_, this);

    temp_response_.id = -1;
    EXPECT_DEATH(responseReceived(temp_response_));
    temp_response_.id = 1;
    EXPECT_DEATH(responseReceived(temp_response_));

    // incorrect connection
    MockMessageConnection foo;
    temp_response_.id = 0;
    string buffer;
    temp_response_.appendToString(&buffer);
    EXPECT_DEATH(client_.messageReceived(&foo, temp_response_.typeCode(), buffer));

    // wrong type code
    EXPECT_DEATH(client_.messageReceived(sequencer_, 42, buffer));

    // bad parsing
    EXPECT_DEATH(client_.messageReceived(sequencer_, temp_response_.typeCode(), string()));
    
    // correct
    responseReceived(temp_response_);

    EXPECT_DEATH(responseReceived(temp_response_));
    temp_response_.id = 1;
    responseReceived(temp_response_);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
