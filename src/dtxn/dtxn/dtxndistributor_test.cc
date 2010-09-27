// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/dtxndistributor.h"
#include "stupidunit/stupidunit.h"

using namespace dtxn;

class CoordinatedTransactionTest : public Test {
public:
    CoordinatedTransactionTest() : txn_(4, "foo") {}

    CoordinatedTransaction txn_;
};

TEST_F(CoordinatedTransactionTest, NoPartitionCommit) {
    EXPECT_FALSE(txn_.isAllDone());
    EXPECT_EQ(1, txn_.received().size());
    EXPECT_EQ(CoordinatedTransaction::CLIENT_PARTITION, txn_.received()[0].first);
    EXPECT_EQ("foo", txn_.received()[0].second);

    // Send a commit response to the client without involving any partitions
    // empty messages are okay: maybe the transaction only wants to report commit/abort.
    txn_.commit("");
    EXPECT_DEATH(txn_.commit(""));
    EXPECT_DEATH(txn_.commit("bar"));
    EXPECT_DEATH(txn_.abort("bar"));
    EXPECT_DEATH(txn_.send(2, "bar"));

    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_EQ(1, txn_.sent().size());
    EXPECT_EQ(CoordinatedTransaction::CLIENT_PARTITION, txn_.sent()[0].first);
    EXPECT_EQ("", txn_.sent()[0].second);
    EXPECT_EQ(ExecutionEngine::OK, txn_.status());
    EXPECT_TRUE(txn_.isAllDone());
}

TEST_F(CoordinatedTransactionTest, NoPartitionAbort) {
    // Send an abort response to the client without involving any partitions
    txn_.abort("");
    EXPECT_TRUE(txn_.haveClientResponse());
    EXPECT_DEATH(txn_.commit("bar"));
    EXPECT_DEATH(txn_.abort("bar"));
    EXPECT_DEATH(txn_.send(2, "bar"));

    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_EQ(1, txn_.sent().size());
    EXPECT_EQ(CoordinatedTransaction::CLIENT_PARTITION, txn_.sent()[0].first);
    EXPECT_EQ("", txn_.sent()[0].second);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, txn_.status());
    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_EQ(0, txn_.numInvolved());
}

TEST_F(CoordinatedTransactionTest, BadSend) {
    EXPECT_DEATH(txn_.send(-1, "foo"));
    EXPECT_DEATH(txn_.send(4, "foo"));
    EXPECT_DEATH(txn_.send(2, ""));
    txn_.send(2, "foo");
    EXPECT_DEATH(txn_.send(2, "bar"));
}

TEST_F(CoordinatedTransactionTest, SinglePartition) {
    // A single message then all finished = sp txn
    txn_.send(2, "bar");
    txn_.setAllDone();

    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_FALSE(txn_.haveClientResponse());
    EXPECT_EQ(1, txn_.sent().size());
    EXPECT_EQ(2, txn_.sent()[0].first);
    EXPECT_EQ("bar", txn_.sent()[0].second);

    // Message gets sent
    txn_.sentMessages();
    EXPECT_DEATH(txn_.send(1, "foo"));
    EXPECT_FALSE(txn_.receivedAll());
    
    // Response is received
    EXPECT_DEATH(txn_.receive(1, "r", ExecutionEngine::OK));
    txn_.receive(2, "", ExecutionEngine::OK);  // empty is permitted: eg committing writes?
    EXPECT_DEATH(txn_.receive(2, "r", ExecutionEngine::OK));
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_EQ(1, txn_.received().size());
    txn_.readyNextRound();

    // coordinator handles it
    txn_.commit("commit");
    EXPECT_TRUE(txn_.haveClientResponse());
    EXPECT_EQ(1, txn_.sent().size());
    EXPECT_EQ(CoordinatedTransaction::CLIENT_PARTITION, txn_.sent()[0].first);
    EXPECT_EQ("commit", txn_.sent()[0].second);
    EXPECT_EQ(ExecutionEngine::OK, txn_.status());
}

TEST_F(CoordinatedTransactionTest, SinglePartitionTimeoutAbort) {
    // A single message then all finished = sp txn
    txn_.send(2, "bar");
    txn_.setAllDone();
    txn_.sentMessages();

    // Abort here: maybe we have a timeout?
    txn_.abort("abort");
    EXPECT_DEATH(txn_.abort("again"));
    EXPECT_DEATH(txn_.commit("again"));
    EXPECT_DEATH(txn_.receive(2, "r", ExecutionEngine::OK));
 
    EXPECT_TRUE(txn_.haveClientResponse());
}

TEST_F(CoordinatedTransactionTest, GeneralTransaction) {
    txn_.send(0, "first");
    EXPECT_FALSE(txn_.isAllDone());
    EXPECT_EQ(1, txn_.sent().size());

    // Message gets sent
    txn_.setMultiplePartitions();
    txn_.sentMessages();
    
    // Response is received
    txn_.receive(0, "r", ExecutionEngine::OK);
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_EQ(1, txn_.received().size());

    // coordinator sends round two
    txn_.readyNextRound();
    txn_.send(2, "second");
    txn_.setAllDone();
    EXPECT_TRUE(txn_.isAllDone());

    txn_.sentMessages();
    txn_.receive(2, "r", ExecutionEngine::OK);
    EXPECT_TRUE(txn_.receivedAll());
    txn_.readyNextRound();

    // coordinator decides to abort
    txn_.abort("bad");
    EXPECT_EQ(1, txn_.sent().size());
    EXPECT_EQ(CoordinatedTransaction::CLIENT_PARTITION, txn_.sent()[0].first);
    EXPECT_EQ("bad", txn_.sent()[0].second);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, txn_.status());

    EXPECT_EQ(2, txn_.numInvolved());
    ASSERT_EQ(2, txn_.involvedPartitions().size());
    EXPECT_EQ(0, txn_.involvedPartitions()[0]);
    EXPECT_EQ(2, txn_.involvedPartitions()[1]);
}

TEST_F(CoordinatedTransactionTest, OneShotAbort) {
    txn_.send(0, "msg");
    txn_.send(1, "msg");
    txn_.send(2, "msg");
    txn_.send(3, "msg");
    txn_.setAllDone();

    // Message gets sent
    txn_.setMultiplePartitions();
    txn_.sentMessages();
    
    // Response is received
    txn_.receive(0, "abcd", ExecutionEngine::OK);
    txn_.receive(1, "r", ExecutionEngine::ABORT_USER);
    txn_.receive(2, "abcd", ExecutionEngine::OK);
    txn_.receive(3, "r", ExecutionEngine::ABORT_USER);
    EXPECT_TRUE(txn_.receivedAll());
    txn_.readyNextRound();
    EXPECT_EQ(ExecutionEngine::ABORT_USER, txn_.status());

    // We should discard the "abcd" responses: we only keep aborts
    int empty_count = 0;
    for (size_t i = 0; i < txn_.received().size(); ++i) {
        if (txn_.received()[i].second.empty()) {
            empty_count += 1;
        } else {
            EXPECT_EQ("r", txn_.received()[i].second);
        }
    }
    EXPECT_EQ(2, empty_count);

    // Aborting the transaction is still okay
    txn_.abort("bad");
    EXPECT_TRUE(txn_.haveClientResponse());
}

TEST_F(CoordinatedTransactionTest, OneShotDeadlockAbort) {
    txn_.send(0, "msg");
    txn_.send(1, "msg");
    txn_.send(2, "msg");
    txn_.setAllDone();

    // Message gets sent
    txn_.setMultiplePartitions();
    txn_.sentMessages();
    
    // Response is received
    txn_.receive(0, "abcd", ExecutionEngine::OK);
    EXPECT_DEATH(txn_.receive(1, "r", ExecutionEngine::ABORT_DEADLOCK));
    txn_.receive(1, "", ExecutionEngine::ABORT_DEADLOCK);
    txn_.receive(2, "abcd", ExecutionEngine::ABORT_USER);
    EXPECT_TRUE(txn_.receivedAll());
    txn_.readyNextRound();
    EXPECT_EQ(ExecutionEngine::ABORT_DEADLOCK, txn_.status());

    // We should discard all responses for a deadlock abort
    int empty_count = 0;
    for (size_t i = 0; i < txn_.received().size(); ++i) {
        if (txn_.received()[i].second.empty()) {
            empty_count += 1;
        }
    }
    EXPECT_EQ(3, empty_count);

    // Aborting a deadlocked transaction with a message is not okay
    EXPECT_DEATH(txn_.abort("foo"));
    txn_.abort("");
    EXPECT_TRUE(txn_.haveClientResponse());
    EXPECT_EQ(ExecutionEngine::ABORT_DEADLOCK, txn_.status());
}

TEST_F(CoordinatedTransactionTest, OneShotDependencyRemoval) {
    txn_.send(0, "msg");
    txn_.send(1, "msg");
    txn_.setAllDone();

    // Message gets sent
    txn_.setMultiplePartitions();
    txn_.sentMessages();
    
    // Responses received
    EXPECT_FALSE(txn_.hasResponse(0));
    txn_.receive(0, "abcd", ExecutionEngine::OK);
    EXPECT_TRUE(txn_.hasResponse(0));
    EXPECT_DEATH(txn_.removeResponse(1));
    txn_.receive(1, "", ExecutionEngine::OK);
    EXPECT_TRUE(txn_.receivedAll());

    // remove a response
    EXPECT_DEATH(txn_.removeResponse(2));
    txn_.removeResponse(1);
    EXPECT_FALSE(txn_.receivedAll());
    EXPECT_FALSE(txn_.hasResponse(1));

    // receive the response again
    txn_.receive(1, "", ExecutionEngine::OK);
    EXPECT_TRUE(txn_.receivedAll());

    txn_.readyNextRound();
    txn_.commit("foo");
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
