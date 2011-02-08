// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/distributedtransaction.h"
#include "stupidunit/stupidunit.h"

using namespace dtxn;

class PartitionStateTest : public Test {
protected:
    PartitionState state_;
};

TEST_F(PartitionStateTest, UnknownDone) {
    EXPECT_FALSE(state_.isParticipant());
    EXPECT_FALSE(state_.active());
    EXPECT_FALSE(state_.finished());

    // unknown -> finished
    state_.setDone();
    EXPECT_DEATH(state_.setDone());
    EXPECT_DEATH(state_.setDone());

    EXPECT_FALSE(state_.isParticipant());
    EXPECT_FALSE(state_.active());
    EXPECT_TRUE(state_.finished());
}

TEST_F(PartitionStateTest, Active) {
    // unknown -> active
    state_.setActive();
    EXPECT_TRUE(state_.isParticipant());
    EXPECT_TRUE(state_.active());
    state_.setActive();

    // active -> preparing
    state_.setDone();
    EXPECT_DEATH(state_.setDone());
    EXPECT_DEATH(state_.setActive());

    EXPECT_TRUE(state_.isParticipant());
    EXPECT_FALSE(state_.active());
    EXPECT_FALSE(state_.finished());
    EXPECT_TRUE(state_.preparing());

    state_.setPrepared();
    EXPECT_DEATH(state_.setPrepared());
    EXPECT_FALSE(state_.preparing());
    EXPECT_TRUE(state_.prepared());
    EXPECT_FALSE(state_.finished());

    state_.setFinished();
    EXPECT_DEATH(state_.setFinished());
    EXPECT_FALSE(state_.isParticipant());
    EXPECT_FALSE(state_.prepared());
    EXPECT_TRUE(state_.finished());
}

class DistributedTransactionTest : public Test {
public:
    DistributedTransactionTest() : txn_(4) {
    }

    DistributedTransaction txn_;
};

TEST_F(DistributedTransactionTest, Empty) {
    // Initially transactions are empty
    EXPECT_FALSE(txn_.isAllDone());
    EXPECT_EQ(0, txn_.sent().size());
    EXPECT_EQ(0, txn_.received().size());
}

TEST_F(DistributedTransactionTest, BadSend) {
    EXPECT_DEATH(txn_.send(-1, "foo"));
    EXPECT_DEATH(txn_.send(4, "foo"));
    EXPECT_DEATH(txn_.send(2, ""));
    txn_.send(2, "foo");
    EXPECT_DEATH(txn_.send(2, "bar"));
}

TEST_F(DistributedTransactionTest, BadPartitionIndicies) {
    bool v;
    EXPECT_DEATH(v = txn_.isParticipant(-1));
    EXPECT_DEATH(v = txn_.isParticipant(4));
    EXPECT_DEATH(v = txn_.isActive(-1));
    EXPECT_DEATH(v = txn_.isActive(4));
    EXPECT_DEATH(v = txn_.isPrepared(-1));
    EXPECT_DEATH(v = txn_.isPrepared(4));
}

TEST_F(DistributedTransactionTest, SinglePartition) {
    // A single message then all finished = sp txn
    EXPECT_FALSE(txn_.isActive(2));
    txn_.send(2, "bar");
    EXPECT_TRUE(txn_.multiple_partitions());
    EXPECT_TRUE(txn_.isActive(2));
    txn_.setAllDone();
    EXPECT_FALSE(txn_.multiple_partitions());
    EXPECT_DEATH(txn_.send(3, "foo"));
    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_FALSE(txn_.isActive(2));
    EXPECT_FALSE(txn_.isPrepared(2));

    EXPECT_EQ(1, txn_.sent().size());
    EXPECT_EQ(2, txn_.sent()[0].first);
    EXPECT_EQ("bar", txn_.sent()[0].second);

    // Message gets sent
    txn_.sentMessages();
    EXPECT_FALSE(txn_.receivedAll());

    // Response is received
    EXPECT_DEATH(txn_.receive(1, "r", DistributedTransaction::OK));
    txn_.receive(2, "", DistributedTransaction::OK);  // empty is permitted: eg committing writes?
    EXPECT_TRUE(txn_.isPrepared(2));
    EXPECT_DEATH(txn_.receive(2, "r", DistributedTransaction::OK));
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_EQ(1, txn_.received().size());
    txn_.readyNextRound();
    EXPECT_DEATH(txn_.send(2, "foo"));
    EXPECT_DEATH(txn_.send(1, "foo"));

    EXPECT_EQ(DistributedTransaction::OK, txn_.status());
}

TEST_F(DistributedTransactionTest, GeneralTransaction) {
    txn_.send(0, "first");
    EXPECT_FALSE(txn_.isAllDone());
    EXPECT_TRUE(txn_.multiple_partitions());
    EXPECT_EQ(1, txn_.sent().size());

    // Message gets sent
    txn_.sentMessages();

    // Response is received
    txn_.receive(0, "r", DistributedTransaction::OK);
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_EQ(1, txn_.received().size());

    // coordinator sends round two
    txn_.readyNextRound();
    txn_.send(2, "second");
    EXPECT_TRUE(txn_.isActive(2));
    txn_.setAllDone();
    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_FALSE(txn_.isActive(0));
    EXPECT_FALSE(txn_.isActive(2));
    EXPECT_TRUE(txn_.multiple_partitions());
    EXPECT_DEATH(txn_.send(0, "second"));
    EXPECT_DEATH(txn_.send(1, "second"));
    EXPECT_EQ(2, txn_.sent().size());  // includes the explicit prepare

    txn_.sentMessages();
    txn_.receive(2, "r", DistributedTransaction::OK);
    EXPECT_FALSE(txn_.isPrepared(0));
    txn_.receive(0, "", DistributedTransaction::OK);  // explicit prepare ack
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_TRUE(txn_.isPrepared(0));
    EXPECT_TRUE(txn_.isPrepared(2));
    EXPECT_EQ(2, txn_.received().size());
    // The "client" should only need to deal with the one "real" response, not the "prepare" ack
    txn_.removePrepareResponses();
    txn_.readyNextRound();
    EXPECT_EQ(1, txn_.received().size());

    EXPECT_DEATH(txn_.send(0, "foo"));
    EXPECT_EQ(2, txn_.numParticipants());
    ASSERT_EQ(2, txn_.getParticipants().size());
    EXPECT_EQ(0, txn_.getParticipants()[0]);
    EXPECT_EQ(2, txn_.getParticipants()[1]);
}

TEST_F(DistributedTransactionTest, OneShotAbort) {
    txn_.send(0, "msg");
    txn_.send(1, "msg");
    txn_.send(2, "msg");
    txn_.send(3, "msg");
    txn_.setAllDone();
    EXPECT_TRUE(txn_.multiple_partitions());

    // Message gets sent
    txn_.sentMessages();

    // Response is received
    txn_.receive(0, "abcd", DistributedTransaction::OK);
    txn_.receive(1, "r", DistributedTransaction::ABORT_USER);
    txn_.receive(2, "abcd", DistributedTransaction::OK);
    txn_.receive(3, "s", DistributedTransaction::ABORT_USER);
    EXPECT_TRUE(txn_.receivedAll());
    txn_.readyNextRound();
    EXPECT_EQ(DistributedTransaction::ABORT_USER, txn_.status());

    // We should discard the "abcd" responses: we only keep aborts
    ASSERT_EQ(2, txn_.received().size());
    EXPECT_EQ(1, txn_.received()[0].first);
    EXPECT_EQ("r", txn_.received()[0].second);
    EXPECT_EQ(3, txn_.received()[1].first);
    EXPECT_EQ("s", txn_.received()[1].second);

    EXPECT_DEATH(txn_.send(1, "foo"));
}

TEST_F(DistributedTransactionTest, OneShotDeadlockAbort) {
    txn_.send(0, "msg");
    txn_.send(1, "msg");
    txn_.send(2, "msg");
    txn_.setAllDone();

    // Message gets sent
    txn_.sentMessages();
    
    // Response is received
    txn_.receive(0, "abcd", DistributedTransaction::OK);
    EXPECT_DEATH(txn_.receive(1, "r", DistributedTransaction::ABORT_DEADLOCK));
    txn_.receive(1, "", DistributedTransaction::ABORT_DEADLOCK);
    txn_.receive(2, "abcd", DistributedTransaction::ABORT_USER);
    EXPECT_TRUE(txn_.receivedAll());
    txn_.readyNextRound();
    EXPECT_EQ(DistributedTransaction::ABORT_DEADLOCK, txn_.status());

    // keep only abort responses for a deadlock abort
    ASSERT_EQ(1, txn_.received().size());
    EXPECT_EQ(1, txn_.received()[0].first);
    EXPECT_EQ("", txn_.received()[0].second);

    EXPECT_EQ(DistributedTransaction::ABORT_DEADLOCK, txn_.status());
}

TEST_F(DistributedTransactionTest, OneShotDependencyRemoval) {
    txn_.send(0, "msg");
    txn_.send(1, "msg");
    txn_.setAllDone();

    // Message gets sent
    txn_.sentMessages();
    
    // Responses received
    EXPECT_FALSE(txn_.hasResponse(0));
    txn_.receive(0, "abcd", DistributedTransaction::OK);
    EXPECT_TRUE(txn_.hasResponse(0));
    EXPECT_DEATH(txn_.removeResponse(1));
    txn_.receive(1, "", DistributedTransaction::OK);
    EXPECT_TRUE(txn_.receivedAll());

    // remove a response
    EXPECT_DEATH(txn_.removeResponse(2));
    txn_.removeResponse(1);
    EXPECT_FALSE(txn_.receivedAll());
    EXPECT_FALSE(txn_.hasResponse(1));

    // receive the response again
    txn_.receive(1, "", DistributedTransaction::OK);
    EXPECT_TRUE(txn_.receivedAll());

    txn_.readyNextRound();
}

TEST_F(DistributedTransactionTest, PrepareResponseReceived) {
    txn_.send(0, "msg");
    txn_.send(1, "msg");

    // Messages sent and responses received
    txn_.sentMessages();
    
    // Responses received
    txn_.receive(0, "abcd", DistributedTransaction::OK);
    txn_.receive(1, "abcd", DistributedTransaction::OK);
    txn_.readyNextRound();

    // Indicates everyone is prepared
    txn_.setAllDone();
    txn_.sentMessages();

    EXPECT_DEATH(txn_.receive(0, "a", DistributedTransaction::OK));
    txn_.receive(0, "", DistributedTransaction::OK);
    EXPECT_FALSE(txn_.receivedAll());
    txn_.receive(1, "", DistributedTransaction::OK);
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_EQ(2, txn_.received().size());
    txn_.removePrepareResponses();
    EXPECT_EQ(0, txn_.received().size());
    txn_.readyNextRound();
}

TEST_F(DistributedTransactionTest, IndividualPartitionsDone) {
    txn_.send(0, "first");
    txn_.setDone(0);
    txn_.setDone(2);
    txn_.setDone(3);

    EXPECT_FALSE(txn_.isAllDone());
    EXPECT_TRUE(txn_.multiple_partitions());
    EXPECT_TRUE(txn_.isParticipant(0));
    EXPECT_FALSE(txn_.isParticipant(1));
    EXPECT_TRUE(txn_.isDone(0));
    EXPECT_FALSE(txn_.isDone(1));
    EXPECT_TRUE(txn_.isDone(2));

    // Message gets sent
    txn_.sentMessages();

    // Response is received
    txn_.receive(0, "r", DistributedTransaction::OK);
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_EQ(1, txn_.received().size());
    EXPECT_TRUE(txn_.isPrepared(0));
    EXPECT_TRUE(txn_.isParticipant(0));
    EXPECT_FALSE(txn_.isParticipant(1));

    // coordinator sends round two
    txn_.readyNextRound();
    txn_.send(1, "second");
    EXPECT_DEATH(txn_.send(0, "second"));  // marked as done
    EXPECT_DEATH(txn_.send(2, "second"));  // marked as done

    txn_.setDone(1);
    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_TRUE(txn_.multiple_partitions());

    txn_.sentMessages();
    txn_.receive(1, "r", DistributedTransaction::OK);
    EXPECT_TRUE(txn_.receivedAll());
    EXPECT_TRUE(txn_.isPrepared(0));
    EXPECT_TRUE(txn_.isPrepared(1));
    txn_.readyNextRound();
}

TEST_F(DistributedTransactionTest, IndividualPartitionsDoneSetAll) {
    txn_.send(0, "first");
    txn_.setDone(0);
    txn_.setAllDone();
    EXPECT_TRUE(txn_.isAllDone());
    EXPECT_FALSE(txn_.multiple_partitions());
}

TEST_F(DistributedTransactionTest, IndividualPartitionsPrepare) {
    // start and receive a first round
    txn_.send(0, "first");
    txn_.sentMessages();
    txn_.receive(0, "r", DistributedTransaction::OK);
    txn_.readyNextRound();

    // setting a partition done sends a "prepared" message
    txn_.send(1, "second");
    txn_.setDone(0);
    ASSERT_EQ(2, txn_.sent().size());
    EXPECT_EQ(1, txn_.sent()[0].first);
    EXPECT_EQ(0, txn_.sent()[1].first);
    EXPECT_EQ("", txn_.sent()[1].second);

    // receive second round
    txn_.sentMessages();
    txn_.receive(0, "", DistributedTransaction::OK);
    txn_.receive(1, "", DistributedTransaction::OK);
    txn_.readyNextRound();

    // set all done: one prepare goes out
    txn_.setAllDone();
    ASSERT_EQ(1, txn_.sent().size());
    EXPECT_EQ(1, txn_.sent()[0].first);
    EXPECT_EQ("", txn_.sent()[0].second);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
