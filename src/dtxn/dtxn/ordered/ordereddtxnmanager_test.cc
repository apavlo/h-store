// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <tr1/functional>

#include "dtxn/executionengine.h"
#include "dtxn/distributedtransaction.h"
#include "dtxn/messages.h"
#include "dtxn/ordered/ordereddtxnmanager.h"
#include "io/eventloop.h"
#include "mockmessageconnection.h"
#include "net/messageserver.h"
#include "stupidunit/stupidunit.h"

using std::make_pair;
using std::string;
using std::vector;

using namespace dtxn;

class OrderedDtxnManagerTest : public Test {
public:
    static const int NUM_PARTITIONS = 4;

    OrderedDtxnManagerTest() :
            transaction_(NUM_PARTITIONS),
            transaction2_(NUM_PARTITIONS),
            txn_(NULL),
            callback_(std::tr1::bind(&OrderedDtxnManagerTest::callback, this)),
            callback2_(std::tr1::bind(&OrderedDtxnManagerTest::callback2, this)),
            called_(false),
            called2_(false),
            manager_(NULL) {
        for (int i = 0; i < NUM_PARTITIONS; ++i) {
            replicas_.push_back(new MockMessageConnection());
            replica_handles_.push_back(msg_server_.addConnection(replicas_.back()));
        }
        response_.status = ExecutionEngine::OK;
        manager_ = new OrderedDtxnManager(&event_loop_, &msg_server_, replica_handles_);
    }

    ~OrderedDtxnManagerTest() {
        delete manager_;
    }

    void callback() {
        delete txn_;
        txn_ = NULL;
        called_ = true;
    }

    void callback2() {
        called2_ = true;
    }

    DistributedTransaction transaction_;
    DistributedTransaction transaction2_;
    DistributedTransaction* txn_;
    std::tr1::function<void()> callback_;
    std::tr1::function<void()> callback2_;
    bool called_;
    bool called2_;

    vector<MockMessageConnection*> replicas_;
    vector<net::ConnectionHandle*> replica_handles_;
    io::MockEventLoop event_loop_;
    net::MessageServer msg_server_;
    OrderedDtxnManager* manager_;

    Fragment fragment_;
    FragmentResponse response_;
    dtxn::CommitDecision decision_;
};

TEST_F(OrderedDtxnManagerTest, BadCreate) {
    vector<net::ConnectionHandle*> replica_handles;
    EXPECT_DEATH(OrderedDtxnManager(&event_loop_, &msg_server_, replica_handles));

    EXPECT_DEATH(OrderedDtxnManager(NULL, &msg_server_, replica_handles_));
    EXPECT_DEATH(OrderedDtxnManager(&event_loop_, NULL, replica_handles_));
};

TEST_F(OrderedDtxnManagerTest, SetCallbackTargets) {
    EXPECT_EQ(true, replicas_[0]->target() != NULL);
    EXPECT_EQ(true, replicas_[1]->target() != NULL);
    delete manager_;
    manager_ = NULL;
    EXPECT_FALSE(msg_server_.registered(FragmentResponse::typeCode()));
};

TEST_F(OrderedDtxnManagerTest, BadExecute) {
    // Execute without data not okay
    EXPECT_DEATH(manager_->execute(&transaction_, callback_));

    // Execute before receiving responses: not okay
    transaction_.send(0, "work");
    manager_->execute(&transaction_, callback_);
    EXPECT_DEATH(manager_->execute(&transaction_, callback_));
}

TEST_F(OrderedDtxnManagerTest, SinglePartitionCommit) {
    // A single partition request.
    transaction_.send(0, "work");
    transaction_.setAllDone();
    EXPECT_FALSE(transaction_.multiple_partitions());
    manager_->execute(&transaction_, callback_);

    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(0, fragment_.id);
    EXPECT_EQ(false, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("work", fragment_.transaction);
    EXPECT_EQ(false, replicas_[1]->hasMessage());
    EXPECT_FALSE(called_);

    // Commit the single partition transaction
    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    response_.result = "foo";
    manager_->responseReceived(replica_handles_[0], response_);

    // Response should get returned via callback
    EXPECT_TRUE(called_);
    EXPECT_EQ(DistributedTransaction::OK, transaction_.status());
    EXPECT_EQ(1, transaction_.received().size());
    EXPECT_EQ(0, transaction_.received()[0].first);
    EXPECT_EQ("foo", transaction_.received()[0].second);
}

TEST_F(OrderedDtxnManagerTest, SinglePartitionAbort) {
    // A single partition request is aborted
    // A single partition request is committed and deleted in the callback
    transaction_.send(0, "work");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    response_.id = 0;
    response_.status = ExecutionEngine::ABORT_USER;
    response_.result = "error";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);

    EXPECT_EQ(DistributedTransaction::ABORT_USER, transaction_.status());
    EXPECT_EQ(1, transaction_.received().size());
    EXPECT_EQ(0, transaction_.received()[0].first);
    EXPECT_EQ("error", transaction_.received()[0].second);
}

TEST_F(OrderedDtxnManagerTest, SinglePartitionMultipleRequestsDeleteCallback) {
    // A single partition request is committed and deleted in the callback
    txn_ = new DistributedTransaction(NUM_PARTITIONS);
    txn_->send(0, "work");
    txn_->setAllDone();
    manager_->execute(txn_, callback_);
    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    EXPECT_EQ(NULL, txn_);

    // perform a second request
    called_ = false;

    transaction_.send(1, "morework");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    response_.id = 1;
    response_.status = ExecutionEngine::OK;
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_TRUE(called_);
    EXPECT_EQ(DistributedTransaction::OK, transaction_.status());
}

TEST_F(OrderedDtxnManagerTest, OneShotCommit) {
    // Multi-partition request
    transaction_.send(0, "one");
    transaction_.send(1, "two");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);

    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(0, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("one", fragment_.transaction);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(0, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("two", fragment_.transaction);
    EXPECT_FALSE(called_);

    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    response_.result = "oneout";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(called_);
    response_.result = "twoout";
    manager_->responseReceived(replica_handles_[1], response_);

    EXPECT_TRUE(called_);
    ASSERT_EQ(2, transaction_.received().size());
    EXPECT_EQ(0, transaction_.received()[0].first);
    EXPECT_EQ("oneout", transaction_.received()[0].second);
    EXPECT_EQ(1, transaction_.received()[1].first);
    EXPECT_EQ("twoout", transaction_.received()[1].second);

    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // Commit messages should go out after we call finish
    manager_->finish(&transaction_, true, callback_);
    replicas_[0]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(true, decision_.commit);
    replicas_[1]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(true, decision_.commit);
}

TEST_F(OrderedDtxnManagerTest, OneShotPartitionAbort) {
    // Multi-partition request
    transaction_.send(0, "one");
    transaction_.send(1, "two");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);

    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    response_.result = "oneout";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(called_);
    response_.status = ExecutionEngine::ABORT_USER;
    response_.result = "twoerror";
    manager_->responseReceived(replica_handles_[1], response_);

    EXPECT_TRUE(called_);
    ASSERT_EQ(1, transaction_.received().size());
    EXPECT_EQ(1, transaction_.received()[0].first);
    EXPECT_EQ("twoerror", transaction_.received()[0].second);

    // Abort messages should go out automatically
    replicas_[0]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(false, decision_.commit);
    replicas_[1]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(false, decision_.commit);

    // Can't call finish on an aborted transaction
    EXPECT_DEATH(manager_->finish(&transaction_, false, callback_));
}

TEST_F(OrderedDtxnManagerTest, SinglePartitionQueuing) {
    // A client sends a single partition request. It should get passed through.
    transaction_.send(0, "one");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(0, fragment_.id);
    EXPECT_EQ("one", fragment_.transaction);

    // A second single partition request also gets passed through
    transaction2_.send(0, "two");
    transaction2_.setAllDone();
    manager_->execute(&transaction2_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(1, fragment_.id);
    EXPECT_EQ("two", fragment_.transaction);

    // Commit out of order: second transaction first
    EXPECT_FALSE(called_);
    response_.id = 1;
    response_.status = ExecutionEngine::OK;
    response_.result = "twoout";
    manager_->responseReceived(replica_handles_[0], response_);

    EXPECT_TRUE(called_);
    called_ = false;
    ASSERT_EQ(1, transaction2_.received().size());
    EXPECT_EQ(0, transaction2_.received()[0].first);
    EXPECT_EQ("twoout", transaction2_.received()[0].second);

    // Queue another transaction to get passed through
    DistributedTransaction three(NUM_PARTITIONS);
    three.send(0, "three");
    three.setAllDone();
    manager_->execute(&three, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(2, fragment_.id);
    EXPECT_EQ("three", fragment_.transaction);

    // Commit the first transaction
    response_.id = 0;
    response_.result = "oneout";
    manager_->responseReceived(replica_handles_[0], response_);

    EXPECT_TRUE(called_);
    called_ = false;
    ASSERT_EQ(1, transaction_.received().size());
    EXPECT_EQ(0, transaction_.received()[0].first);
    EXPECT_EQ("oneout", transaction_.received()[0].second);

    // Commit the final transcation
    response_.id = 2;
    response_.result = "threeout";
    manager_->responseReceived(replica_handles_[0], response_);

    EXPECT_TRUE(called_);
    ASSERT_EQ(1, three.received().size());
    EXPECT_EQ(0, three.received()[0].first);
    EXPECT_EQ("threeout", three.received()[0].second);
}

TEST_F(OrderedDtxnManagerTest, SinglePartitionMultipleRounds) {
    // Start a request
    transaction_.send(0, "one");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_TRUE(fragment_.multiple_partitions);
    EXPECT_FALSE(fragment_.last_fragment);
    response_.id = 0;
    response_.result = "oneout";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    called_ = false;

    // Send second and final round: "downgrade" to single partition
    transaction_.send(0, "done");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    // TODO: It would be better to represent this by adding a "txn_state" field: OPEN, PREPARE, COMMIT
    // then we could treat single partition and multi-partition transactions the same way at the participants
    EXPECT_FALSE(fragment_.multiple_partitions);
    EXPECT_TRUE(fragment_.last_fragment);
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);

    EXPECT_DEATH(manager_->finish(&transaction_, true, callback_));
}

TEST_F(OrderedDtxnManagerTest, SinglePartitionMultipleRoundQueuing) {
    // Send round 1
    transaction_.send(1, "round1");
    manager_->execute(&transaction_, callback_);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(0, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);

    // Start another multi-partition transaction: blocked
    transaction2_.send(0, "sp");
    manager_->execute(&transaction2_, callback2_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    
    // reply to the blocked mp transaction
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_TRUE(called_);
    called_ = false;

    // round 2: this becomes single partition; this and the next txn should be sent
    transaction_.send(1, "round2");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(false, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);

    // Finish transaction2
    response_.id = 1;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called2_);
    manager_->finish(&transaction2_, true, callback2_);
    replicas_[0]->getMessage(&fragment_);

    // Start another multi-partition transaction: should get passed through
    // This previously was a bug: the wrong partitions were getting marked as "done"
    DistributedTransaction transaction3(NUM_PARTITIONS);
    transaction3.send(0, "mp");
    transaction3.send(1, "mp");
    manager_->execute(&transaction3, callback2_);
    EXPECT_TRUE(replicas_[0]->hasMessage());
    EXPECT_TRUE(replicas_[1]->hasMessage());
}

TEST_F(OrderedDtxnManagerTest, MultiplePartitionQueuingWithSinglePartition) {
    // Start multi-partition
    transaction_.send(0, "round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);

    // Start and finish a single partition transaction
    transaction2_.send(1, "sp");
    transaction2_.setAllDone();
    manager_->execute(&transaction2_, callback2_);
    replicas_[1]->getMessage(&fragment_);
    response_.id = 1;
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_TRUE(called2_);
    called2_ = false;

    // Finish the multi-partition transaction
    // This was previously causing an invalid access to the ordereddtxnmanager's queue
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    called_ = false;
    manager_->finish(&transaction_, false, callback_);
    EXPECT_TRUE(called_);
}

// Start an sp, then an mp: sp does not block the mp
TEST_F(OrderedDtxnManagerTest, SinglePartitionNotBlockingMultiplePartition) {
    // Start single partition
    transaction_.send(0, "round1");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);

    // Start a multi-partition transaction: it should get passed through
    transaction2_.send(0, "mp");
    manager_->execute(&transaction2_, callback2_);
    replicas_[0]->getMessage(&fragment_);
}

TEST_F(OrderedDtxnManagerTest, TwoRoundCommit) {
    // Send round 1
    transaction_.send(0, "round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);
    EXPECT_EQ("round1", fragment_.transaction);
    EXPECT_EQ(false, replicas_[1]->hasMessage());

    // Received round 1 response
    response_.status = ExecutionEngine::OK;
    response_.result = "round1out";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    called_ = false;
    ASSERT_EQ(1, transaction_.received().size());
    EXPECT_EQ(0, transaction_.received()[0].first);
    EXPECT_EQ("round1out", transaction_.received()[0].second);

    // Send round 2 and finish with a different callback
    transaction_.send(1, "round2");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback2_);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("round2", fragment_.transaction);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("", fragment_.transaction);

    // Round 2 response
    response_.result = "two";
    EXPECT_FALSE(called2_);
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_FALSE(called2_);
    EXPECT_DEATH(manager_->responseReceived(replica_handles_[0], response_));
    response_.result = "";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called2_);
    called2_ = false;
    ASSERT_EQ(1, transaction_.received().size());
    EXPECT_EQ(1, transaction_.received()[0].first);
    EXPECT_EQ("two", transaction_.received()[0].second);

    // Commit
    EXPECT_FALSE(called_);
    manager_->finish(&transaction_, true, callback_);
    replicas_[0]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(true, decision_.commit);
    replicas_[1]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(true, decision_.commit);
    EXPECT_TRUE(called_);
}

TEST_F(OrderedDtxnManagerTest, TwoRoundQueuing) {
    // Send round 1
    transaction_.send(0, "round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(0, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);

    // Start a single partition transaction: passed through.
    transaction2_.send(0, "sp");
    transaction2_.setAllDone();
    manager_->execute(&transaction2_, callback2_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(1, fragment_.id);
    EXPECT_EQ(false, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);

    // Start a couple one shot multi-partition transactions: blocked
    DistributedTransaction txn3(NUM_PARTITIONS);
    txn3.send(0, "txn3");
    txn3.send(1, "txn3");
    txn3.setAllDone();
    manager_->execute(&txn3, callback_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());
    DistributedTransaction txn4(NUM_PARTITIONS);
    txn4.send(1, "txn4");
    manager_->execute(&txn4, callback_);

    // reply to the blocked mp transaction: crash
    response_.id = 2;
    EXPECT_DEATH(manager_->responseReceived(replica_handles_[1], response_));

    // reply to single partition transaction: finished
    response_.id = 1;
    response_.status = ExecutionEngine::OK;
    response_.result = "sp1";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called2_);

    // reply to first mp txn
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    called_ = false;

    // finish first mp txn: txn 3 and 4 both go out (finished)
    transaction_.send(1, "round2");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(2, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(3, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);
}

// Start a mp txn; send sp txn, finish mp txn in second round: bug caused sp to be resent
TEST_F(OrderedDtxnManagerTest, TwoRoundQueuingSPUnfinished) {
    // Send round 1
    transaction_.send(0, "round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);

    // Start a single partition transaction: passed through.
    transaction2_.send(0, "sp");
    transaction2_.setAllDone();
    manager_->execute(&transaction2_, callback2_);
    replicas_[0]->getMessage(&fragment_);

    // reply to first mp txn
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);

    // send last round for mp txn
    transaction_.send(0, "round2");
    transaction_.setAllDone();
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    // OLD BUG: this previously sent sp txn again, and id = 1 (if one partition), or it crashed
    EXPECT_EQ(0, fragment_.id);
}

TEST_F(OrderedDtxnManagerTest, TwoRoundQueuingAbort) {
    // Send round 1
    transaction_.send(0, "round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(0, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);

    // Start a multi-partition transaction
    transaction2_.send(1, "sp");
    manager_->execute(&transaction2_, callback2_);
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // Abort round 1: other transaction goes out
    response_.id = 0;
    response_.status = ExecutionEngine::ABORT_USER;
    response_.result = "";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(1, fragment_.id);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);
}

TEST_F(OrderedDtxnManagerTest, UnfinishedCommit) {
    // Send round 1
    transaction_.send(0, "0round1");
    transaction_.send(1, "1round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);
    replicas_[1]->getMessage(&fragment_);

    // Received round 1 responses
    response_.status = ExecutionEngine::OK;
    response_.result = "out";
    manager_->responseReceived(replica_handles_[0], response_);
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_TRUE(called_);
    called_ = false;

    // Commit: this requires prepare messages to be sent out.
    manager_->finish(&transaction_, true, callback_);

    // Prepare
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("", fragment_.transaction);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("", fragment_.transaction);

    // responses: do not expect any data to come back; transaction committed at this point
    EXPECT_DEATH(manager_->responseReceived(replica_handles_[0], response_));
    response_.result = "";
    manager_->responseReceived(replica_handles_[0], response_);
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_TRUE(called_);

    // Commit finally goes out
    replicas_[0]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(true, decision_.commit);
    replicas_[1]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(true, decision_.commit);
}

TEST_F(OrderedDtxnManagerTest, UnfinishedCommitSinglePartition) {
    // Send unfinished round 1
    transaction_.send(0, "0round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);

    // Received round 1 responses
    response_.status = ExecutionEngine::OK;
    response_.result = "out";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    called_ = false;

    // Commit: this ends up becoming a single partition transaction.
    manager_->finish(&transaction_, true, callback_);

    // Prepare (and commit)
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(false, fragment_.multiple_partitions);
    EXPECT_EQ(true, fragment_.last_fragment);
    EXPECT_EQ("", fragment_.transaction);
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // responses: do not expect any data to come back; transaction committed at this point
    EXPECT_DEATH(manager_->responseReceived(replica_handles_[0], response_));
    response_.result = "";
    EXPECT_FALSE(called_);
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);

    // No decision sent out: this became single partition!
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());
}

TEST_F(OrderedDtxnManagerTest, UnfinishedAbort) {
    // Send round 1
    transaction_.send(0, "0round1");
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);

    // Received round 1 responses
    response_.status = ExecutionEngine::OK;
    response_.result = "out";
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    called_ = false;

    // abort
    manager_->finish(&transaction_, false, callback_);
    replicas_[0]->getMessage(&decision_);
    EXPECT_EQ(0, decision_.id);
    EXPECT_EQ(false, decision_.commit);
    EXPECT_TRUE(called_);
}


TEST_F(OrderedDtxnManagerTest, DisjointTxnsNotBlocking) {
    // txn1: 0 involved, 1, 2 done, 3 unknown
    transaction_.send(0, "0round1");
    transaction_.setDone(1);
    transaction_.setDone(2);
    manager_->execute(&transaction_, callback_);
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(true, fragment_.multiple_partitions);
    EXPECT_EQ(false, fragment_.last_fragment);

    // txn2: 0 unknown, 1, 2 involved, 3 done
    transaction2_.send(1, "3round1");
    transaction2_.send(2, "3round1");
    transaction2_.setDone(3);
    manager_->execute(&transaction2_, callback2_);
    replicas_[1]->getMessage(&fragment_);
    replicas_[2]->getMessage(&fragment_);

    // txn3: 1, 3 involved
    DistributedTransaction txn3(NUM_PARTITIONS);
    txn3.send(0, "0");
    txn3.send(3, "3");
    manager_->execute(&txn3, callback2_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[3]->hasMessage());

    // txn2: responded and send round to 0, 1: blocked due to txn1
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    manager_->responseReceived(replica_handles_[1], response_);
    manager_->responseReceived(replica_handles_[2], response_);
    EXPECT_TRUE(called2_);
    called2_ = false;
    transaction2_.send(0, "round2");
    transaction2_.send(1, "round2");
    transaction2_.setAllDone();
    manager_->execute(&transaction2_, callback2_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // txn1: responded and finished
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_TRUE(called_);
    called_ = false;
    manager_->finish(&transaction_, true, callback_);
//    replicas_[0]->getMessage(&decision_);
//    EXPECT_EQ(0, decision_.id);

    // txn1 finishing causes txn2 round 2 to be sent
//    replicas_[0]->getMessage(&fragment_);
    replicas_[1]->getMessage(&fragment_);
    EXPECT_EQ(1, fragment_.id);
    EXPECT_EQ(true, fragment_.last_fragment);

    // txn2 last fragment being sent causes txn3 to be sent
    replicas_[0]->getMessage(&fragment_);
    EXPECT_EQ(2, fragment_.id);
    EXPECT_EQ(false, fragment_.last_fragment);
    replicas_[2]->getMessage(&fragment_);
}


#ifdef FOO
TEST_F(OrderedDtxnManagerTest, TransactionIds) {
    createManager(new SingleDistributor());

    // Bad txn ids
    transaction_.id = -1;
    EXPECT_DEATH(manager_->requestReceived(client_handle_, transaction_));

    transaction_.id = 0;
    manager_->requestReceived(client_handle_, transaction_);

    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(0, transaction_.id);

    // this is ignored: we assume it is an old response from an aborted transaction
    response_.id = -1;  // ignored
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(client_->hasMessage());
    response_.id = -2;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(client_->hasMessage());
    response_.id = 1;
    EXPECT_DEATH(manager_->responseReceived(replica_handles_[0], response_));
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);

    MockMessageConnection* other = new MockMessageConnection();
    net::ConnectionHandle* other_handle = msg_server_.addConnection(other);

    // other sends txn id = 0; distributor sends txn id = 1
    manager_->requestReceived(other_handle, transaction_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(1, transaction_.id);

    // original client sends another transaction which finishes first
    transaction_.id = 1;
    manager_->requestReceived(client_handle_, transaction_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(2, transaction_.id);
    response_.id = 2;
    manager_->responseReceived(replica_handles_[0], response_);
    client_->getMessage(&response_);
    EXPECT_EQ(1, response_.id);

    // other client's transaction finishes
    response_.id = 0;  // ignored
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(other->hasMessage());
    response_.id = -1;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(other->hasMessage());
    response_.id = 2;
    EXPECT_DEATH(manager_->responseReceived(replica_handles_[0], response_));
    response_.id = 1;
    manager_->responseReceived(replica_handles_[0], response_);
    other->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
}

TEST_F(OrderedDtxnManagerTest, SinglePartitionClientClose) {
    createManager(new SingleDistributor());

    // A client sends a single partition request to the sequencer. It should get passed through.
    manager_->requestReceived(client_handle_, transaction_);

    // second request which completes
    transaction_.id = 1;
    manager_->requestReceived(client_handle_, transaction_);
    response_.id = 1;
    manager_->responseReceived(replica_handles_[0], response_);

    // The client connection gets closed, while the transaction is pending
    msg_server_.connectionClosed(client_);
    client_ = NULL;

    // the single partition transaction completes
    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    response_.result = "foo";
    manager_->responseReceived(replica_handles_[0], response_);
}

class OneShotCoordinator : public MockCoordinator {
public:
    OneShotCoordinator() : destination_a_(0), destination_b_(1) {}

    virtual void doFragment(CoordinatedTransaction* transaction) {
        if (transaction->received().size() == 1) {
            transaction->send(destination_a_, "one");
            transaction->send(destination_b_, "two");
            transaction->setAllDone();
        } else {
            assert(transaction->received().size() == 2);
            transaction->commit("output");
        }
    }

    int destination_a_;
    int destination_b_;
};

TEST_F(OrderedDtxnManagerTest, OneShotDeadlockAbort) {
    createManager(new OneShotCoordinator());
    manager_->requestReceived(client_handle_, transaction_);

    // Abort with a message
    response_.status = ExecutionEngine::ABORT_USER;
    response_.result = "foo";
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_EQ(1, coordinator_->active_count_);
    response_.status = ExecutionEngine::OK;

    // abort with a deadlock: no message should go out
    response_.status = ExecutionEngine::ABORT_DEADLOCK;
    response_.result.clear();
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_EQ(0, coordinator_->active_count_);

    // Abort message should go out.
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::ABORT_DEADLOCK, response_.status);
    EXPECT_EQ("", response_.result);
}

TEST_F(OrderedDtxnManagerTest, OneShotTimeout) {
    createManager(new OneShotCoordinator());
    manager_->requestReceived(client_handle_, transaction_);

    // A timeout is created
    EXPECT_EQ(200, event_loop_.last_timeout_->milliseconds_);

    // trigger the timeout
    // TODO: Test that the timeout is reset?
    event_loop_.triggerLastTimeout();

    // Abort messages should go out.
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, response_.status);
    EXPECT_EQ("", response_.result);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);

    // another request is sent out and times out, and is aborted
    transaction_.id = 1;
    manager_->requestReceived(client_handle_, transaction_);
    event_loop_.triggerLastTimeout();
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);

    // If response messages come back due to timing issues, they should be ignored
    manager_->responseReceived(replica_handles_[0], response_);
    manager_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    manager_->responseReceived(replica_handles_[0], response_);
    manager_->responseReceived(replica_handles_[1], response_);
}

class DependencyTest : public OrderedDtxnManagerTest {
public:
    DependencyTest() : OrderedDtxnManagerTest() {
        // Add a third connection
        replicas_.push_back(new MockMessageConnection());
        replica_handles_.push_back(msg_server_.addConnection(replicas_[2]));
        createManager(new OneShotCoordinator());
    }

    void setDestinations(int a, int b) {
        OneShotCoordinator* coordinator = (OneShotCoordinator*) coordinator_;
        coordinator->destination_a_ = a;
        coordinator->destination_b_ = b;
    }
};

TEST_F(DependencyTest, DependencyCommit) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    manager_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // start 3rd multi-partition
    transaction_.id = 2;
    setDestinations(1, 2);
    manager_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(3, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from the 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[1], response_);
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[1], response_);

    // get both responses from the 3rd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[2], response_);
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[2], response_);

    // no one has messages
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get commit from first replica
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);

    // all commits go out

    // Send out the abort messages
    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[2]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_EQ(true, decision.commit);
}

// A: 1
// B: 1 2
// C: - 2
// receive B, A, C
TEST_F(DependencyTest, DependencyFinishBeforeDependentReady) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    manager_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from the 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[1], response_);

    // get response from first replica
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get response from the 3rd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[2], response_);

    EXPECT_FALSE(replicas_[0]->hasMessage());
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[2]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_EQ(true, decision.commit);
}

// A: 1
// B: 1 2
// C: - 2
// receive B, A, C
TEST_F(DependencyTest, DependencyPartlyDoneOnArrival) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    manager_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from the 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[1], response_);

    // get response from first replica
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get response from the 3rd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[2], response_);

    EXPECT_FALSE(replicas_[0]->hasMessage());
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[2]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_EQ(true, decision.commit);
}


// A: 1
// B: 1 2
// C: - 2
// receive B1, A, B2, C
TEST_F(DependencyTest, DependencyDoneOnArrival) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    manager_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get 1st response from 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[1], response_);

    // get response from first replica
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get 2nd response from 2; response from 3
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[1], response_);
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[2], response_);

    EXPECT_FALSE(replicas_[0]->hasMessage());
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[2]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_EQ(true, decision.commit);
}

// A: 1
// B: 1 2
// C: - 2
// receive B, C, A aborts, B repeats 2, done
TEST_F(DependencyTest, DependencyAbort) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    manager_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[1], response_);

    // get response from third replica
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[2], response_);

    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get abort from first replica
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get the repeat from 2: answer goes out
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[1], response_);

    EXPECT_FALSE(replicas_[0]->hasMessage());
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[2]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
}

// A: 1 2
// B: 1 2
// receive B, A aborts
TEST_F(DependencyTest, DependencyAbortPartialReceived) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // get responses from 1st replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[0], response_);

    // get abort from 2nd replica
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[1], response_);

    // get the repeat from 1 and answer from 2: answer goes out
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);
    manager_->responseReceived(replica_handles_[1], response_);

    CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
}

// A: 1 2
// B: 1 2
// receive B1, A aborts 1, b2 arrives with dependency
TEST_F(DependencyTest, DependencyAbortAlreadyFinished) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // response for b1
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[1], response_);

    // get a1
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);

    // abort goes out
    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);

    // get a2
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    manager_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // get b2 with dependency: ignored
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[1], response_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // get b2 without dependency: done
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
}

// A: 1 2 3
// B: 1 2 3
// receive A1, B1 aborts, A2, A3 (depending on A1), A2, A3 (new chain), B2, B3
TEST_F(DependencyTest, DependencyAbortChainAlreadyFinished) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);
    transaction_.id = 1;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);
    transaction_.id = 2;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // response for a1
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);

    // abort b1
    response_.status = ExecutionEngine::ABORT_USER;
    manager_->responseReceived(replica_handles_[1], response_);

    // abort goes out
    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);

    // get a2, a3
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[0], response_);

    // get a2, a3 with new dependency
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[0], response_);

    // get b2, b3 with dependency: ignored
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);

    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_TRUE(decision.commit);
}

// A: 1 2 3
// B: 1 2 3
// receive A1, A2, A3, B1 aborts, A2, A3, B2, B3
TEST_F(DependencyTest, DependencyAbortChain) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);
    transaction_.id = 1;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);
    transaction_.id = 2;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // responses for a
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[0], response_);

    // abort b1
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[1], response_);

    // abort goes out
    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);

    // get a2, a3
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[0], response_);

    // get b2, b3
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);

    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_TRUE(decision.commit);
}

// A: 1 2 3
// B: 1 2 3
// receive A1, A2, A3, B1 aborts, A2, B2, A3, B3
TEST_F(DependencyTest, DependencyAbortChainAlt) {
    // start 1st multi-partition
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);
    transaction_.id = 1;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);
    transaction_.id = 2;
    setDestinations(0, 1);
    manager_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // responses for a
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 1;
    response_.dependency = 0;
    manager_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[0], response_);

    // abort b1
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[1], response_);

    // abort goes out
    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_FALSE(decision.commit);

    // get a2, b2
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    manager_->responseReceived(replica_handles_[0], response_);
    manager_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);

    // get a3, b3
    response_.id = 2;
    response_.dependency = 1;
    manager_->responseReceived(replica_handles_[0], response_);
    manager_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(2, decision.id);
    EXPECT_TRUE(decision.commit);
}

class TwoRoundDistributor : public MockCoordinator {
public:
    virtual void doFragment(CoordinatedTransaction* transaction) {
        assert(transaction->received().size() == 1);
        if (transaction->received()[0].first == CoordinatedTransaction::CLIENT_PARTITION) {
            if (transaction->received()[0].second == "single") {
                transaction->send(0, "single");
                transaction->setAllDone();
            } else {
                assert(transaction->received()[0].second == "start");
                transaction->send(0, "one");
            }
        } else if (transaction->received()[0].second == "one") {
            assert(transaction->received()[0].first == 0);
            transaction->send(1, "two");
            transaction->setAllDone();
        } else {
            if (transaction->received()[0].first == 0) {
                assert(transaction->received()[0].second == "single");
                transaction->commit("done single");
            } else {
                assert(transaction->received()[0].first == 1);
                transaction->commit("done multiple");
            }
        }
    }
};

TEST_F(OrderedDtxnManagerTest, TwoRoundClientClose) {
    // Start the transaction
    createManager(new TwoRoundDistributor());
    transaction_.transaction = "start";
    manager_->requestReceived(client_handle_, transaction_);

    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(false, replicas_[1]->hasMessage());

    // The client connection is closed
    msg_server_.connectionClosed(client_);
    client_ = NULL;

    // We continue to execute the transaction
    response_.status = ExecutionEngine::OK;
    response_.result = "one";
    manager_->responseReceived(replica_handles_[0], response_);

    replicas_[1]->getMessage(&transaction_);
    EXPECT_EQ(false, replicas_[0]->hasMessage());

    response_.result = "two";
    manager_->responseReceived(replica_handles_[1], response_);

    // Partitions still get messages
    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
}

#endif

int main() {
    return TestSuite::globalInstance()->runAll();
}
