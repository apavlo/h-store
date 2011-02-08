// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <tr1/functional>

#include "dtxn/distributedtransaction.h"
#include "dtxn/dtxnmanager.h"
#include "dtxn/mockdtxnmanager.h"
#include "protodtxn/protodtxncoordinator.h"
#include "stupidunit/stupidunit.h"

using dtxn::DistributedTransaction;
using std::string;

using namespace protodtxn;

class ProtoCoordinatorTest : public Test, public google::protobuf::Closure {
public:
    ProtoCoordinatorTest () :
            coordinator_(&manager_, 4),
            called_(false) {
        request_.set_transaction_id(0);
        finish_request_.set_transaction_id(0);
        finish_request_.set_commit(true);
    }

    ~ProtoCoordinatorTest() {
    }

    void Run() {
        called_ = true;
    }

    dtxn::MockDtxnManager manager_;
    ProtoDtxnCoordinator coordinator_;
    CoordinatorFragment request_;
    CoordinatorResponse response_;

    FinishRequest finish_request_;
    FinishResponse finish_response_;

    bool called_;
};

TEST(ProtoDtxn, StatusEnum) {
    EXPECT_EQ((int) FragmentResponse::OK, (int) DistributedTransaction::OK);
    EXPECT_EQ((int) FragmentResponse::ABORT_USER, (int) DistributedTransaction::ABORT_USER);
    EXPECT_EQ((int) FragmentResponse::ABORT_DEADLOCK, (int) DistributedTransaction::ABORT_DEADLOCK);
}

TEST_F(ProtoCoordinatorTest, BadRequests) {
    // No fragments!
    request_.set_last_fragment(true);
    EXPECT_DEATH(coordinator_.Execute(NULL, &request_, &response_, this));

    // Two fragments to one destination
    CoordinatorFragment::PartitionFragment* fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("work");
    fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("work2");
    EXPECT_DEATH(coordinator_.Execute(NULL, &request_, &response_, this));
}

TEST_F(ProtoCoordinatorTest, SinglePartition) {
    request_.set_last_fragment(true);
    CoordinatorFragment::PartitionFragment* fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("work");

    // Receive the work
    coordinator_.Execute(NULL, &request_, &response_, this);
    EXPECT_FALSE(manager_.transaction_->multiple_partitions());
    EXPECT_TRUE(manager_.transaction_->isAllDone());
    EXPECT_EQ(1, manager_.transaction_->sent().size());
    EXPECT_EQ(0, manager_.transaction_->sent()[0].first);
    EXPECT_EQ("work", manager_.transaction_->sent()[0].second);
    EXPECT_FALSE(called_);

    // Receive responses: passed back to the client
    manager_.transaction_->receive(0, "output", DistributedTransaction::OK);
    manager_.callback_();

    EXPECT_TRUE(called_);
    called_ = false;
    EXPECT_EQ(1, response_.response_size());
    EXPECT_EQ(0, response_.response(0).partition_id());
    EXPECT_EQ("output", response_.response(0).output());

    // ProtoDtxn requires .Finish() to be called (but it must match! it must commit in this case)
    finish_request_.set_commit(false);
    EXPECT_DEATH(coordinator_.Finish(NULL, &finish_request_, &finish_response_, this));
    finish_request_.set_commit(true);
    coordinator_.Finish(NULL, &finish_request_, &finish_response_, this);
    // TODO: Should DtxnManager also want .finish for single partition transactions?
    EXPECT_FALSE(manager_.commit_);
    EXPECT_TRUE(called_);
}

TEST_F(ProtoCoordinatorTest, OneShotCommit) {
    request_.set_last_fragment(true);
    CoordinatorFragment::PartitionFragment* fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("zero");
    fragment = request_.add_fragment();
    fragment->set_partition_id(1);
    fragment->set_work("one");

    // Receive the work
    coordinator_.Execute(NULL, &request_, &response_, this);
    EXPECT_TRUE(manager_.transaction_->multiple_partitions());
    EXPECT_TRUE(manager_.transaction_->isAllDone());
    ASSERT_EQ(2, manager_.transaction_->sent().size());
    EXPECT_EQ(0, manager_.transaction_->sent()[0].first);
    EXPECT_EQ("zero", manager_.transaction_->sent()[0].second);
    EXPECT_EQ(1, manager_.transaction_->sent()[1].first);
    EXPECT_EQ("one", manager_.transaction_->sent()[1].second);
    EXPECT_FALSE(called_);

    // Receive responses: passed back to the client
    manager_.transaction_->receive(0, "zeroout", DistributedTransaction::OK);
    manager_.transaction_->receive(1, "oneout", DistributedTransaction::OK);
    manager_.callback_();

    EXPECT_TRUE(called_);
    ASSERT_EQ(2, response_.response_size());
    EXPECT_EQ(0, response_.response(0).partition_id());
    EXPECT_EQ("zeroout", response_.response(0).output());
    EXPECT_EQ(1, response_.response(1).partition_id());
    EXPECT_EQ("oneout", response_.response(1).output());

    // Commit the transaction
    manager_.transaction_ = NULL;
    called_ = false;
    coordinator_.Finish(NULL, &finish_request_, &finish_response_, this);
    EXPECT_TRUE(manager_.transaction_ != NULL);
    EXPECT_TRUE(manager_.commit_);
    EXPECT_FALSE(called_);
    manager_.callback_();
    EXPECT_TRUE(called_);
}

TEST_F(ProtoCoordinatorTest, OneShotParticipantAbort) {
    request_.set_last_fragment(true);
    CoordinatorFragment::PartitionFragment* fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("zero");
    fragment = request_.add_fragment();
    fragment->set_partition_id(1);
    fragment->set_work("one");

    // Receive the work
    coordinator_.Execute(NULL, &request_, &response_, this);

    // Receive responses: passed back to the client
    manager_.transaction_->receive(0, "zeroout", DistributedTransaction::OK);
    manager_.transaction_->receive(1, "", DistributedTransaction::ABORT_USER);
    manager_.callback_();
    manager_.callback_ = NULL;

    EXPECT_TRUE(called_);
    ASSERT_EQ(1, response_.response_size());
    EXPECT_EQ(FragmentResponse::ABORT_USER, response_.status());
    EXPECT_EQ(1, response_.response(0).partition_id());
    EXPECT_EQ("", response_.response(0).output());

    // We must call Finish with abort
    called_ = false;
    EXPECT_DEATH(coordinator_.Finish(NULL, &finish_request_, &finish_response_, this));
    finish_request_.set_commit(false);
    coordinator_.Finish(NULL, &finish_request_, &finish_response_, this);
    // DtxnManager does *not* get the finish call
    EXPECT_EQ(NULL, manager_.callback_);
    EXPECT_TRUE(called_);
}

TEST_F(ProtoCoordinatorTest, GeneralTransaction) {
    CoordinatorFragment::PartitionFragment* fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("work");

    coordinator_.Execute(NULL, &request_, &response_, this);
    ASSERT_EQ(1, manager_.transaction_->sent().size());
    EXPECT_EQ(0, manager_.transaction_->sent()[0].first);
    EXPECT_EQ("work", manager_.transaction_->sent()[0].second);
    EXPECT_FALSE(called_);

    // Receive responses: passed back to the client
    manager_.transaction_->receive(0, "zeroout", DistributedTransaction::OK);
    manager_.callback_();
    EXPECT_TRUE(called_);
    called_ = false;
    ASSERT_EQ(1, response_.response_size());
    EXPECT_EQ(FragmentResponse::OK, response_.status());
    EXPECT_EQ(0, response_.response(0).partition_id());
    EXPECT_EQ("zeroout", response_.response(0).output());    

    // Finish the transaction with round 2
    fragment->set_partition_id(1);
    fragment->set_work("round2");
    request_.set_last_fragment(true);

    response_.Clear();
    coordinator_.Execute(NULL, &request_, &response_, this);
    ASSERT_EQ(2, manager_.transaction_->sent().size());
    EXPECT_EQ(1, manager_.transaction_->sent()[0].first);
    EXPECT_EQ("round2", manager_.transaction_->sent()[0].second);
    EXPECT_EQ(0, manager_.transaction_->sent()[1].first);
    EXPECT_EQ("", manager_.transaction_->sent()[1].second);
    EXPECT_FALSE(called_);

    manager_.transaction_->receive(1, "oneout", DistributedTransaction::OK);
    manager_.transaction_->receive(0, "", DistributedTransaction::OK);
    manager_.transaction_->removePrepareResponses();
    manager_.callback_();
    EXPECT_TRUE(called_);
    called_ = false;
    ASSERT_EQ(1, response_.response_size());
    EXPECT_EQ(FragmentResponse::OK, response_.status());
    EXPECT_EQ(1, response_.response(0).partition_id());
    EXPECT_EQ("oneout", response_.response(0).output());

    // Not permitted to continue this transaction: it is finished!
    EXPECT_DEATH(coordinator_.Execute(NULL, &request_, &response_, this));

    // Commit the transaction: finishes the 2pc
    EXPECT_FALSE(manager_.commit_);
    coordinator_.Finish(NULL, &finish_request_, &finish_response_, this);
    EXPECT_TRUE(manager_.commit_);
    manager_.callback_();
    EXPECT_TRUE(called_);
}

TEST_F(ProtoCoordinatorTest, CommitUnfinishedMPtoSPDowngrade) {
    CoordinatorFragment::PartitionFragment* fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("work");

    coordinator_.Execute(NULL, &request_, &response_, this);
    ASSERT_EQ(1, manager_.transaction_->sent().size());
    EXPECT_EQ(0, manager_.transaction_->sent()[0].first);
    EXPECT_EQ("work", manager_.transaction_->sent()[0].second);
    EXPECT_FALSE(called_);

    // Receive responses: passed back to the client
    manager_.transaction_->receive(0, "zeroout", DistributedTransaction::OK);
    manager_.callback_();
    EXPECT_TRUE(called_);
    called_ = false;
    ASSERT_EQ(1, response_.response_size());
    EXPECT_EQ(FragmentResponse::OK, response_.status());
    EXPECT_EQ(0, response_.response(0).partition_id());
    EXPECT_EQ("zeroout", response_.response(0).output());    

    // Commit: this "downgrades" the transaction to a single partition transaction!
    coordinator_.Finish(NULL, &finish_request_, &finish_response_, this);
    EXPECT_TRUE(manager_.commit_);
    manager_.callback_();
    EXPECT_TRUE(called_);
}

TEST_F(ProtoCoordinatorTest, DonePartitions) {
    CoordinatorFragment::PartitionFragment* fragment = request_.add_fragment();
    fragment->set_partition_id(0);
    fragment->set_work("work");
    request_.add_done_partition(1);
    request_.add_done_partition(1);
    EXPECT_DEATH(coordinator_.Execute(NULL, &request_, &response_, this));

    request_.clear_done_partition();
    request_.add_done_partition(1);
    request_.set_last_fragment(true);
    EXPECT_DEATH(coordinator_.Execute(NULL, &request_, &response_, this));

    request_.clear_last_fragment();
    request_.clear_done_partition();
    request_.add_done_partition(2);
    request_.add_done_partition(0);
    request_.add_done_partition(3);
    coordinator_.Execute(NULL, &request_, &response_, this);

    ASSERT_EQ(1, manager_.transaction_->sent().size());
    EXPECT_TRUE(manager_.transaction_->isDone(0));
    EXPECT_FALSE(manager_.transaction_->isDone(1));
    EXPECT_TRUE(manager_.transaction_->isDone(2));
    EXPECT_TRUE(manager_.transaction_->isDone(3));

    // respond to all fragments and abort to free memory
    manager_.transaction_->receive(0, "", DistributedTransaction::OK);
    manager_.callback_();
    finish_request_.set_commit(false);
    coordinator_.Finish(NULL, &finish_request_, &finish_response_, this);
    manager_.callback_();
}

int main() {
    int result = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
