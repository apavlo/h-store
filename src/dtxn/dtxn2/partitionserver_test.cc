// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <string>
#include <vector>

#include "base/circularbuffer.h"
//~ #include "base/stlutil.h"
#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
//~ #include "dtxn/mockexecutionengine.h"
#include "dtxn2/partitionserver.h"
#include "dtxn2/scheduler.h"
#include "dtxn2/transactionstate.h"
#include "io/eventloop.h"
#include "mockmessageconnection.h"
#include "net/messageserver.h"
#include "replication/nulllog.h"
#include "stupidunit/stupidunit.h"

using dtxn::CommitDecision;
using dtxn::ExecutionEngine;
using dtxn::Fragment;
using dtxn::FragmentResponse;
using std::string;
using std::vector;
using namespace dtxn2;


class MockScheduler : public Scheduler {
public:
    MockScheduler() :
            commit_count_(0),
            abort_count_(0),
            last_decided_id_(0),
            next_work_(false),
            work_(false) {}

    virtual ~MockScheduler() {}

    virtual void fragmentArrived(TransactionState* transaction) {
        arrived_.push_back(transaction);
    }

    virtual void decide(TransactionState* transaction, bool commit) {
        last_decided_id_ = transaction->last_fragment().request().id;
        if (commit) {
            commit_count_ += 1;
        } else {
            abort_count_ += 1;
        }
    }

    virtual bool doWork(SchedulerOutput* output) {
        while (!next_replicate_.empty()) {
            output->replicate(next_replicate_.dequeue());
        }

        while (!outputs_.empty()) {
            output->executed(outputs_.dequeue());
        }

        work_ = true;
        return next_work_;
    }

    void nextOutput(int index, ExecutionEngine::Status status, const string& output) {
        TransactionState* txn = arrived_.at(index);
        arrived_.erase(index);
        txn->mutable_last()->mutable_response()->status = status;
        txn->mutable_last()->mutable_response()->result = output;
        outputs_.push_back(txn->mutable_last());
    }

    void nextReplicateAndOutput(int index, ExecutionEngine::Status status, const string& output) {
        TransactionState* txn = arrived_.at(index);
        next_replicate_.push_back(txn);
        nextOutput(index, status, output);
    }

    int commit_count_;
    int abort_count_;

    int last_decided_id_;

    //~ string next_result_;
    //~ ExecutionEngine::Status next_status_;

    CircularBuffer<TransactionState*> arrived_;
    CircularBuffer<FragmentState*> outputs_;
    CircularBuffer<TransactionState*> next_replicate_;
    bool next_work_;
    bool work_;
};

//~ class MockTransactionState : public TransactionState {
//~ public:
    //~ void addFragment(const Fragment& fragment) {
        //~ TransactionState::addFragment(fragment);
    //~ }
//~ };

class PartitionServerTest : public Test {
public:
    PartitionServerTest() :
            server_(&scheduler_, &event_loop_, &msg_server_, &log_),
            client_(new MockMessageConnection()) {
        client_handle_ = msg_server_.addConnection(client_);
        fragment_.multiple_partitions = false;
        fragment_.last_fragment = true;
        fragment_.transaction = "foo";
        fragment_.client_id = 0;
        fragment_.id = 0;
    }

    MockScheduler scheduler_;
    io::MockEventLoop event_loop_;
    net::MessageServer msg_server_;
    replication::NullLog log_;

    PartitionServer server_;

    MockMessageConnection* client_;
    net::ConnectionHandle* client_handle_;
    
    Fragment fragment_;
    FragmentResponse response_;
    CommitDecision decision_;
};

TEST_F(PartitionServerTest, Idle) {
    // Nothing happening: calls the scheduler
    EXPECT_FALSE(scheduler_.work_);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_TRUE(scheduler_.work_);
    scheduler_.next_work_ = true;
    EXPECT_TRUE(event_loop_.idle());
}

TEST_F(PartitionServerTest, SinglePartition) {
    server_.fragmentReceived(client_handle_, fragment_);
    EXPECT_EQ(1, scheduler_.arrived_.size());
    EXPECT_FALSE(client_->hasMessage());

    // Commit/abort not permitted
    decision_.id = fragment_.id;
    decision_.commit = true;
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));
    decision_.commit = false;
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));

    // Start another transaction: skipping transaction ids is permitted
    EXPECT_DEATH(server_.fragmentReceived(client_handle_, fragment_));
    fragment_.id = 15;
    server_.fragmentReceived(client_handle_, fragment_);
    ASSERT_EQ(2, scheduler_.arrived_.size());
    EXPECT_FALSE(client_->hasMessage());

    // Respond to the latest client message
    scheduler_.nextReplicateAndOutput(1, ExecutionEngine::OK, "out");

    // The idle handler invokes order and execute
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(15, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("out", response_.result);
    EXPECT_EQ(0, scheduler_.outputs_.size());
    EXPECT_EQ(0, scheduler_.next_replicate_.size());

    // Respond to the next
    scheduler_.nextReplicateAndOutput(0, ExecutionEngine::ABORT_USER, "out2");

    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, response_.status);
    EXPECT_EQ("out2", response_.result);
}

TEST_F(PartitionServerTest, MultiPartitionOneRoundCommit) {
    fragment_.multiple_partitions = true;
    server_.fragmentReceived(client_handle_, fragment_);

    // Commit not permitted
    decision_.id = fragment_.id;
    decision_.commit = true;
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));

    // Respond
    scheduler_.nextReplicateAndOutput(0, ExecutionEngine::OK, "out");
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("out", response_.result);

    // Commit
    server_.decisionReceived(client_handle_, decision_);
    EXPECT_EQ(1, scheduler_.commit_count_);
    EXPECT_EQ(0, scheduler_.last_decided_id_);
}

TEST_F(PartitionServerTest, MultiPartitionOneRoundAbort) {
    fragment_.multiple_partitions = true;
    server_.fragmentReceived(client_handle_, fragment_);

    // Respond with abort
    scheduler_.nextReplicateAndOutput(0, ExecutionEngine::ABORT_USER, "");
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, response_.status);
    EXPECT_EQ("", response_.result);

    decision_.id = fragment_.id;
    decision_.commit = true;
    // commit not permitted
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));
    // abort okay
    decision_.commit = false;
    server_.decisionReceived(client_handle_, decision_);
    EXPECT_EQ(1, scheduler_.abort_count_);
    EXPECT_EQ(0, scheduler_.last_decided_id_);
}

TEST_F(PartitionServerTest, MultiPartitionMultiRoundEarlyAbort) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_.fragmentReceived(client_handle_, fragment_);

    // Respond with ok
    scheduler_.nextOutput(0, ExecutionEngine::OK, "");
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("", response_.result);

    // abort
    decision_.id = fragment_.id;
    decision_.commit = true;
    // commit not permitted
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));
    // abort okay
    decision_.commit = false;
    server_.decisionReceived(client_handle_, decision_);
    EXPECT_EQ(1, scheduler_.abort_count_);
    EXPECT_EQ(0, scheduler_.last_decided_id_);
}

TEST_F(PartitionServerTest, SinglePartitionMultiRound) {
    // A single partition multi-round transaction *starts* as a multi-partition transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_.fragmentReceived(client_handle_, fragment_);

    // Respond with ok
    scheduler_.nextOutput(0, ExecutionEngine::OK, "");
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);

    // Receive the next fragment which downgrades to single partition
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    server_.fragmentReceived(client_handle_, fragment_);

    // Output: this needs to wait for replication
    TransactionState* txn = scheduler_.arrived_.at(0);
    scheduler_.nextOutput(0, ExecutionEngine::OK, "bar");
    event_loop_.idle();
    EXPECT_FALSE(client_->hasMessage());
    scheduler_.next_replicate_.push_back(txn);
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ("bar", response_.result);
    EXPECT_EQ(0, scheduler_.commit_count_);
    EXPECT_EQ(0, scheduler_.abort_count_);

    // Commit or abort not permitted: this was committed as a single partition
    decision_.id = fragment_.id;
    decision_.commit = true;
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));
    decision_.commit = false;
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));
}

TEST_F(PartitionServerTest, SinglePartitionAbortNoReplicate) {
    // A single partition multi-round transaction *starts* as a multi-partition transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_.fragmentReceived(client_handle_, fragment_);

    // Respond with ok
    scheduler_.nextOutput(0, ExecutionEngine::OK, "");
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);

    // Receive the next fragment which downgrades to single partition
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    server_.fragmentReceived(client_handle_, fragment_);

    // Output abort: respond immediately; do not replicate
    scheduler_.nextOutput(0, ExecutionEngine::ABORT_USER, "");
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ("", response_.result);
    EXPECT_EQ(0, scheduler_.commit_count_);
    EXPECT_EQ(0, scheduler_.abort_count_);

    // Commit, abort and replicate not permitted: this was aborted as a single partition
    decision_.id = fragment_.id;
    decision_.commit = true;
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));
    decision_.commit = false;
    EXPECT_DEATH(server_.decisionReceived(client_handle_, decision_));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
