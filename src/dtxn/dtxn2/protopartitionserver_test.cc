// Copyright 2011 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <string>
#include <vector>

#include "base/circularbuffer.h"
////~ #include "base/stlutil.h"
#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "dtxn2/partitionserver.h"
#include "dtxn2/protopartitionserver.h"
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

using protodtxn::DtxnPartitionFragment;
using google::protobuf::Closure;


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

class ProtoPartitionServerTest : public Test {
public:
    ProtoPartitionServerTest() :
            dtxn_server_(&scheduler_, &event_loop_, &msg_server_, &log_),
            proto_server_(&dtxn_server_, &msg_server_),
            client_(new MockMessageConnection()),
            called_(false),
            callback_(NULL) {
        client_handle_ = msg_server_.addConnection(client_);
        fragment_.multiple_partitions = false;
        fragment_.last_fragment = true;
        fragment_.transaction = "foo";
        fragment_.client_id = 0;
        fragment_.id = 0;
        callback_ = google::protobuf::NewPermanentCallback(
                this, &ProtoPartitionServerTest::callback);
    }

    ~ProtoPartitionServerTest() {
        delete callback_;
    }

    void callback() {
        called_ = true;
    }

    MockScheduler scheduler_;
    io::MockEventLoop event_loop_;
    net::MessageServer msg_server_;
    replication::NullLog log_;

    PartitionServer dtxn_server_;
    ProtoPartitionServer proto_server_;

    MockMessageConnection* client_;
    net::ConnectionHandle* client_handle_;

    Fragment fragment_;
    FragmentResponse response_;
    CommitDecision decision_;

    bool called_;
    Closure* callback_;
};

TEST_F(ProtoPartitionServerTest, Simple) {
    DtxnPartitionFragment proto_fragment;
    proto_fragment.set_transaction_id(42);
    proto_fragment.set_commit(DtxnPartitionFragment::PREPARE);
    proto_fragment.set_work("work");
    protodtxn::FragmentResponse proto_response;

    proto_server_.Execute(NULL, &proto_fragment, &proto_response, callback_);
    EXPECT_FALSE(called_);
    ASSERT_EQ(1, scheduler_.arrived_.size());
    EXPECT_TRUE(scheduler_.arrived_.front()->last_fragment().request().multiple_partitions);
    EXPECT_TRUE(scheduler_.arrived_.front()->last_fragment().request().last_fragment);
    EXPECT_EQ("work", scheduler_.arrived_.front()->last_fragment().request().transaction);

    // give this a response
    scheduler_.nextReplicateAndOutput(0, ExecutionEngine::OK, "out");
    event_loop_.idle();

    EXPECT_TRUE(called_);
    EXPECT_EQ(protodtxn::FragmentResponse::OK, proto_response.status());
    EXPECT_EQ("out", proto_response.output());
    called_ = false;

    // abort
    protodtxn::FinishRequest finish;
    finish.set_transaction_id(42);
    finish.set_commit(false);
    protodtxn::FinishResponse finish_response;
    proto_server_.Finish(NULL, &finish, &finish_response, callback_);
    EXPECT_TRUE(called_);
}

int main() {
    int error = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return error;
}
