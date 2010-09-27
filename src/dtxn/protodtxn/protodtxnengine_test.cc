// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/assert.h"
#include "dtxn/executionengine.h"
#include "dtxn/transactionstate.h"
#include "io/eventloop.h"
#include "protodtxn/dtxn.pb.h"
#include "protodtxn/protodtxnengine.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

using namespace protodtxn;

class MockEngine : public ExecutionEngine {
public:
    MockEngine() : last_id_(-1), undo_count_(0), finish_id_(-1), finish_commit_(false) {}

    virtual void Execute(::google::protobuf::RpcController* controller,
            const Fragment* request,
            FragmentResponse* response,
            ::google::protobuf::Closure* done) {
        if (request->undoable() && request->transaction_id() > last_id_) {
            undo_count_ += 1;
        }

        assert(request->transaction_id() >= last_id_);
        last_id_ = request->transaction_id();

        response->set_output(request->work() + request->work());
        response->set_status(FragmentResponse::OK);

        done->Run();
    }

    virtual void Finish(::google::protobuf::RpcController* controller,
            const FinishRequest* request,
            FinishResponse* response,
            ::google::protobuf::Closure* done) {
        assert(request->transaction_id() <= last_id_);
        assert(finish_id_ < request->transaction_id());
        assert(undo_count_ > 0);
        finish_id_ = request->transaction_id();
        finish_commit_ = request->commit();
        undo_count_ -= 1;

        done->Run();
    }

    int32_t last_id_;
    int undo_count_;

    int32_t finish_id_;
    bool finish_commit_;
};


class ProtoEngineTest : public Test {
public:
    ProtoEngineTest () : engine_(&event_loop_, &mock_), undo_(NULL) {
    }

    ~ProtoEngineTest() {
    }

    io::MockEventLoop event_loop_;
    MockEngine mock_;
    ProtoDtxnEngine engine_;
    dtxn::ExecutionEngine::Status status_;
    string output_;
    void* undo_;
};


TEST_F(ProtoEngineTest, SinglePartition) {
    status_ = engine_.tryExecute("work", &output_, NULL, NULL);
    EXPECT_EQ(dtxn::ExecutionEngine::OK, status_);
    EXPECT_EQ("workwork", output_);
}

TEST_F(ProtoEngineTest, MultiPartitionSingleFragment) {
    status_ = engine_.tryExecute("work", &output_, &undo_, NULL);
    EXPECT_TRUE(undo_ != NULL);
    EXPECT_EQ(undo_, (void*) mock_.last_id_);
    EXPECT_EQ(1, mock_.undo_count_);

    engine_.freeUndo(undo_);
    EXPECT_EQ(undo_, (void*) mock_.finish_id_);
    EXPECT_TRUE(mock_.finish_commit_);
    EXPECT_EQ(0, mock_.undo_count_);

    // Abort the next transaction
    void* next_undo = NULL;
    status_ = engine_.tryExecute("work", &output_, &next_undo, NULL);
    EXPECT_NE(undo_, NULL);
    EXPECT_NE(undo_, next_undo);
    EXPECT_EQ(next_undo, (void*) mock_.last_id_);
    EXPECT_EQ(1, mock_.undo_count_);

    engine_.applyUndo(next_undo);
    EXPECT_EQ(next_undo, (void*) mock_.finish_id_);
    EXPECT_FALSE(mock_.finish_commit_);
    EXPECT_EQ(0, mock_.undo_count_);
}

TEST_F(ProtoEngineTest, MultiPartitionMultiFragment) {
    status_ = engine_.tryExecute("frag1", &output_, &undo_, NULL);
    EXPECT_TRUE(undo_ != NULL);
    EXPECT_EQ(undo_, (void*) mock_.last_id_);
    EXPECT_EQ(1, mock_.undo_count_);
    void* last_undo = undo_;

    status_ = engine_.tryExecute("frag2", &output_, &undo_, NULL);
    EXPECT_EQ(last_undo, undo_);
    EXPECT_EQ(undo_, (void*) mock_.last_id_);
    EXPECT_EQ(1, mock_.undo_count_);

    engine_.applyUndo(undo_);
    EXPECT_EQ(undo_, (void*) mock_.finish_id_);
    EXPECT_FALSE(mock_.finish_commit_);
    EXPECT_EQ(0, mock_.undo_count_);
}

int main() {
    int result = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
