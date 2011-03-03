// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/stlutil.h"
#include "dtxn/dtxnserver.h"
#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "dtxn/scheduler.h"
#include "dtxn/transactionstate.h"
#include "io/eventloop.h"
#include "mockmessageconnection.h"
#include "net/messageserver.h"
#include "replication/mockfaulttolerantlog.h"
#include "stupidunit/stupidunit.h"


using namespace dtxn;
using std::string;
using std::vector;

class MockScheduler : public Scheduler {
public:
    MockScheduler() : commit_count_(0), abort_count_(0), execute_count_(0),
            last_executed_(NULL),
            next_status_(ExecutionEngine::INVALID), next_idle_(false), idle_(false),
            do_not_output_(false) {}

    virtual ~MockScheduler() {}

    virtual void fragmentArrived(FragmentState* fragment) {
        arrived_.push_back(fragment);
    }

    virtual void commit(TransactionState* transaction) {
        commit_count_ += 1;
    }

    virtual void abort(TransactionState* transaction) {        
        // Remove from the output queue: this is the scheduler's responsability
        // TODO: Test and fix this?
        //~ outputs_.eraseValue(transaction);

        abort_count_ += 1;
    }

    virtual void order(vector<FragmentState*>* queue) {
        STLExtend(queue, next_order_);
        next_order_.clear();
    }

    virtual FragmentState* executed() {
        if (outputs_.empty()) return NULL;
        return outputs_.dequeue();
    }

    virtual void execute(FragmentState* fragment) {
        execute_count_ += 1;
        last_executed_ = fragment;

        if (next_status_ != ExecutionEngine::INVALID) {
            FragmentResponse* response = fragment->mutable_response();
            response->result = next_result_;
            next_result_.clear();
            response->status = next_status_;
            next_status_ = ExecutionEngine::INVALID;

            if (!do_not_output_) {
                outputs_.push_back(fragment);
            }
        }
    }

    virtual bool idle() {
        idle_ = true;
        return next_idle_;
    }

    void nextOrder(int index) {
        next_order_.push_back(arrived_.at(index));
        arrived_.erase(index);
    }

    int commit_count_;
    int abort_count_;
    int execute_count_;

    FragmentState* last_executed_;

    string next_result_;
    ExecutionEngine::Status next_status_;

    CircularBuffer<FragmentState*> arrived_;
    CircularBuffer<FragmentState*> outputs_;
    vector<FragmentState*> next_order_;
    bool next_idle_;
    bool idle_;

    bool do_not_output_;
};


class BaseDtxnServerTest : public Test {
public:
    BaseDtxnServerTest() :
            server_(NULL),
            scheduler_(new MockScheduler()),
            client_(new MockMessageConnection()) {
        client_handle_ = msg_server_.addConnection(client_);
        fragment_.transaction = "hello world";
    }

    ~BaseDtxnServerTest() {
        delete server_;
    }

    DtxnServer* server_;
    MockScheduler* scheduler_;
    io::MockEventLoop event_loop_;
    MockMessageConnection* client_;
    net::ConnectionHandle* client_handle_;
    net::MessageServer msg_server_;
    Fragment fragment_;
    FragmentResponse response_;
    CommitDecision decision_;

    //~ TransactionState* txn_;
    //~ FragmentState* fragment_state_;
};

class DtxnServerTest : public BaseDtxnServerTest {
public:
    DtxnServerTest() {
        server_ = new DtxnServer(scheduler_, &event_loop_, &msg_server_, NULL);
    }
};

TEST_F(DtxnServerTest, NullCreate) {
    EXPECT_DEATH(DtxnServer(NULL, &event_loop_, &msg_server_, NULL));
    EXPECT_DEATH(DtxnServer(scheduler_, NULL, &msg_server_, NULL));
    EXPECT_DEATH(DtxnServer(scheduler_, &event_loop_, NULL, NULL));
}

TEST_F(DtxnServerTest, Idle) {
    // Nothing happening: calls the scheduler
    EXPECT_FALSE(scheduler_->idle_);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_TRUE(scheduler_->idle_);
    scheduler_->next_idle_ = true;
    EXPECT_TRUE(event_loop_.idle());
}

TEST_F(DtxnServerTest, UnregisterCallbacksInDestructor) {
    io::MockEventLoop loop;
    net::MessageServer msg_server_;
    {
        DtxnServer s(new MockScheduler(), &loop, &msg_server_, NULL);
        EXPECT_TRUE(loop.idle_callback_enabled_);
        EXPECT_TRUE(msg_server_.registered<Fragment>());
    }
    // Make sure the idle handler is unregistered in the destructor
    EXPECT_FALSE(loop.idle_callback_enabled_);
    EXPECT_FALSE(msg_server_.registered<Fragment>());
}

TEST_F(DtxnServerTest, SinglePartition) {
    fragment_.multiple_partitions = false;

    fragment_.id = 0;
    server_->fragmentReceived(client_handle_, fragment_);
    EXPECT_EQ(1, scheduler_->arrived_.size());
    EXPECT_FALSE(client_->hasMessage());

    // Start another transaction: skipping transaction ids is permitted
    EXPECT_DEATH(server_->fragmentReceived(client_handle_, fragment_));
    fragment_.id = 15;
    server_->fragmentReceived(client_handle_, fragment_);
    ASSERT_EQ(2, scheduler_->arrived_.size());
    EXPECT_FALSE(client_->hasMessage());

    // Respond to the latest client message
    scheduler_->next_result_ = "out";
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(1);

    // The idle handler invokes order and execute
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(15, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("out", response_.result);
    EXPECT_EQ(0, scheduler_->next_order_.size());
    EXPECT_EQ(1, scheduler_->execute_count_);

    // Respond to the next
    scheduler_->next_result_ = "out2";
    scheduler_->next_status_ = ExecutionEngine::ABORT_USER;
    scheduler_->nextOrder(0);

    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, response_.status);
    EXPECT_EQ("out2", response_.result);
    EXPECT_EQ(2, scheduler_->execute_count_);
}

TEST_F(DtxnServerTest, MultiplePhaseCommit) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_->fragmentReceived(client_handle_, fragment_);

    // cannot receive another part until we have responded
    fragment_.transaction = "part2";
    EXPECT_DEATH(server_->fragmentReceived(client_handle_, fragment_));

    scheduler_->next_result_ = "";
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("", response_.result);

    // receive part 2
    server_->fragmentReceived(client_handle_, fragment_);
    EXPECT_EQ("part2", scheduler_->arrived_.at(0)->request().transaction);

    // can't commit until we sent out results!
    decision_.commit = true;
    EXPECT_DEATH(server_->decisionReceived(client_handle_, decision_));

    scheduler_->next_result_ = "foo";
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    EXPECT_FALSE(event_loop_.idle());
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("foo", response_.result);

    server_->decisionReceived(client_handle_, decision_);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_EQ(1, scheduler_->commit_count_);
    EXPECT_EQ(false, client_->hasMessage());
}

// start a transaction as a general transaction, then it becomes single partition
TEST_F(DtxnServerTest, MultiplePhaseSinglePartition) {
    // starts as a general transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_->fragmentReceived(client_handle_, fragment_);

    scheduler_->next_result_ = "";
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    event_loop_.idle();
    client_->getMessage(&response_);

    // receive part 2 which makes it single partition. this should get logged then finished
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    fragment_.transaction = "bar";
    server_->fragmentReceived(client_handle_, fragment_);
    EXPECT_EQ("bar", scheduler_->arrived_.at(0)->request().transaction);

    scheduler_->next_result_ = "out";
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    event_loop_.idle();
    client_->getMessage(&response_);
}

TEST_F(DtxnServerTest, PollResponsesOnIdle) {
    fragment_.multiple_partitions = false;

    fragment_.id = 0;
    server_->fragmentReceived(client_handle_, fragment_);
    EXPECT_EQ(1, scheduler_->arrived_.size());
    EXPECT_FALSE(client_->hasMessage());
    event_loop_.idle();
    EXPECT_FALSE(client_->hasMessage());

    // Respond to the latest client message
    scheduler_->next_result_ = "out";
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    // Do not output when executing fragments
    scheduler_->do_not_output_ = true;

    // The idle handler invokes order and execute
    EXPECT_EQ(NULL, scheduler_->last_executed_);
    event_loop_.idle();
    EXPECT_NE(NULL, scheduler_->last_executed_);
    EXPECT_FALSE(client_->hasMessage());

    // Output the order, call the idle loop
    scheduler_->outputs_.push_back(scheduler_->last_executed_);
    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
}

#ifdef FOO
TEST_F(DtxnServerTest, OutOfOrderDestruct) {
    // This transaction is pending
    server_->fragmentReceived(client_handle_, fragment_);
    /*TransactionState* pending = */scheduler_->arrived_.dequeue();

    // This transaction completes
    fragment_.id = 2;
    server_->fragmentReceived(client_handle_, fragment_);
    TransactionState* completed = scheduler_->arrived_.dequeue();
    completed->setWorkUnitStatus(ExecutionEngine::ABORT_DEADLOCK);
    scheduler_->outputs_.push_back(completed);
    event_loop_.idle();
    EXPECT_TRUE(client_->hasMessage());

    // the destructor should skip the completed transaction
}

TEST_F(DtxnServerTest, DeadlockAbort) {
    //~ fragment_.multiple_partitions = false;
    //~ fragment_.id = 0;
    server_->fragmentReceived(client_handle_, fragment_);
    TransactionState* txn = scheduler_->arrived_.front();
    txn->setWorkUnitStatus(ExecutionEngine::ABORT_DEADLOCK);
    scheduler_->outputs_.push_back(txn);

    event_loop_.idle();
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::ABORT_DEADLOCK, response_.status);
    EXPECT_EQ("", response_.result);
    EXPECT_EQ(1, scheduler_->abort_count_);
}

TEST_F(DtxnServerTest, BadDecision) {
    // Decision for a transaction that has not started
    EXPECT_DEATH(server_->decisionReceived(client_handle_, decision_));
    server_->fragmentReceived(client_handle_, fragment_);
    // Decision for a queued single partition transaction
    EXPECT_DEATH(server_->decisionReceived(client_handle_, decision_));

    TransactionState* txn = scheduler_->arrived_.at(0);
    txn->setWorkUnitStatus(ExecutionEngine::OK);
    scheduler_->outputs_.push_back(txn);
    event_loop_.idle();
    EXPECT_TRUE(client_->hasMessage());

    // Decision for a finished transaction
    EXPECT_DEATH(server_->decisionReceived(client_handle_, decision_));
}

TEST_F(DtxnServerTest, BadDoneMessage) {
    // Begin a multiple partition transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_->fragmentReceived(client_handle_, fragment_);
    TransactionState* txn = scheduler_->arrived_.dequeue();
    txn->setWorkUnitStatus(ExecutionEngine::OK);
    scheduler_->outputs_.push_back(txn);
    event_loop_.idle();
    EXPECT_TRUE(client_->hasMessage());

    // Execute a fragment with the done bit set
    fragment_.last_fragment = true;
    server_->fragmentReceived(client_handle_, fragment_);
    txn->setWorkUnitStatus(ExecutionEngine::OK);
    scheduler_->outputs_.push_back(txn);
    event_loop_.idle();

    // Add another piece of work for this transaction: NOT PERMITTED
    EXPECT_DEATH(server_->fragmentReceived(client_handle_, fragment_));
}
#endif

class DtxnServerLogTest : public BaseDtxnServerTest {
public:
    DtxnServerLogTest() {
        server_ = new DtxnServer(scheduler_, &event_loop_, &msg_server_, &log_);
    }

    template <typename T>
    void nextBackupLog(const T& message, int index) {
        string serialized;
        message.appendToString(&serialized);
        log_.nextLogEntryCallback(index, serialized, NULL);
    }

    replication::MockFaultTolerantLog log_;
};

TEST_F(DtxnServerLogTest, PrimarySinglePartition) {
    log_.is_primary(true);

    // Receive a single partition transaction and give it to the scheduler.
    server_->fragmentReceived(client_handle_, fragment_);
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_EQ(1, log_.flush_count_);

    // Client can't get message yet: must be replicated
    EXPECT_FALSE(client_->hasMessage());
    ASSERT_EQ(1, log_.submitted_.size());

    // Queue a second transaction
    fragment_.id = 2;
    server_->fragmentReceived(client_handle_, fragment_);
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    // Replication completes for first transaction; client gets results after idle
    log_.callbackSubmitted(0);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_EQ(2, log_.submitted_.size());
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_TRUE(client_->hasMessage());
    client_->message_.clear();

    // A transaction begins and aborts: replicated and not responded
    fragment_.id = 3;
    server_->fragmentReceived(client_handle_, fragment_);
    scheduler_->next_status_ = ExecutionEngine::ABORT_USER;
    scheduler_->nextOrder(0);
    event_loop_.idle();
    EXPECT_EQ(3, log_.submitted_.size());
    EXPECT_FALSE(client_->hasMessage());
}

TEST_F(DtxnServerLogTest, BackupSinglePartition) {
    log_.is_primary(false);

    fragment_.transaction = "foo";
    nextBackupLog(fragment_, 0);
    EXPECT_EQ(0, scheduler_->execute_count_);
    event_loop_.idle();
    EXPECT_EQ(1, scheduler_->execute_count_);
}

TEST_F(DtxnServerLogTest, PrimaryMultiPartition) {
    log_.is_primary(true);

    // Receive a multi-partition transaction (not finished!) and give it to the scheduler.
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_->fragmentReceived(client_handle_, fragment_);
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    event_loop_.idle();

    // TODO: Client should get the message here?
    EXPECT_FALSE(client_->hasMessage());
    ASSERT_EQ(1, log_.submitted_.size());
    fragment_.parseFromString(log_.submitted_[0]);
    EXPECT_EQ("hello world", fragment_.transaction);
    log_.callbackSubmitted(0);
    EXPECT_TRUE(client_->hasMessage());
    client_->message_.clear();

    // Add the final piece to the transaction
    fragment_.transaction = "foo";
    fragment_.last_fragment = true;
    server_->fragmentReceived(client_handle_, fragment_);
    scheduler_->next_status_ = ExecutionEngine::OK;
    scheduler_->nextOrder(0);
    event_loop_.idle();

    // No answer to the client; NOW we replicate
    EXPECT_FALSE(client_->hasMessage());
    ASSERT_EQ(2, log_.submitted_.size());
    fragment_.parseFromString(log_.submitted_[1]);
    EXPECT_EQ("foo", fragment_.transaction);
    log_.callbackSubmitted(1);
    EXPECT_TRUE(client_->hasMessage());
    client_->message_.clear();

    // decision is received and replicated
    decision_.id = fragment_.id;
    decision_.commit = true;
    server_->decisionReceived(client_handle_, decision_);
    ASSERT_EQ(3, log_.submitted_.size());
    decision_.parseFromString(log_.submitted_[2]);
    EXPECT_TRUE(decision_.commit);

    // decision finalized, nothing visible happens
    log_.callbackSubmitted(2);
    EXPECT_FALSE(client_->hasMessage());
}

TEST_F(DtxnServerLogTest, BackupMultiPartition) {
    log_.is_primary(false);

    // start the multi-partition transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    scheduler_->next_status_ = ExecutionEngine::OK;
    nextBackupLog(fragment_, 0);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_EQ(1, scheduler_->execute_count_);

    // add another chunk
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    scheduler_->next_status_ = ExecutionEngine::OK;
    nextBackupLog(fragment_, 1);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_EQ(2, scheduler_->execute_count_);

    decision_.id = fragment_.id;
    decision_.commit = true;
    nextBackupLog(decision_, 2);
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_EQ(1, scheduler_->commit_count_);
}

TEST_F(DtxnServerLogTest, BackupMultiPartitionQueuedDecision) {
    log_.is_primary(false);

    // start the multi-partition transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    scheduler_->next_status_ = ExecutionEngine::OK;
    nextBackupLog(fragment_, 0);

    // add another chunk
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    scheduler_->next_status_ = ExecutionEngine::OK;
    nextBackupLog(fragment_, 1);

    // add the decision
    decision_.id = fragment_.id;
    decision_.commit = true;
    nextBackupLog(decision_, 2);

    // Execute everything in one go
    EXPECT_FALSE(event_loop_.idle());
    EXPECT_EQ(2, scheduler_->execute_count_);
    EXPECT_EQ(1, scheduler_->commit_count_);
}

#ifdef foo
TEST_F(DtxnServerLogTest, PrimaryMultiPartitionTimeoutBeforeReplication) {
    log_.is_primary(true);

    // Receive a multi-partition transaction that finishes
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    server_->fragmentReceived(client_handle_, fragment_);
    TransactionState* txn = scheduler_->arrived_.dequeue();
    txn->setWorkUnitStatus(ExecutionEngine::OK);
    scheduler_->outputs_.push_back(txn);
    event_loop_.idle();

    // Replication occurs
    EXPECT_EQ(1, log_.submitted_.size());

    // Now an abort is received
    decision_.commit = false;
    server_->decisionReceived(client_handle_, decision_);

    // The abort is replicated, even without the acks for the replication
    EXPECT_EQ(2, log_.submitted_.size());
    EXPECT_FALSE(client_->hasMessage());

    // Ack the original replication: no output (already aborted)
    log_.callbackSubmitted(0);
    EXPECT_FALSE(client_->hasMessage());

    // Ack the abort: still okay
    log_.callbackSubmitted(1);
    EXPECT_FALSE(client_->hasMessage());
}

TEST_F(DtxnServerLogTest, PrimaryMultiPartitionTimeoutWhileQueued) {
    log_.is_primary(true);

    // Receive a multi-partition transaction that finishes
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    server_->fragmentReceived(client_handle_, fragment_);
    TransactionState* txn = scheduler_->arrived_.dequeue();
    txn->setWorkUnitStatus(ExecutionEngine::OK);
    scheduler_->outputs_.push_back(txn);

    // Receive an abort *before* we try to replicate the entry
    decision_.commit = false;
    server_->decisionReceived(client_handle_, decision_);

    // Removed from the output queue: this is the scheduler's responsability
    EXPECT_EQ(0, scheduler_->outputs_.size());

    // No replication occurs: we just forget about the transaction
    event_loop_.idle();
    event_loop_.idle();
    EXPECT_EQ(0, log_.submitted_.size());
    EXPECT_FALSE(client_->hasMessage());
}

TEST_F(DtxnServerLogTest, PrimaryMultiPartitionAbortBeforePrepare) {
    log_.is_primary(true);

    // Unfinished multi-partition transaction
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    server_->fragmentReceived(client_handle_, fragment_);
    TransactionState* txn = scheduler_->arrived_.dequeue();
    txn->setWorkUnitStatus(ExecutionEngine::OK);
    scheduler_->outputs_.push_back(txn);
    event_loop_.idle();

    // No replication
    EXPECT_EQ(0, log_.submitted_.size());
    EXPECT_TRUE(client_->hasMessage());
    client_->message_.clear();

    // Abort decision is received: no replication or responses
    decision_.commit = false;
    server_->decisionReceived(client_handle_, decision_);
    EXPECT_EQ(0, log_.submitted_.size());
    EXPECT_FALSE(client_->hasMessage());
}
#endif

int main() {
    return TestSuite::globalInstance()->runAll();
}
