// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <netinet/in.h>

#include "base/stlutil.h"
#include "dtxn/dtxndistributor.h"
#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "dtxn/ordered/ordereddistributor.h"
#include "io/eventloop.h"
#include "mockmessageconnection.h"
#include "net/messageserver.h"
#include "stupidunit/stupidunit.h"

using std::make_pair;
using std::string;
using std::vector;

using namespace dtxn;

class MockCoordinator : public Coordinator {
public:
    MockCoordinator() : active_count_(0) {}

    virtual void begin(CoordinatedTransaction* transaction) {
        active_count_ += 1;
        transaction->state(new int(42));
        doFragment(transaction);
        sendRound(transaction);
    }

    virtual void nextRound(CoordinatedTransaction* transaction) {
        assert(active_count_ > 0);
        assert(*(int*)transaction->state() == 42);
        doFragment(transaction);
        sendRound(transaction);
    }

    virtual void doFragment(CoordinatedTransaction* transaction) = 0;

    virtual void done(CoordinatedTransaction* transaction) {
        assert(active_count_ > 0);
        assert(*(int*)transaction->state() == 42);
        delete (int*) transaction->state();
        transaction->state(NULL);
        active_count_ -= 1;
    }

    int active_count_;
};

class NullCoordinator : public MockCoordinator {
public:
    virtual void doFragment(CoordinatedTransaction* transaction) {
        assert(false);
    }
};

class OrderedDistributorTest : public Test {
public:
    OrderedDistributorTest() :
            client_(new MockMessageConnection), coordinator_(NULL), distributor_(NULL) {
        client_handle_ = msg_server_.addConnection(client_);
        replicas_.push_back(new MockMessageConnection());
        replicas_.push_back(new MockMessageConnection());
        replica_handles_.push_back(msg_server_.addConnection(replicas_[0]));
        replica_handles_.push_back(msg_server_.addConnection(replicas_[1]));
        transaction_.transaction = "hello world";
        response_.status = ExecutionEngine::OK;
    }

    ~OrderedDistributorTest() {
        delete coordinator_;
        delete distributor_;
    }

    // Create the sequencer with a particular distributor.
    void createSequencer(MockCoordinator* coordinator) {
        coordinator_ = coordinator;
        distributor_ = new OrderedDistributor(replica_handles_, coordinator_, &event_loop_, &msg_server_);
        //~ distributor_->connectionArrived(client_);
    }

    MockMessageConnection* client_;
    net::ConnectionHandle* client_handle_;
    vector<MockMessageConnection*> replicas_;
    vector<net::ConnectionHandle*> replica_handles_;
    MockCoordinator* coordinator_;
    io::MockEventLoop event_loop_;
    net::MessageServer msg_server_;
    OrderedDistributor* distributor_;

    Fragment transaction_;
    FragmentResponse response_;
};

TEST_F(OrderedDistributorTest, BadCreate) {
    NullCoordinator distributor;

    vector<net::ConnectionHandle*> replica_handles;
    EXPECT_DEATH(OrderedDistributor(replica_handles, &distributor, &event_loop_, &msg_server_));

    EXPECT_DEATH(OrderedDistributor(replica_handles_, NULL, &event_loop_, &msg_server_));
    EXPECT_DEATH(OrderedDistributor(replica_handles_, &distributor, NULL, &msg_server_));
    EXPECT_DEATH(OrderedDistributor(replica_handles_, &distributor, NULL, &msg_server_));
    EXPECT_DEATH(OrderedDistributor(replica_handles_, &distributor, &event_loop_, NULL));
};

TEST_F(OrderedDistributorTest, SetCallbackTargets) {
    createSequencer(new NullCoordinator());
    EXPECT_EQ(true, replicas_[0]->target() != NULL);
    EXPECT_EQ(true, replicas_[1]->target() != NULL);
};

TEST_F(OrderedDistributorTest, BadClientMessage) {
    // Client sends bad message: distributor says to abort it.
    class BadClientDistributor : public MockCoordinator {
    public:
        virtual void doFragment(CoordinatedTransaction* transaction) {
            transaction->abort("bad client message");
        }
    };
    createSequencer(new BadClientDistributor());

    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(0, coordinator_->active_count_);
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, response_.status);
    EXPECT_EQ("bad client message", response_.result);
}

TEST_F(OrderedDistributorTest, EmptyMessage) {
    // distributor returns an empty message
    class EmptyDistributor : public MockCoordinator {
    public:
        virtual void doFragment(CoordinatedTransaction* transaction) {
            transaction->commit("");
        }
    };
    createSequencer(new EmptyDistributor());
    distributor_->requestReceived(client_handle_, transaction_);
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("", response_.result);
}

class SingleDistributor : public MockCoordinator {
public:
    virtual void doFragment(CoordinatedTransaction* transaction) {
        assert(transaction->received().size() == 1);
        if (transaction->received()[0].first == CoordinatedTransaction::CLIENT_PARTITION) {
            // original message from client: send to partition
            transaction->send(0, transaction->received()[0].second);
            transaction->setAllDone();
        } else {
            // response from partition: pass it through
            assert(transaction->received()[0].first == 0);
            transaction->commit(transaction->received()[0].second);
        }
    }
};

TEST_F(OrderedDistributorTest, SinglePartitionCommit) {
    createSequencer(new SingleDistributor());

    // A client sends a single partition request to the sequencer. It should get passed through.
    transaction_.transaction = "single";
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(NULL, event_loop_.last_timeout_);  // no timeouts for SP transactions

    EXPECT_EQ(1, coordinator_->active_count_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(0, transaction_.id);
    EXPECT_EQ(false, transaction_.multiple_partitions);
    EXPECT_EQ(true, transaction_.last_fragment);
    EXPECT_EQ("single", transaction_.transaction);
    EXPECT_EQ(false, replicas_[1]->hasMessage());

    // Commit the single partition transaction
    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    response_.result = "foo";
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_EQ(0, coordinator_->active_count_);

    // Response should get passed through to the client.
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("foo", response_.result);
}

TEST_F(OrderedDistributorTest, TransactionIds) {
    createSequencer(new SingleDistributor());

    // Bad txn ids
    transaction_.id = -1;
    EXPECT_DEATH(distributor_->requestReceived(client_handle_, transaction_));

    transaction_.id = 0;
    distributor_->requestReceived(client_handle_, transaction_);

    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(0, transaction_.id);

    // this is ignored: we assume it is an old response from an aborted transaction
    response_.id = -1;  // ignored
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(client_->hasMessage());
    response_.id = -2;
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(client_->hasMessage());
    response_.id = 1;
    EXPECT_DEATH(distributor_->responseReceived(replica_handles_[0], response_));
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[0], response_);
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);

    MockMessageConnection* other = new MockMessageConnection();
    net::ConnectionHandle* other_handle = msg_server_.addConnection(other);

    // other sends txn id = 0; distributor sends txn id = 1
    distributor_->requestReceived(other_handle, transaction_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(1, transaction_.id);

    // original client sends another transaction which finishes first
    transaction_.id = 1;
    distributor_->requestReceived(client_handle_, transaction_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(2, transaction_.id);
    response_.id = 2;
    distributor_->responseReceived(replica_handles_[0], response_);
    client_->getMessage(&response_);
    EXPECT_EQ(1, response_.id);

    // other client's transaction finishes
    response_.id = 0;  // ignored
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(other->hasMessage());
    response_.id = -1;
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(other->hasMessage());
    response_.id = 2;
    EXPECT_DEATH(distributor_->responseReceived(replica_handles_[0], response_));
    response_.id = 1;
    distributor_->responseReceived(replica_handles_[0], response_);
    other->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
}

TEST_F(OrderedDistributorTest, SinglePartitionAbort) {
    createSequencer(new SingleDistributor());

    // A client sends a single partition request to the sequencer. It should get passed through.
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(true, replicas_[0]->hasMessage());
    EXPECT_EQ(false, replicas_[1]->hasMessage());

    // Abort the single partition transaction
    response_.status = ExecutionEngine::ABORT_USER;
    response_.result = "foo";
    distributor_->responseReceived(replica_handles_[0], response_);

    // Response should get passed through to the client.
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, response_.status);
    EXPECT_EQ("foo", response_.result);
}

TEST_F(OrderedDistributorTest, SinglePartitionQueuing) {
    createSequencer(new SingleDistributor());

    // A client sends a single partition request to the sequencer. It should get passed through.
    transaction_.transaction = "single";
    distributor_->requestReceived(client_handle_, transaction_);

    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(false, transaction_.multiple_partitions);
    EXPECT_EQ("single", transaction_.transaction);

    // A second single partition request also gets passed through
    transaction_.id = 1;
    transaction_.transaction = "second";
    distributor_->requestReceived(client_handle_, transaction_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(false, transaction_.multiple_partitions);
    EXPECT_EQ("second", transaction_.transaction);
    EXPECT_EQ(2, coordinator_->active_count_);

    // Commit out of order: second transaction
    response_.id = 1;
    response_.status = ExecutionEngine::OK;
    response_.result = "second";
    distributor_->responseReceived(replica_handles_[0], response_);

    // Response should get passed through to the client.
    client_->getMessage(&response_);
    EXPECT_EQ("second", response_.result);

    // Queue another transaction to get passed through
    transaction_.id = 2;
    distributor_->requestReceived(client_handle_, transaction_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // Commit the first transaction
    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    response_.result = "first";
    distributor_->responseReceived(replica_handles_[0], response_); 
    EXPECT_EQ(1, coordinator_->active_count_);

    // Response should get passed through to the client.
    client_->getMessage(&response_);
    EXPECT_EQ("first", response_.result);

    // Commit the final transcation
    response_.id = 2;
    distributor_->responseReceived(replica_handles_[0], response_); 
    EXPECT_EQ(0, coordinator_->active_count_);
    client_->getMessage(&response_);    
}

TEST_F(OrderedDistributorTest, SinglePartitionClientClose) {
    createSequencer(new SingleDistributor());

    // A client sends a single partition request to the sequencer. It should get passed through.
    distributor_->requestReceived(client_handle_, transaction_);

    // second request which completes
    transaction_.id = 1;
    distributor_->requestReceived(client_handle_, transaction_);
    response_.id = 1;
    distributor_->responseReceived(replica_handles_[0], response_);

    // The client connection gets closed, while the transaction is pending
    msg_server_.connectionClosed(client_);
    client_ = NULL;

    // the single partition transaction completes
    response_.id = 0;
    response_.status = ExecutionEngine::OK;
    response_.result = "foo";
    distributor_->responseReceived(replica_handles_[0], response_);
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

TEST_F(OrderedDistributorTest, OneShotCommit) {
    createSequencer(new OneShotCoordinator());
    distributor_->requestReceived(client_handle_, transaction_);

    EXPECT_EQ(1, coordinator_->active_count_);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(true, transaction_.multiple_partitions);
    EXPECT_EQ(true, transaction_.last_fragment);
    EXPECT_EQ("one", transaction_.transaction);
    replicas_[1]->getMessage(&transaction_);
    EXPECT_EQ(true, transaction_.multiple_partitions);
    EXPECT_EQ(true, transaction_.last_fragment);
    EXPECT_EQ("two", transaction_.transaction);

    response_.status = ExecutionEngine::OK;
    distributor_->responseReceived(replica_handles_[1], response_);
    EXPECT_EQ(1, coordinator_->active_count_);
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_EQ(0, coordinator_->active_count_);

    // Commit messages should go out.
    client_->getMessage(&response_);
    EXPECT_EQ(0, response_.id);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("output", response_.result);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(0, decision.id);
    EXPECT_EQ(true, decision.commit);
}

TEST_F(OrderedDistributorTest, OneShotReplicaAbort) {
    createSequencer(new OneShotCoordinator());
    distributor_->requestReceived(client_handle_, transaction_);

    EXPECT_EQ(1, coordinator_->active_count_);

    response_.status = ExecutionEngine::ABORT_USER;
    response_.result = "foo";
    distributor_->responseReceived(replica_handles_[1], response_);
    EXPECT_EQ(1, coordinator_->active_count_);
    response_.result.clear();
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_EQ(0, coordinator_->active_count_);

    // Abort messages should go out.
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::ABORT_USER, response_.status);
    // abort response should get passed through
    EXPECT_EQ("foo", response_.result);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);

    // Responses should be ignored
    distributor_->responseReceived(replica_handles_[0], response_);
    distributor_->responseReceived(replica_handles_[1], response_);
}

TEST_F(OrderedDistributorTest, OneShotDeadlockAbort) {
    createSequencer(new OneShotCoordinator());
    distributor_->requestReceived(client_handle_, transaction_);

    // Abort with a message
    response_.status = ExecutionEngine::ABORT_USER;
    response_.result = "foo";
    distributor_->responseReceived(replica_handles_[1], response_);
    EXPECT_EQ(1, coordinator_->active_count_);
    response_.status = ExecutionEngine::OK;

    // abort with a deadlock: no message should go out
    response_.status = ExecutionEngine::ABORT_DEADLOCK;
    response_.result.clear();
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_EQ(0, coordinator_->active_count_);

    // Abort message should go out.
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::ABORT_DEADLOCK, response_.status);
    EXPECT_EQ("", response_.result);
}

TEST_F(OrderedDistributorTest, OneShotTimeout) {
    createSequencer(new OneShotCoordinator());
    distributor_->requestReceived(client_handle_, transaction_);

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
    distributor_->requestReceived(client_handle_, transaction_);
    event_loop_.triggerLastTimeout();
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(false, decision.commit);

    // If response messages come back due to timing issues, they should be ignored
    distributor_->responseReceived(replica_handles_[0], response_);
    distributor_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    distributor_->responseReceived(replica_handles_[0], response_);
    distributor_->responseReceived(replica_handles_[1], response_);
}

class DependencyTest : public OrderedDistributorTest {
public:
    DependencyTest() : OrderedDistributorTest() {
        // Add a third connection
        replicas_.push_back(new MockMessageConnection());
        replica_handles_.push_back(msg_server_.addConnection(replicas_[2]));
        createSequencer(new OneShotCoordinator());
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
    distributor_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // start 3rd multi-partition
    transaction_.id = 2;
    setDestinations(1, 2);
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(3, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from the 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[1], response_);
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[1], response_);

    // get both responses from the 3rd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[2], response_);
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[2], response_);

    // no one has messages
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get commit from first replica
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[0], response_);

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
    distributor_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from the 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[1], response_);

    // get response from first replica
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[0], response_);

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
    distributor_->responseReceived(replica_handles_[2], response_);

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
    distributor_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from the 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[1], response_);

    // get response from first replica
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[0], response_);

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
    distributor_->responseReceived(replica_handles_[2], response_);

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
    distributor_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get 1st response from 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[1], response_);

    // get response from first replica
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[0], response_);

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
    distributor_->responseReceived(replica_handles_[1], response_);
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[2], response_);

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
    distributor_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(1, 2);
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(2, coordinator_->active_count_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();
    replicas_[2]->message_.clear();

    // get responses from 2nd replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[1], response_);
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[1], response_);

    // get response from third replica
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[2], response_);

    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());
    EXPECT_FALSE(replicas_[2]->hasMessage());

    // get abort from first replica
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[0], response_);

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
    distributor_->responseReceived(replica_handles_[1], response_);

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
    distributor_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // get responses from 1st replica
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[0], response_);

    // get abort from 2nd replica
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[1], response_);

    // get the repeat from 1 and answer from 2: answer goes out
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[0], response_);
    distributor_->responseReceived(replica_handles_[1], response_);

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
    distributor_->requestReceived(client_handle_, transaction_);

    // start 2nd multi-partition
    transaction_.id = 1;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // response for b1
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[1], response_);

    // get a1
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[0], response_);

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
    distributor_->responseReceived(replica_handles_[0], response_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // get b2 with dependency: ignored
    response_.status = ExecutionEngine::OK;
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[1], response_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // get b2 without dependency: done
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[1], response_);
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
    distributor_->requestReceived(client_handle_, transaction_);
    transaction_.id = 1;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);
    transaction_.id = 2;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // response for a1
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[0], response_);

    // abort b1
    response_.status = ExecutionEngine::ABORT_USER;
    distributor_->responseReceived(replica_handles_[1], response_);

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
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[0], response_);

    // get a2, a3 with new dependency
    response_.id = 1;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[0], response_);

    // get b2, b3 with dependency: ignored
    response_.id = 1;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);

    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[1], response_);
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
    distributor_->requestReceived(client_handle_, transaction_);
    transaction_.id = 1;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);
    transaction_.id = 2;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // responses for a
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[0], response_);

    // abort b1
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[1], response_);

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
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[0], response_);

    // get b2, b3
    response_.id = 1;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);

    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[1], response_);
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
    distributor_->requestReceived(client_handle_, transaction_);
    transaction_.id = 1;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);
    transaction_.id = 2;
    setDestinations(0, 1);
    distributor_->requestReceived(client_handle_, transaction_);

    // drop previous messages so we don't get confused
    replicas_[0]->message_.clear();
    replicas_[1]->message_.clear();

    // responses for a
    response_.status = ExecutionEngine::OK;
    response_.id = 0;
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 1;
    response_.dependency = 0;
    distributor_->responseReceived(replica_handles_[0], response_);
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[0], response_);

    // abort b1
    response_.status = ExecutionEngine::ABORT_USER;
    response_.id = 0;
    response_.dependency = -1;
    distributor_->responseReceived(replica_handles_[1], response_);

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
    distributor_->responseReceived(replica_handles_[0], response_);
    distributor_->responseReceived(replica_handles_[1], response_);
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(1, decision.id);
    EXPECT_TRUE(decision.commit);

    // get a3, b3
    response_.id = 2;
    response_.dependency = 1;
    distributor_->responseReceived(replica_handles_[0], response_);
    distributor_->responseReceived(replica_handles_[1], response_);
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

TEST_F(OrderedDistributorTest, TwoRoundCommit) {
    createSequencer(new TwoRoundDistributor());
    transaction_.transaction = "start";
    distributor_->requestReceived(client_handle_, transaction_);

    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(true, transaction_.multiple_partitions);
    EXPECT_EQ("one", transaction_.transaction);
    EXPECT_EQ(false, replicas_[1]->hasMessage());

    response_.status = ExecutionEngine::OK;
    response_.result = "one";
    distributor_->responseReceived(replica_handles_[0], response_);

    replicas_[1]->getMessage(&transaction_);
    EXPECT_EQ(true, transaction_.multiple_partitions);
    EXPECT_EQ("two", transaction_.transaction);
    EXPECT_EQ(false, replicas_[0]->hasMessage());

    response_.result = "two";
    distributor_->responseReceived(replica_handles_[1], response_);

    // Commit messages should go out.
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("done multiple", response_.result);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
}

TEST_F(OrderedDistributorTest, TwoRoundClientClose) {
    // Start the transaction
    createSequencer(new TwoRoundDistributor());
    transaction_.transaction = "start";
    distributor_->requestReceived(client_handle_, transaction_);

    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(false, replicas_[1]->hasMessage());

    // The client connection is closed
    msg_server_.connectionClosed(client_);
    client_ = NULL;

    // We continue to execute the transaction
    response_.status = ExecutionEngine::OK;
    response_.result = "one";
    distributor_->responseReceived(replica_handles_[0], response_);

    replicas_[1]->getMessage(&transaction_);
    EXPECT_EQ(false, replicas_[0]->hasMessage());

    response_.result = "two";
    distributor_->responseReceived(replica_handles_[1], response_);

    // Partitions still get messages
    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
}

TEST_F(OrderedDistributorTest, TwoRoundQueuing) {
    createSequencer(new TwoRoundDistributor());
    transaction_.transaction = "start";
    distributor_->requestReceived(client_handle_, transaction_);

    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(true, transaction_.multiple_partitions);
    EXPECT_EQ("one", transaction_.transaction);

    // Queue a single partition transaction
    transaction_.id = 1;
    transaction_.transaction = "single";
    distributor_->requestReceived(client_handle_, transaction_);
    EXPECT_EQ(1, coordinator_->active_count_);
    EXPECT_FALSE(replicas_[0]->hasMessage());
    EXPECT_FALSE(replicas_[1]->hasMessage());

    // Commit first round
    response_.status = ExecutionEngine::OK;
    response_.result = "one";
    distributor_->responseReceived(replica_handles_[0], response_);

    // second round is sent out AND single partition is sent out
    replicas_[1]->getMessage(&transaction_);
    EXPECT_EQ(true, transaction_.multiple_partitions);
    EXPECT_EQ("two", transaction_.transaction);
    replicas_[0]->getMessage(&transaction_);
    EXPECT_EQ(false, transaction_.multiple_partitions);
    EXPECT_EQ("single", transaction_.transaction);    
    EXPECT_EQ(2, coordinator_->active_count_);

    // complete single partition
    response_.id = 1;
    response_.result = "single";
    distributor_->responseReceived(replica_handles_[0], response_);
    client_->getMessage(&response_);
    EXPECT_EQ("done single", response_.result);
    EXPECT_EQ(1, coordinator_->active_count_);

    // complete multiple partition
    response_.id = 0;
    response_.result = "two";
    distributor_->responseReceived(replica_handles_[1], response_);

    // Commit messages should go out.
    client_->getMessage(&response_);
    EXPECT_EQ(ExecutionEngine::OK, response_.status);
    EXPECT_EQ("done multiple", response_.result);

    dtxn::CommitDecision decision;
    replicas_[0]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
    replicas_[1]->getMessage(&decision);
    EXPECT_EQ(true, decision.commit);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
