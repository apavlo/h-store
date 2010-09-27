// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/executionengine.h"
#include "dtxn/transactionstate.h"
#include "stupidunit/stupidunit.h"

using namespace dtxn;
using std::string;

TEST(ReplicationStateMachine, ReplicateExecute) {
    ReplicationStateMachine state;
    EXPECT_FALSE(state.done());
    EXPECT_TRUE(state.initial_state());

    state.setReplicated();
    EXPECT_DEATH(state.setReplicated());
    EXPECT_FALSE(state.done());
    EXPECT_FALSE(state.initial_state());

    state.setExecuted();
    EXPECT_DEATH(state.setReplicated());
    EXPECT_DEATH(state.setExecuted());
    EXPECT_TRUE(state.done());
    EXPECT_FALSE(state.initial_state());
}

TEST(ReplicationStateMachine, ExecuteReplicate) {
    ReplicationStateMachine state;

    state.setExecuted();
    EXPECT_DEATH(state.setExecuted());
    EXPECT_FALSE(state.done());
    EXPECT_FALSE(state.initial_state());

    state.setReplicated();
    EXPECT_DEATH(state.setReplicated());
    EXPECT_DEATH(state.setReplicated());
    EXPECT_TRUE(state.done());
    EXPECT_FALSE(state.initial_state());
}

class TransactionStateTest : public Test {
public:
    TransactionStateTest() : fragment_state_(NULL) {
        fragment_.client_id = INITIAL_ID-1;
        fragment_.id = INITIAL_ID;
        fragment_.transaction = "foo";
    }

    static const int INITIAL_ID = 42;

    Fragment fragment_;
    FragmentResponse* response_;
    TransactionState transaction_;
    FragmentState* fragment_state_;
};

TEST_F(TransactionStateTest, BadFragment) {
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = false;
    EXPECT_DEATH(transaction_.addFragment(fragment_));
}

TEST_F(TransactionStateTest, Undo) {
    EXPECT_NE(transaction_.undo(), NULL);
    EXPECT_EQ(NULL, *transaction_.undo());
    *transaction_.undo() = this;

    EXPECT_EQ(this, *transaction_.undo());
    transaction_.clear();
    EXPECT_EQ(NULL, *transaction_.undo());
}

TEST_F(TransactionStateTest, SinglePartition) {
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    fragment_state_ = transaction_.addFragment(fragment_);

    // Fragment is copied
    EXPECT_FALSE(fragment_state_->multiple_partitions());
    EXPECT_TRUE(fragment_state_->request().last_fragment);
    EXPECT_EQ(fragment_.transaction, fragment_state_->request().transaction);

    // Replication complete
    fragment_state_->setReplicated();
    EXPECT_FALSE(fragment_state_->readyToSend());
    EXPECT_DEATH(fragment_state_->setReplicated());

    // Execution complete
    EXPECT_EQ(INITIAL_ID, fragment_state_->response().id);  // id is set for us
    fragment_state_->setHasResults();
    EXPECT_TRUE(fragment_state_->readyToSend());
    EXPECT_DEATH(fragment_state_->setHasResults());
}

TEST_F(TransactionStateTest, MultipleRoundsSinglePartition) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    fragment_state_ = transaction_.addFragment(fragment_);

    // Add one that makes this single partition
    fragment_.multiple_partitions = false;
    fragment_.last_fragment = true;
    fragment_state_ = transaction_.addFragment(fragment_);
}

TEST_F(TransactionStateTest, MultiplePartitionsOneShot) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = true;
    fragment_state_ = transaction_.addFragment(fragment_);

    EXPECT_TRUE(fragment_state_->multiple_partitions());
    EXPECT_TRUE(fragment_state_->request().last_fragment);

    EXPECT_DEATH(transaction_.addFragment(fragment_));

    // Execution complete
    fragment_state_->setHasResults();
    EXPECT_FALSE(fragment_state_->readyToSend());
    // we can call setHasResults multiple times: dependency aborts
    fragment_state_->setHasResults();

    // Replication complete
    fragment_state_->setReplicated();
    EXPECT_TRUE(fragment_state_->readyToSend());
    EXPECT_DEATH(fragment_state_->setReplicated());
    fragment_state_->setHasResults();

    // Decide the transaction: commit
    transaction_.decide(true);
    EXPECT_TRUE(transaction_.decision());
    transaction_.decisionExecuted();

    // Replicate the decision
    EXPECT_FALSE(transaction_.finished());
    transaction_.decisionReplicated();
    EXPECT_TRUE(transaction_.finished());
}

TEST_F(TransactionStateTest, MultiplePartitionsUserAbort) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    fragment_state_ = transaction_.addFragment(fragment_);

    // Execution complete
    fragment_state_->mutable_response()->status = ExecutionEngine::ABORT_USER;
    fragment_state_->setHasResults();
    fragment_state_->setReplicated();

    // Trying to add another fragment: does not work due to the abort!
    EXPECT_DEATH(transaction_.addFragment(fragment_));
}

TEST_F(TransactionStateTest, QueuedFinished) {
    fragment_.multiple_partitions = true;
    fragment_.last_fragment = false;
    fragment_state_ = transaction_.addFragment(fragment_);
    fragment_state_->mutable_response()->status = ExecutionEngine::OK;
    fragment_state_->setReplicated();

    // Mark the transaction as completed
    transaction_.decide(true);
    transaction_.decisionReplicated();
    transaction_.decisionExecuted();
    // NOT DONE YET: We haven't pulled the results out of the queue yet
    EXPECT_FALSE(transaction_.finished());

    fragment_state_->setHasResults();
    EXPECT_TRUE(transaction_.finished());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
