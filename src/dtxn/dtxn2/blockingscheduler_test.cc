// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/messages.h"
#include "dtxn/mockexecutionengine.h"
#include "dtxn2/blockingscheduler.h"
#include "dtxn2/mockscheduler.h"
#include "dtxn2/transactionstate.h"
#include "stupidunit/stupidunit.h"

using dtxn::ExecutionEngine;
using dtxn::MockExecutionEngine;
using dtxn::Fragment;
using namespace dtxn2;

class BlockingSchedulerTest : public Test {
public:
    BlockingSchedulerTest() : scheduler_(&engine_), fragment_state_(NULL) {
        engine_.result_ = "out";
        fragment_.multiple_partitions = false;
        fragment_.last_fragment = true;
        fragment_.transaction = "foo";
        fragment_.client_id = 1;
        fragment_.id = 1;
    }

    MockExecutionEngine engine_;
    MockSchedulerOutput output_;
    BlockingScheduler scheduler_;

    MockTransactionState txn_state_;
    MockTransactionState txn_state2_;
    Fragment fragment_;
    FragmentState* fragment_state_;
};

TEST_F(BlockingSchedulerTest, Idle) {
    // Nothing is done yet
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(NULL, output_.last_executed_);
}

TEST_F(BlockingSchedulerTest, SinglePartition) {
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(&txn_state_, output_.last_replicate_);
    EXPECT_EQ(txn_state_.mutable_last(), output_.last_executed_);

    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(ExecutionEngine::OK, txn_state_.last_fragment().response().status);
    EXPECT_EQ("out", txn_state_.last_fragment().response().result);

    // Can't commit or abort single partition transactions
    EXPECT_DEATH(scheduler_.decide(&txn_state_, true));
    EXPECT_DEATH(scheduler_.decide(&txn_state_, false));
}

TEST_F(BlockingSchedulerTest, SinglePartitionAbort) {
    engine_.commit_ = false;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(&txn_state_, output_.last_replicate_);
    EXPECT_EQ(txn_state_.mutable_last(), output_.last_executed_);

    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(ExecutionEngine::ABORT_USER, txn_state_.last_fragment().response().status);
    EXPECT_EQ("out", txn_state_.last_fragment().response().result);
}

TEST_F(BlockingSchedulerTest, MultiPartitionOneRoundCommitBlockOthers) {
    fragment_.multiple_partitions = true;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);

    // Start a second transaction: replicated but not executed
    fragment_.multiple_partitions = false;
    txn_state2_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state2_);
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(2, output_.replicate_count_);
    EXPECT_EQ(&txn_state2_, output_.last_replicate_);
    EXPECT_EQ(txn_state_.mutable_last(), output_.last_executed_);
    output_.reset();

    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(ExecutionEngine::OK, txn_state_.last_fragment().response().status);
    EXPECT_EQ("out", txn_state_.last_fragment().response().result);

    // Nothing happens on idle
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(NULL, output_.last_executed_);

    // Commit the multi-partition transaction
    scheduler_.decide(&txn_state_, true);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.free_count_);

    // Output single partition
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(txn_state2_.mutable_last(), output_.last_executed_);
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(ExecutionEngine::OK, txn_state2_.last_fragment().response().status);
    EXPECT_EQ("out", txn_state2_.last_fragment().response().result);
}

TEST_F(BlockingSchedulerTest, MultiPartitionOneRoundAbort) {
    fragment_.multiple_partitions = true;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);

    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(1, engine_.undo_stack_.size());

    // Abort the multi-partition transaction
    scheduler_.decide(&txn_state_, false);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.undo_count_);
    EXPECT_FALSE(scheduler_.doWork(&output_));
}

TEST_F(BlockingSchedulerTest, MultiPartitionMultiRoundCommit) {
    fragment_.last_fragment = false;
    fragment_.multiple_partitions = true;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);

    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(txn_state_.mutable_last(), output_.last_executed_);
    EXPECT_EQ(1, engine_.undo_stack_.size());
    output_.reset();

    // Add a single partition transaction
    fragment_.last_fragment = true;
    fragment_.multiple_partitions = false;
    txn_state2_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state2_);
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(NULL, output_.last_executed_);

    // Cannot commit: unfinished!
    EXPECT_DEATH(scheduler_.decide(&txn_state_, true));

    // Add the last fragment: both are replicated; mp is executed
    fragment_.last_fragment = true;
    fragment_.multiple_partitions = true;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(&txn_state2_, output_.last_replicate_);
    EXPECT_EQ(2, output_.replicate_count_);
    EXPECT_EQ(txn_state_.mutable_last(), output_.last_executed_);
    output_.reset();

    // idle does nothing
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(NULL, output_.last_executed_);

    // Commit
    scheduler_.decide(&txn_state_, true);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.free_count_);

    // Single partition transaction is done
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(txn_state2_.mutable_last(), output_.last_executed_);
    EXPECT_EQ(3, engine_.execute_count_);
}

TEST_F(BlockingSchedulerTest, MultiPartitionCommitUnfinished) {
    fragment_.last_fragment = false;
    fragment_.multiple_partitions = true;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);

    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.execute_count_);
    output_.reset();

    // Prepare the unfinished transaction
    fragment_.last_fragment = true;
    fragment_.multiple_partitions = true;
    fragment_.transaction.clear();
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.execute_count_);
    EXPECT_EQ(&txn_state_, output_.last_replicate_);
    EXPECT_EQ(txn_state_.mutable_last(), output_.last_executed_);
    output_.reset();

    // commit
    scheduler_.decide(&txn_state_, true);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.free_count_);

    // idle does nothing
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(NULL, output_.last_executed_);
}

TEST_F(BlockingSchedulerTest, MultiPartitionAbortUnfinished) {
    fragment_.last_fragment = false;
    fragment_.multiple_partitions = true;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);

    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(1, engine_.undo_stack_.size());
    output_.reset();

    // Add a single partition transaction
    fragment_.last_fragment = true;
    fragment_.multiple_partitions = false;
    txn_state2_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state2_);
    EXPECT_FALSE(scheduler_.doWork(&output_));

    // Abort the unfinished transaction
    scheduler_.decide(&txn_state_, false);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.undo_count_);

    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(2, engine_.execute_count_);
    EXPECT_EQ(&txn_state2_, output_.last_replicate_);
    EXPECT_EQ(1, output_.replicate_count_);
    EXPECT_EQ(txn_state2_.mutable_last(), output_.last_executed_);
    output_.reset();

    // idle does nothing
    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(NULL, output_.last_replicate_);
    EXPECT_EQ(NULL, output_.last_executed_);
}

TEST_F(BlockingSchedulerTest, SinglePartitionMultiRound) {
    fragment_.last_fragment = false;
    fragment_.multiple_partitions = true;
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);

    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(1, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.execute_count_);
    output_.reset();

    fragment_.last_fragment = true;
    fragment_.multiple_partitions = false;
    fragment_.transaction.clear();
    txn_state_.addFragment(fragment_);
    scheduler_.fragmentArrived(&txn_state_);

    EXPECT_FALSE(scheduler_.doWork(&output_));
    EXPECT_EQ(&txn_state_, output_.last_replicate_);
    EXPECT_EQ(txn_state_.mutable_last(), output_.last_executed_);
    EXPECT_EQ(0, engine_.undo_stack_.size());
    EXPECT_EQ(1, engine_.free_count_);
    EXPECT_EQ(1, engine_.execute_count_);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
