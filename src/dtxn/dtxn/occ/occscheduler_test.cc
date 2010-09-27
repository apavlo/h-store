// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/locks/lockingcounter.h"
#include "dtxn/occ/occscheduler.h"
#include "stupidunit/stupidunit.h"

using std::string;

using namespace dtxn;

class OCCSchedulerTest : public Test {
public:
    OCCSchedulerTest() : counter_(NUM_COUNTERS), scheduler_(&counter_) {}

    void getAndValidateResults(Transaction* transaction, int counter, const std::string& op,
            int value, ExecutionEngine::Status expected_status = ExecutionEngine::OK) {
        assert(counter < NUM_COUNTERS);
        EXPECT_FALSE(scheduler_.idle());
        Transaction* out = scheduler_.getWorkUnitResult();
        ASSERT_EQ(transaction, out);
        EXPECT_EQ(expected_status, out->last_status());
        EXPECT_EQ(makeWork(counter, op), out->last_work_unit());
        if (expected_status == ExecutionEngine::ABORT_USER) {
            EXPECT_EQ(0, out->output()->size());
        } else {
            assert(expected_status == ExecutionEngine::OK);
            EXPECT_EQ(value, LockingCounter::toValue(*out->output()));
            out->output()->clear();
        }
        out->resultSent();
    }

    // makes one-shot transactions
    Transaction* validateOperation(int counter, const std::string& op, int value) {
        assert(counter < NUM_COUNTERS);
        Transaction* transaction =
            scheduler_.begin(NULL, makeWork(counter, op), true, true);
        getAndValidateResults(transaction, counter, op, value);
        return transaction;
    }

    string makeWork(int counter, const string& op) {
        return MultiLockingCounter::makeWork(counter, op);
    }

    static const int NUM_COUNTERS = 2;
    MultiLockingCounter counter_;
    OCCScheduler scheduler_;
};

TEST_F(OCCSchedulerTest, BadCreate) {
    EXPECT_DEATH(OCCScheduler(NULL));
}

TEST_F(OCCSchedulerTest, IdleNothingToDo) {
    // No work queued: idle callback returns false to block on network.
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_FALSE(scheduler_.idle());

    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());
}

TEST_F(OCCSchedulerTest, SinglePartition) {
    Transaction* txn = validateOperation(0, OP_ADD, 2);
    EXPECT_FALSE(scheduler_.idle());
    scheduler_.commit(txn);
}

TEST_F(OCCSchedulerTest, MultiPhaseNotSupported) {
    // mp txn begins execution
    Transaction* mp = scheduler_.begin(NULL, makeWork(0, OP_GET), false, true);
    EXPECT_FALSE(scheduler_.idle());
    mp->resultSent();
    mp->addWorkUnit(OP_ADD, true);
    // TODO: We should support this
    EXPECT_DEATH(scheduler_.workUnitAdded(mp));
    scheduler_.abort(mp);
}

TEST_F(OCCSchedulerTest, OneShotNoDependencies) {
    // one shot txn begins execution
    Transaction* mp = validateOperation(0, OP_GET, 1);

    // a non-depending sp txn begins and commits
    Transaction* sp = scheduler_.begin(NULL, makeWork(0, OP_ADD), true, false);
    getAndValidateResults(sp, 0, OP_ADD, 2);
    scheduler_.commit(sp);

    // commit the mp transaction: the sp txn finishes
    scheduler_.commit(mp);
}

TEST_F(OCCSchedulerTest, OneShotDependencies) {
    // one shot txn begins execution
    Transaction* mp = validateOperation(0, OP_ADD, 2);

    // a depending sp txn begins 
    Transaction* sp = scheduler_.begin(NULL, makeWork(0, OP_GET), true, false);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // commit the mp transaction: the sp txn finishes
    scheduler_.commit(mp);
    getAndValidateResults(sp, 0, OP_GET, 2);
    scheduler_.commit(sp);
}

TEST_F(OCCSchedulerTest, OneShotCascadingAbort) {
    // one shot txn begins execution
    Transaction* mp = validateOperation(0, OP_ADD, 2);

    // a depending sp txn begins 
    Transaction* sp = scheduler_.begin(NULL, makeWork(0, OP_GET), true, false);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // abort the one shot, the depending txn is re-executed and committed
    scheduler_.abort(mp);
    getAndValidateResults(sp, 0, OP_GET, 1);
    scheduler_.commit(sp);
}

TEST_F(OCCSchedulerTest, AbortReadOnlyAbort) {
    LockingCounter counter;
    OCCScheduler scheduler(&counter);
    Transaction* mp = scheduler.begin(NULL, OP_GET, true, true);
    EXPECT_FALSE(scheduler.idle());
    scheduler.abort(mp);

    mp = scheduler.begin(NULL, OP_ADD, true, true);
    Transaction* sp = scheduler.begin(NULL, OP_GET, true, false);
    EXPECT_FALSE(scheduler.idle());
    scheduler.abort(mp);
    EXPECT_FALSE(scheduler.idle());
    scheduler.commit(sp);
}

TEST_F(OCCSchedulerTest, OneShotReorder) {
    Transaction* one = validateOperation(0, OP_ADD, 2);

    // This transaction is dependent and is "speculative"
    Transaction* two = scheduler_.begin(NULL, makeWork(0, OP_ADD), true, true);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // a non-interfering one shot transaction can be re-ordered and committed
    Transaction* three = validateOperation(1, OP_ADD, 2);

    // a dependent sp transaction is speculative
    Transaction* sp = scheduler_.begin(NULL, makeWork(1, OP_GET), true, true);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // commit three, sp does not finish
    // TODO: Change this behaviour?
    scheduler_.commit(three);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // commit one, two and sp finishes
    scheduler_.commit(one);
    getAndValidateResults(two, 0, OP_ADD, 3);
    getAndValidateResults(sp, 1, OP_GET, 2);

    scheduler_.commit(sp);
    scheduler_.commit(two);
}

TEST_F(OCCSchedulerTest, CommitAbortSpeculative) {
    Transaction* one = validateOperation(0, OP_ADD, 2);
    Transaction* spec = scheduler_.begin(NULL, makeWork(0, OP_ADD), true, true);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());

    // committing a speculative transaction: not permitted
    EXPECT_DEATH(scheduler_.commit(spec));
    // TODO: aborting a speculative transaction: should be permitted (eg. timeouts?)
    EXPECT_DEATH(scheduler_.abort(spec));

    scheduler_.abort(one);
    getAndValidateResults(spec, 0, OP_ADD, 2);
    scheduler_.abort(spec);
}

TEST_F(OCCSchedulerTest, AbortUnsentTransaction) {
    // Start a transaction that is executed and completed
    Transaction* txn = scheduler_.begin(NULL, makeWork(0, OP_ADD), false, true);
    EXPECT_FALSE(scheduler_.idle());
    EXPECT_EQ(1, counter_.counter(0).execute_count_);

    // Abort the transaction, such as via timeout: removed from the result queue
    // TODO: See comment in orderedscheduler_test.cc
    scheduler_.abort(txn);
    EXPECT_EQ(1, counter_.counter(0).undo_count_);
    EXPECT_EQ(NULL, scheduler_.getWorkUnitResult());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
