// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <tr1/unordered_map>

#include "base/assert.h"
#include "base/stlutil.h"
#include "dtxn/executionengine.h"
#include "dtxn/locks/lock.h"
#include "dtxn/locks/lockingcounter.h"
#include "dtxn/locks/lockscheduler.h"
#include "dtxn/locks/locktransaction.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

namespace dtxn {

class LockSchedulerTest : public Test {
public:
    LockSchedulerTest() : manager_(&counter_) {}

    void getAndValidateResults(Transaction* transaction, const string& op,
            int value, ExecutionEngine::Status expected_status = ExecutionEngine::OK) {
        Transaction* out = manager_.getWorkUnitResult();
        EXPECT_EQ(transaction, out);
        EXPECT_EQ(expected_status, out->last_status());
        EXPECT_EQ(op, out->last_work_unit());
        if (expected_status == ExecutionEngine::ABORT_USER) {
            EXPECT_EQ(0, out->output()->size());
        } else {
            assert(expected_status == ExecutionEngine::OK);
            EXPECT_EQ(value, LockingCounter::toValue(*out->output()));
            out->output()->clear();
        }
        out->resultSent();
    }

    Transaction* validateOperation(const string& op, int value) {
        Transaction* transaction = manager_.begin(NULL, op, false, true);
        manager_.idle();
        getAndValidateResults(transaction, op, value);
        return transaction;
    }

    void addAndValidateOperation(Transaction* transaction, const string& op, int value,
            ExecutionEngine::Status expected_status = ExecutionEngine::OK) {
        transaction->addWorkUnit(op, false);
        manager_.workUnitAdded(transaction);
        manager_.idle();
        getAndValidateResults(transaction, op, value, expected_status);
    }

    LockingCounter counter_;
    // TODO: Rename this scheduler to match the other scheduler tests?
    LockScheduler manager_;
};

TEST_F(LockSchedulerTest, BadCreate) {
    EXPECT_DEATH(LockScheduler(NULL));
}

TEST_F(LockSchedulerTest, SequentialTransactions) {
    Transaction* transaction = validateOperation(OP_GET, 1);
    manager_.commit(transaction);
    
    transaction = validateOperation(OP_ADD, 2);
    manager_.commit(transaction);
}

TEST_F(LockSchedulerTest, ConcurrentRead) {
    Transaction* t1 = validateOperation(OP_GET, 1);

    Transaction* t2 = validateOperation(OP_GET, 1);

    manager_.commit(t2);
    manager_.commit(t1);
}

TEST_F(LockSchedulerTest, ReadWrite) {
    Transaction* t1 = validateOperation(OP_GET, 1);

    Transaction* t2 = manager_.begin(NULL, OP_ADD, false, true);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    // We cannot commit a transaction with pending work
    EXPECT_DEATH(manager_.commit(t2));
    // We cannot add work to a blocked transaction
    EXPECT_DEATH(t2->addWorkUnit(OP_GET, false));
    manager_.commit(t1);
    manager_.idle();

    // now we get results for t2
    getAndValidateResults(t2, OP_ADD, 2);
    manager_.commit(t2);
}

TEST_F(LockSchedulerTest, SinglePartitionWithoutLocks) {
    // Start an MP transaction
    Transaction* mp = validateOperation(OP_ADD, 2);
    EXPECT_TRUE(counter_.last_with_locks_);

    // Execute an SP transaction on top: it must use locks
    Transaction* sp = manager_.begin(NULL, OP_GET, true, false);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    // Add a conflicting SP transaction
    Transaction* sp2 = manager_.begin(NULL, OP_ADD, true, false);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());
    
    // Commit mp: this causes SP to be queued for execution
    manager_.commit(mp);
    EXPECT_FALSE(manager_.idle());
    getAndValidateResults(sp, OP_GET, 2);
    getAndValidateResults(sp2, OP_ADD, 3);
    manager_.commit(sp);
    manager_.commit(sp2);

    // Nothing active: execute without locks
    sp = manager_.begin(NULL, OP_ADD, true, false);
    manager_.idle();
    getAndValidateResults(sp, OP_ADD, 4);
    EXPECT_FALSE(counter_.last_with_locks_);
    manager_.commit(sp);
}

TEST_F(LockSchedulerTest, UserAbort) {
    Transaction* t1 = validateOperation(OP_ADD, 2);

    Transaction* t2 = manager_.begin(NULL, OP_GET, false, true);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    addAndValidateOperation(t1, OP_ADD, 3);

    addAndValidateOperation(t1, OP_ABORT, 0, ExecutionEngine::ABORT_USER);
    manager_.abort(t1);
    manager_.idle();

    // TODO: Should we verify the transaction pointers?
    // EXPECT_DEATH(manager_.abort(t1));

    // now we get results for t2
    getAndValidateResults(t2, OP_GET, 1);
    manager_.commit(t2);
}

TEST_F(LockSchedulerTest, ExternalAbort) {
    Transaction* t1 = validateOperation(OP_ADD, 2);
    Transaction* t2 = manager_.begin(NULL, OP_GET, false, true);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    // external abort
    manager_.abort(t1);
    manager_.idle();

    // the other transaction completes
    getAndValidateResults(t2, OP_GET, 1);
    manager_.commit(t2);
}

TEST_F(LockSchedulerTest, AbortBlockedTransaction) {
    Transaction* t1 = validateOperation(OP_ADD, 2);
    Transaction* t2 = manager_.begin(NULL, OP_GET, false, true);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    // abort the blocked transaction
    manager_.abort(t2);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    manager_.commit(t1);
}

TEST_F(LockSchedulerTest, AbortUnprocessedTransaction) {
    // t1 is in the to be executed queue when the abort arrives.
    Transaction* t1 = manager_.begin(NULL, OP_ADD, false, true);
    manager_.abort(t1);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());
}

TEST_F(LockSchedulerTest, AbortUnsentTransaction) {
    // Start a transaction that is executed and completed
    Transaction* txn = manager_.begin(NULL, OP_ADD, false, true);
    manager_.idle();
    EXPECT_EQ(1, counter_.execute_count_);

    // Abort the transaction, such as via timeout: removed from the result queue
    // TODO: See comment in orderedscheduler_test.cc
    manager_.abort(txn);
    manager_.idle();
    EXPECT_EQ(1, counter_.undo_count_);
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());
}

TEST_F(LockSchedulerTest, BackupApplyTransaction) {
    vector<string> transaction;
    transaction.push_back(OP_ADD);
    transaction.push_back(OP_ADD);

    // Apply a transaction as a backup
    manager_.backupApplyTransaction(transaction);
    EXPECT_EQ(3, counter_.counter());

    // Begin an MP transaction that holds locks
    Transaction* txn = manager_.begin(NULL, OP_ADD, true, true);
    manager_.idle();

    // Rolling log forward when transactions are active is not permitted
    EXPECT_DEATH(manager_.backupApplyTransaction(transaction));
    manager_.abort(txn);
}

TEST_F(LockSchedulerTest, Deadlock) {
    // Both read the variable
    Transaction* t1 = validateOperation(OP_GET, 1);
    Transaction* t2 = validateOperation(OP_GET, 1);

    // Both try to write the variable: deadlock
    t1->addWorkUnit(OP_ADD, false);
    manager_.workUnitAdded(t1);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());
    t2->addWorkUnit(OP_ADD, false);
    manager_.workUnitAdded(t2);
    manager_.idle();
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    // The transaction manager will find the two transaction cycle
    vector<LockTransaction*> cycle = manager_.detectDeadlock();
    EXPECT_EQ(2, cycle.size());
    EXPECT_TRUE(base::contains(cycle, t1));
    EXPECT_TRUE(base::contains(cycle, t2));

    // Abort t1
    manager_.abort(t1);

    // no more cycles
    cycle = manager_.detectDeadlock();
    EXPECT_EQ(0, cycle.size());

    // The other transaction should complete correctly
    manager_.idle();
    getAndValidateResults(t2, OP_ADD, 2);
    manager_.commit(t2);
}

// TODO: Test prioritization: sp txns should be killed over mp txns
TEST_F(LockSchedulerTest, DeadlockDetectOnIdle) {
    // Both read the variable
    Transaction* t1 = validateOperation(OP_GET, 1);
    Transaction* t2 = validateOperation(OP_GET, 1);
    EXPECT_FALSE(manager_.idle());  // no deadlock possible (0 blocked transactions)
    EXPECT_FALSE(manager_.idle());

    // Both try to write the variable: deadlock
    t1->addWorkUnit(OP_ADD, false);
    manager_.workUnitAdded(t1);
    EXPECT_FALSE(manager_.idle());  // no deadlock possible (1 blocked transactions)
    EXPECT_FALSE(manager_.idle());
    t2->addWorkUnit(OP_ADD, false);
    manager_.workUnitAdded(t2);

    // Idle: Executes the transaction; does not do deadlock detection, but we should poll due to
    // blocked transactions.
    EXPECT_TRUE(manager_.idle());
    EXPECT_EQ(4, counter_.execute_count_);

    // First call to idle with blocked transactions: no deadlock detection yet, but we should poll
    EXPECT_TRUE(manager_.idle());
    EXPECT_EQ(NULL, manager_.getWorkUnitResult());

    // The second call to idle causes deadlock detection to trigger
    EXPECT_FALSE(manager_.idle());
    int abort_count = 0;
    int commit_count = 0;
    Transaction* out = NULL;
    while ((out = manager_.getWorkUnitResult()) != NULL) {
        if (out == t1) {
            t1 = NULL;
        } else {
            assert(out == t2);
            t2 = NULL;
        }

        if (out->last_status() == ExecutionEngine::ABORT_DEADLOCK) {
            abort_count += 1;
            manager_.abort(out);
        } else {
            assert(out->last_status() == ExecutionEngine::OK);
            commit_count += 1;
            manager_.commit(out);
        }
        manager_.idle();
    }
    EXPECT_EQ(1, abort_count);
    EXPECT_EQ(1, commit_count);
    EXPECT_EQ(5, counter_.execute_count_);
}

void doRandomDeadlockTest() {
    // Randomly generates some transactions, then uses the cycle detector to
    // see which transactions are blocked
    //~ srand(time(NULL));
    static const int NUM_VARIABLES = 10;
    static const int NUM_TRANSACTIONS = 5;
    static const int NUM_ROUNDS = 4;

    Transaction* transactions[NUM_TRANSACTIONS] = {};  // C++-ism: initialize to 0
    int missing_work[NUM_TRANSACTIONS] = {};  // C++-ism: initialize to 0
    MultiLockingCounter counter(NUM_VARIABLES);
    LockScheduler manager(&counter);

    std::tr1::unordered_set<LockTransaction*> in_cycle;
    for (int round = 0; round < NUM_ROUNDS; ++round) {
        for (int i = 0; i < sizeof(transactions)/sizeof(*transactions); ++i) {
            // if this transaction is blocked already, skip it
            if (missing_work[i] > 0) {
                continue;
            }

            // select a variable at random
            int variable = rand() % NUM_VARIABLES;
            // select an operation at random
            const string& operation = (rand() % 2 == 0) ? OP_ADD : OP_GET;

            // dispatch the operation
            string op = MultiLockingCounter::makeWork(variable, operation);
            if (transactions[i] == NULL) {
                transactions[i] = manager.begin(NULL, op, false, true);
            } else {
                transactions[i]->addWorkUnit(op, false);
                manager.workUnitAdded(transactions[i]);
            }

            // try to get results
            manager.idle();
            Transaction* transaction_out = manager.getWorkUnitResult();
            if (transaction_out == NULL) {
                missing_work[i] += 1;
            } else {
                assert(transaction_out->last_status() == ExecutionEngine::OK);
                assert(missing_work[i] == 0);
                assert(transaction_out == transactions[i]);
                assert(transaction_out->last_work_unit() == op);
                assert(in_cycle.find((LockTransaction*) transactions[i]) == in_cycle.end());
                // TODO: Validate that results match the serial order?
                transaction_out->resultSent();
            }

            // look for a cycle
            vector<LockTransaction*> cycle = manager.detectDeadlock();
            for (int j = 0; j < cycle.size(); ++j) {
                ptrdiff_t index = std::find(transactions, transactions + NUM_TRANSACTIONS, cycle[j]) - transactions;
                ASSERT(0 <= index && index < NUM_TRANSACTIONS);
                ASSERT(transactions[index] == cycle[j]);
                ASSERT(missing_work[index] > 0);
                in_cycle.insert(cycle[j]);
            }
        }
    }

    // At this point, we have some set of completed, uncomitted and blocked transactions
    // commit as many transactions as possible
    int finished = 0;
    while (finished < NUM_TRANSACTIONS) {
        // Commit all the transactions we can
        int blocked_transactions = 0;
        for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
            // process unfinished transactions
            if (transactions[i] != NULL) {
                if (missing_work[i] == 0) {
                    // this transaction is complete
                    //~ printf("committing %d %p\n", i, transactions[i]);
                    assert(in_cycle.find((LockTransaction*) transactions[i]) == in_cycle.end());
                    manager.commit(transactions[i]);
                    transactions[i] = NULL;
                    finished += 1;
                } else {
                    blocked_transactions += 1;
                }
            }
        }
        assert(blocked_transactions > 0 || finished == NUM_TRANSACTIONS);

        // if we have cycles, we *must* have blocked transactions
        if (!in_cycle.empty()) {
            assert(blocked_transactions > 0);
        }

        // pull out results and update the transactions
        Transaction* transaction_out;
        bool got_results = false;
        manager.idle();
        while ((transaction_out = manager.getWorkUnitResult()) != NULL) {
            got_results = true;
            assert(transaction_out->last_status() == ExecutionEngine::OK);

            ptrdiff_t index = std::find(transactions, transactions + NUM_TRANSACTIONS, transaction_out) - transactions;
            assert(0 <= index && index < NUM_TRANSACTIONS);
            assert(transactions[index] == transaction_out);
            assert(missing_work[index] == 1);
            missing_work[index] = 0;
            blocked_transactions -= 1;
            assert(blocked_transactions >= 0);

            // remove this transaction from our cycle record, if we have one
            //~ printf("finished: %p\n", transaction_out);
            in_cycle.erase((LockTransaction*) transaction_out);
        }

        if (!got_results && blocked_transactions > 0) {
            // nothing changed: break a cycle by aborting a transaction at random
            vector<LockTransaction*> cycle = manager.detectDeadlock();
            assert(!cycle.empty());
            int index = rand() % static_cast<int>(cycle.size());
            for (int i = 0; i < NUM_TRANSACTIONS; ++i) {
                if (transactions[i] != NULL) {
                    index -= 1;
                    if (index == -1) {
                        // abort this transaction
                        //~ printf("aborting %d %p\n", i, transactions[i]);
                        manager.abort(transactions[i]);
                        in_cycle.erase((LockTransaction*) transactions[i]);
                        transactions[i] = NULL;
                        missing_work[i] = 0;
                        blocked_transactions -= 1;
                        assert(blocked_transactions >= 0);
                        finished += 1;
                        break;
                    }

                    assert(index >= 0);
                }
            }
            assert(index == -1);
        }

        // look for a cycle
        vector<LockTransaction*> cycle = manager.detectDeadlock();
        for (int i = 0; i < cycle.size(); ++i) {
            ptrdiff_t index = std::find(transactions, transactions + NUM_TRANSACTIONS, cycle[i]) - transactions;
            ASSERT(0 <= index && index < NUM_TRANSACTIONS);
            ASSERT(transactions[index] == cycle[i]);
            ASSERT(missing_work[index] > 0);
            in_cycle.insert(cycle[i]);
        }
        if (!cycle.empty()) {
            assert(blocked_transactions > 0);
        }
    }
    assert(finished == NUM_TRANSACTIONS);
}

TEST(DeadlockValidator, Random) {
    static const int NUM_TRIALS = 3000;
    srand(static_cast<unsigned int>(time(NULL)));
    for (int trial = 0; trial < NUM_TRIALS; ++trial) {
        //~ printf("\n%d\n", trial);
        doRandomDeadlockTest();
    }
}

}  // namespace dtxn

int main() {
    return TestSuite::globalInstance()->runAll();
}
