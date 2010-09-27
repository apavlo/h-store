// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <algorithm>

#include "base/array.h"
#include "base/stlutil.h"
#include "dtxn/locks/lock.h"
#include "dtxn/locks/locktransaction.h"
#include "stupidunit/stupidunit.h"

using base::contains;
using std::vector;

namespace dtxn {

class LockTest : public Test {
public:
    LockTest() :
            txn_a_(new LockTransaction(NULL, "foo", false, true)),
            txn_b_(new LockTransaction(NULL, "foo", false, true)),
            txn_c_(new LockTransaction(NULL, "foo", false, true)) {}

    ~LockTest() {
        delete txn_a_;
        delete txn_b_;
        delete txn_c_;
    }

    LockTransaction* txn_a_;
    LockTransaction* txn_b_;
    LockTransaction* txn_c_;
    vector<LockTransaction*> granted_transactions_;
    Lock lock_;
};

TEST_F(LockTest, ReadLockNoContention) {
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_b_));
    EXPECT_TRUE(lock_.tryReadLock(txn_b_));

    lock_.unlock(txn_a_, &granted_transactions_);
    lock_.unlock(txn_b_, &granted_transactions_);
    EXPECT_EQ(0, granted_transactions_.size());

    EXPECT_DEATH(lock_.unlock(txn_a_, &granted_transactions_));
    EXPECT_DEATH(lock_.unlock(txn_b_, &granted_transactions_));
}

TEST_F(LockTest, WriteLockNoContention) {
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));

    lock_.unlock(txn_a_, &granted_transactions_);
    EXPECT_DEATH(lock_.unlock(txn_a_, &granted_transactions_));
}

TEST_F(LockTest, WriteWriteContention) {
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
    // Not permitted to acquire lock: waiting on transaction a
    EXPECT_FALSE(lock_.tryWriteLock(txn_b_));

    // a unlocks; lock granted to b
    lock_.unlock(txn_a_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_b_));

    EXPECT_DEATH(lock_.unlock(txn_a_, &granted_transactions_));
    EXPECT_FALSE(lock_.tryWriteLock(txn_c_));
    EXPECT_TRUE(lock_.tryWriteLock(txn_b_));
}

TEST_F(LockTest, WriteLockPermitsRead) {
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_FALSE(lock_.tryReadLock(txn_b_));
}

TEST_F(LockTest, WriteReadContention) {
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
    // Not permitted to acquire lock: waiting on transaction a
    EXPECT_FALSE(lock_.tryReadLock(txn_b_));
    EXPECT_FALSE(lock_.tryReadLock(txn_c_));

    // a unlocks; lock granted to b and c
    lock_.unlock(txn_a_, &granted_transactions_);
    EXPECT_EQ(2, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_b_));
    EXPECT_TRUE(contains(granted_transactions_, txn_c_));
    
    EXPECT_DEATH(lock_.unlock(txn_a_, &granted_transactions_));
    // a can lock again
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
}

TEST_F(LockTest, ReadWriteContention) {
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_b_));
    // waiting on a and b
    EXPECT_FALSE(lock_.tryWriteLock(txn_c_));
    // a and b still have the lock
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_b_));

    lock_.unlock(txn_a_, &granted_transactions_);
    EXPECT_EQ(0, granted_transactions_.size());
    // a tries to read again: fails because of queued writer
    EXPECT_FALSE(lock_.tryReadLock(txn_a_));
    // still queued: this should not change queue position
    EXPECT_FALSE(lock_.tryWriteLock(txn_c_));

    lock_.unlock(txn_b_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_c_));
    EXPECT_TRUE(lock_.tryWriteLock(txn_c_));

    // c holds write lock, a queued for reading, queue b for writing, then a for reading again
    EXPECT_FALSE(lock_.tryWriteLock(txn_b_));
    EXPECT_FALSE(lock_.tryReadLock(txn_a_));

    granted_transactions_.clear();
    lock_.unlock(txn_c_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_a_));

    granted_transactions_.clear();
    lock_.unlock(txn_a_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_b_));

    granted_transactions_.clear();
    lock_.unlock(txn_b_, &granted_transactions_);
    EXPECT_EQ(0, granted_transactions_.size());
}

TEST_F(LockTest, ReadWriteQueuing) {
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_FALSE(lock_.tryWriteLock(txn_b_));
    // b is queued for writing: queue read requests
    EXPECT_FALSE(lock_.tryReadLock(txn_c_));
    LockTransaction txn_d_(NULL, "foo", false, true);
    EXPECT_FALSE(lock_.tryReadLock(&txn_d_));
    
    // cancel b's request: c and d should get the lock
    lock_.cancelRequest(txn_b_, &granted_transactions_);
    EXPECT_EQ(2, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_c_));
    EXPECT_TRUE(contains(granted_transactions_, &txn_d_));
    EXPECT_TRUE(lock_.tryReadLock(txn_c_));
    EXPECT_TRUE(lock_.tryReadLock(&txn_d_));
}

TEST_F(LockTest, ReadWriteUpgradeNoContention) {
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));

    // Not permitted: a holds lock for writing
    EXPECT_FALSE(lock_.tryReadLock(txn_b_));

    lock_.unlock(txn_a_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_b_));
    EXPECT_TRUE(lock_.tryReadLock(txn_b_));
}

TEST_F(LockTest, ReadWriteUpgradeContention) {
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_FALSE(lock_.tryWriteLock(txn_b_));
    // upgrade permitted (higher priority)
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
}

TEST_F(LockTest, ReadWriteUpgradePriority) {
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_b_));
    EXPECT_FALSE(lock_.tryWriteLock(txn_c_));
    // upgrade queued at higher priority
    EXPECT_FALSE(lock_.tryWriteLock(txn_a_));

    // b unlocks, the upgrade is granted
    lock_.unlock(txn_b_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_a_));
    granted_transactions_.clear();

    // a unlocks: c gets it as usual
    lock_.unlock(txn_a_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_c_));
}

TEST_F(LockTest, ReadWriteUpgradeFailure) {
    EXPECT_TRUE(lock_.tryReadLock(txn_a_));
    EXPECT_TRUE(lock_.tryReadLock(txn_b_));

    // Both upgrades are queued
    EXPECT_FALSE(lock_.tryWriteLock(txn_a_));
    EXPECT_FALSE(lock_.tryWriteLock(txn_b_));

    // b unlocks, a should get the lock
    lock_.cancelRequest(txn_b_, &granted_transactions_);
    EXPECT_EQ(0, granted_transactions_.size());
    lock_.unlock(txn_b_, &granted_transactions_);
    EXPECT_EQ(1, granted_transactions_.size());
    EXPECT_TRUE(contains(granted_transactions_, txn_a_));
    EXPECT_TRUE(lock_.tryWriteLock(txn_a_));
}

// Random testing of the lock: randomly lock and unlock the lock from multiple transactions.
// Ensure that the requirements are never violated:
// * only one writer, multiple readers
// * queued transactions only if someone is holding the lock
// This is ugly but easy to write, and gives me good confidence in the lock code
TEST(LockValidator, Random) {
    Lock lock;

    srand(static_cast<unsigned int>(time(NULL)));
    static const int NUM_TRANSACTIONS = 10;
    static const int NUM_ITERATIONS = 500000;
    LockTransaction* transactions[NUM_TRANSACTIONS];
    for (size_t i = 0; i < base::arraySize(transactions); ++i) {
        transactions[i] = new LockTransaction(NULL, "foo", false, true);
    }

    // states: (unlocked, read, write) (read request, write request)
    static const int STATE_UNLOCKED = 0;
    static const int STATE_READ = 1;
    static const int STATE_WRITE = 1 << 1;
    static const int LOCK_MASK = STATE_READ | STATE_WRITE;

    static const int STATE_READ_REQUEST = 1 << 2;
    static const int STATE_WRITE_REQUEST = 1 << 3;
    static const int REQUEST_MASK = STATE_READ_REQUEST | STATE_WRITE_REQUEST;

    // This is a C++-isim for initializing the array to zero
    int txn_states[NUM_TRANSACTIONS] = {};

    for (int iteration = 0; iteration < NUM_ITERATIONS; ++iteration) {
        // Randomly pick a transaction
        int txn = rand() % NUM_TRANSACTIONS;

        // Randomly pick an action
        // possible actions:
        // a) request read lock (only if not requesting a lock)
        // b) request write lock (only if not requesting a lock)
        // c) unlock and/or cancel (only if STATE_LOCKED || STATE_REQUESTED)
        int available_actions = 0;
        if ((txn_states[txn] & REQUEST_MASK) == 0) {
            available_actions += 2;
        }
        if (txn_states[txn] != 0) {
            available_actions += 1;
        }

        int action = rand() % available_actions;
        if (txn_states[txn] & REQUEST_MASK) {
            action += 2;
        }

        if (action == 0) {
            // read!
            assert((txn_states[txn] & REQUEST_MASK) == 0);
            bool result = lock.tryReadLock(transactions[txn]);
            if (result) {
                // we have the read lock
                if (txn_states[txn] == STATE_UNLOCKED) {
                    txn_states[txn] = STATE_READ;
                }
            } else {
                // we are queued for a lock
                assert(txn_states[txn] == 0);
                txn_states[txn] = STATE_READ_REQUEST;
            }
        } else if (action == 1) {
            // write!
            assert((txn_states[txn] & REQUEST_MASK) == 0);
            bool result = lock.tryWriteLock(transactions[txn]);
            if (result) {
                // we have the write lock
                txn_states[txn] = STATE_WRITE;
            } else {
                // we are queued for a write lock
                assert(txn_states[txn] == 0 || txn_states[txn] == STATE_READ);
                txn_states[txn] |= STATE_WRITE_REQUEST;
            }
        } else {
            assert(action == 2);
            vector<LockTransaction*> granted;
            if (txn_states[txn] & REQUEST_MASK) {
                lock.cancelRequest(transactions[txn], &granted);
            }
            if (txn_states[txn] & LOCK_MASK) {
                lock.unlock(transactions[txn], &granted);
            }
            txn_states[txn] = 0;
            for (int j = 0; j < granted.size(); ++j) {
                LockTransaction** it =
                        std::find(transactions, transactions + base::arraySize(transactions), granted[j]);
                ptrdiff_t index = it - transactions;
                assert(txn_states[index] & REQUEST_MASK);
                if (txn_states[index] & STATE_READ_REQUEST) {
                    assert((txn_states[index] & LOCK_MASK) == 0);
                    txn_states[index] = STATE_READ;
                } else {
                    assert(txn_states[index] & STATE_WRITE_REQUEST);
                    txn_states[index] = STATE_WRITE;
                }
            }
        }

        // Validate the state
        int lock_count = 0;
        int request_count = 0;
        bool write = false;
        int total = 0;
        for (int j = 0; j < NUM_TRANSACTIONS; ++j) {
            if (txn_states[j] & LOCK_MASK) {
                lock_count += 1;
                if ((txn_states[j] & LOCK_MASK) == STATE_WRITE) {
                    // only one writer permitted
                    assert(!write);
                    write = true;
                }
            }
            if (txn_states[j] & REQUEST_MASK) {
                request_count += 1;
            }
            if (txn_states[j]) {
                total += 1;
            }
        }
        // more than one lock holder = shared lock
        if (lock_count > 1) {
            assert(!write);
        }
        // any requests pending means there is a lock holder
        if (request_count > 0) {
            assert(lock_count > 0);
        }
        if (lock_count == 1 && request_count == 1) {
            // one lock count and request count: not the same transaction!
            assert(total > 1);
        }
    }

    for (size_t i = 0; i < base::arraySize(transactions); ++i) {
        delete transactions[i];
    }
}

}  // namespace dtxn

int main() {
    return TestSuite::globalInstance()->runAll();
}
