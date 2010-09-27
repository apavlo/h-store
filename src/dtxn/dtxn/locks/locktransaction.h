// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_LOCKS_TRANSACTIONSTATE_H__
#define DTXN_LOCKS_TRANSACTIONSTATE_H__

#include <tr1/unordered_set>
#include <vector>

#include "dtxn/transaction.h"

namespace dtxn {

class Lock;

class LockTransaction : public Transaction {
public:
    LockTransaction(void* external_state, const std::string& initial_work_unit, bool last_fragment,
            bool multiple_partitions) :
            Transaction(external_state, initial_work_unit, last_fragment, multiple_partitions),
            blocked_lock_(NULL) {}

    virtual ~LockTransaction();

    virtual bool tryReadLock(Lock* lock);
    virtual bool tryWriteLock(Lock* lock);

    /** Call when the lock that this transaction was blocked on has been granted. */
    void lockGranted();

    /** Releases all locks held by this transaction (included queued locks). Fills
    granted_transactions with all transactions that have since acquired locks. */
    // TODO: What about the waits-for graph and deadlock detection?
    void dropLocks(std::vector<LockTransaction*>* granted_transactions);

    bool blocked() const { return blocked_lock_ != NULL; }
    Lock* blocked_lock() { return blocked_lock_; }
    bool has_locks() const { return !acquired_locks_.empty() || blocked_lock_ != NULL; }

private:
    typedef std::tr1::unordered_set<Lock*> LockSet;
    LockSet acquired_locks_;

    // If not NULL, we are waiting for this lock
    Lock* blocked_lock_;
};

}  // namespace dtxn

#endif
