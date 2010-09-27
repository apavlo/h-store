// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_LOCKS_LOCK_H__
#define DTXN_LOCKS_LOCK_H__

#include <vector>

#include "base/circularbuffer.h"
#include "base/unordered_set.h"

namespace dtxn {

class LockTransaction;

/** Implements a read/write lock. */
class Lock {
public:
    Lock();

    /** Returns true if transaction acquires the lock for reading. If it is unavailable, the
    request will be queued. NOTE: The lock can be acquired more than once, but it needs to be
    released exactly once. */
    bool tryReadLock(LockTransaction* transaction) {
        return tryOrQueueRequest(transaction, LOCKED_READ);
    }

    bool tryWriteLock(LockTransaction* transaction) {
        return tryOrQueueRequest(transaction, LOCKED_WRITE);
    }

    /** Releases the lock for transaction. Appends transactions that get the lock to granted
    transactions. */
    void unlock(LockTransaction* transaction,
            std::vector<LockTransaction*>* granted_transactions);

    /** Cancels a queued lock request. */
    void cancelRequest(LockTransaction* transaction,
            std::vector<LockTransaction*>* granted_transactions);

    typedef base::unordered_set<LockTransaction*> TransactionSet;

    /** Returns the transactions holding this lock. */
    const TransactionSet& holders() const {
        return holders_;
    }

    enum State {
        UNLOCKED,
        LOCKED_READ,
        LOCKED_WRITE,
    };
    State state() const { return state_; }

private:

    /** Returns true if transaction acquires the lock in state lock_request. Queues the transaction
    if it is not acquired. */
    bool tryOrQueueRequest(LockTransaction* transaction, State lock_request);

    /** Returns true if transaction acquires the lock in state lock_request. Does not queue
    transaction if it is not acquired. */
    bool tryRequest(LockTransaction* transaction, State lock_request);

    /** Tries to admit as many queued requests as possible. */
    void admitQueuedRequests(std::vector<LockTransaction*>* granted_transactions);

    State state_;
    TransactionSet holders_;

    CircularBuffer<std::pair<LockTransaction*, State> > request_queue_;
};

}  // namespace dtxn

#endif
