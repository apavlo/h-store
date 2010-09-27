// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/locks/lock.h"

#include <cstdlib>  // for abort()

#include "base/assert.h"

using std::vector;

namespace dtxn {

Lock::Lock() : state_(UNLOCKED) {}

void Lock::unlock(LockTransaction* transaction, vector<LockTransaction*>* granted_transactions) {
    assert(state_ != UNLOCKED);
    assert(granted_transactions != NULL);
    size_t result = holders_.erase(transaction);
    ASSERT(result == 1);
    
    if (holders_.size() == 0) {
        state_ = UNLOCKED;
    }
    
    // TODO: Only consider the request queue in certain cases
    // If the lock was unlocked, or we are down to one reader, we may be able to permit requests
    //~ if (state_ == UNLOCKED || state_ == LOCKED_READ && holders_.size() == 1) {
    // Admit as many lock requests as possible
    admitQueuedRequests(granted_transactions);
}

void Lock::cancelRequest(LockTransaction* transaction, vector<LockTransaction*>* granted_transactions) {
    // Look for the transaction
    for (int i = 0; i < request_queue_.size(); ++i) {
        if (request_queue_.at(i).first == transaction) {
            request_queue_.erase(i);
            if (i == 0) {
                // the request was at the head of the queue: try to admit lock requests
                admitQueuedRequests(granted_transactions);
            }
            return;
        }
    }
    // This should never happen: if it does, we have a bug
    assert(false);
    abort();
}

bool Lock::tryOrQueueRequest(LockTransaction* transaction, State lock_request) {
    if (tryRequest(transaction, lock_request)) {
        return true;
    } else {
        // Queue the request if it is not already in the queue
        bool found = false;
        for (int i = 0; i < request_queue_.size(); ++i) {
            if (request_queue_.at(i).first == transaction) {
                assert(request_queue_.at(i).second == lock_request);
                found = true;
                break;
            }
        }

        if (!found) {
            if (lock_request == LOCKED_WRITE && state_ == LOCKED_READ &&
                    holders_.find(transaction) != holders_.end()) {
                // this is a lock upgrade: prioritize it
                // NOTE: This works even for multiple lock upgrades, because multiple lock upgrades
                // cause a deadlock so all but one will need to be killed anyway.
                request_queue_.push_front(std::make_pair(transaction, lock_request));
            } else {
                request_queue_.push_back(std::make_pair(transaction, lock_request));
            }
        }
        return false;
    }
}

bool Lock::tryRequest(LockTransaction* transaction, State lock_request) {
    if (lock_request == LOCKED_READ) {
        // Acquire lock when:
        // * The lock is unlocked
        // * We already hold the lock (in either state)
        // * The lock is locked for reading and either the queue is empty OR we are admitting
        //    queued requests
        if (state_ == UNLOCKED
                || holders_.count(transaction) == 1
                || (state_ == LOCKED_READ && (request_queue_.empty()
                        || request_queue_.front().second == LOCKED_READ))) {
            // Add transaction to set of lock holders: if locked more than once it doesn't matter.
            if (state_ == UNLOCKED) {
                state_ = LOCKED_READ;
            }
            holders_.insert(transaction);
            return true;
        } else {
            return false;
        }
    } else {
        assert(lock_request == LOCKED_WRITE);
        // Acquire the lock in one of three cases:
        // * The lock is unlocked
        // * The lock is held for writing by us already
        // * The lock is held for reading by us ONLY -> upgrade to write lock
        // This means that we either need:
        // * The lock is unlocked
        //    OR
        // * The lock is held by only us in either state.
        if (state_ == UNLOCKED ||
                (holders_.size() == 1 && holders_.count(transaction) == 1)) {
            // Add transaction to set of lock holders: if locked more than once it doesn't matter.
            if (state_ == UNLOCKED) {
                assert(holders_.empty());
                holders_.insert(transaction);
            }
            state_ = LOCKED_WRITE;
            assert(holders_.size() == 1);
            return true;
        } else {
            return false;
        }
    }
}

void Lock::admitQueuedRequests(vector<LockTransaction*>* granted_transactions) {
    while (!request_queue_.empty() && tryRequest(request_queue_.front().first, request_queue_.front().second)) {
        granted_transactions->push_back(request_queue_.front().first);
        request_queue_.pop_front();
    }
}

}  // namespace dtxn
