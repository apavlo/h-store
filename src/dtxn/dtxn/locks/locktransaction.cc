// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/locks/locktransaction.h"

#include <cassert>

#include "dtxn/locks/lock.h"

using std::string;
using std::vector;

namespace dtxn {

LockTransaction::~LockTransaction() {
    assert(blocked_lock_ == NULL);
    assert(acquired_locks_.empty());
}

bool LockTransaction::tryReadLock(Lock* lock) {
	// TODO: Combine tryReadLock and tryWriteLock?
    assert(!blocked());
    bool result = lock->tryReadLock(this);
    if (result) {
        acquired_locks_.insert(lock);
    } else {
        blocked_lock_ = lock;
    }
    return result;
}

bool LockTransaction::tryWriteLock(Lock* lock) {
    assert(!blocked());
    bool result = lock->tryWriteLock(this);
    if (result) {
        acquired_locks_.insert(lock);
    } else {
        blocked_lock_ = lock;
    }
    return result;
}

void LockTransaction::lockGranted() {
    assert(blocked());
    acquired_locks_.insert(blocked_lock_);
    blocked_lock_ = NULL;
}

void LockTransaction::dropLocks(vector<LockTransaction*>* granted_transactions) {
    if (blocked_lock_) {
        blocked_lock_->cancelRequest(this, granted_transactions);
        blocked_lock_ = NULL;
    }

    for (LockSet::iterator i = acquired_locks_.begin(); i != acquired_locks_.end(); ++i) {
        (*i)->unlock(this, granted_transactions);
    }
    acquired_locks_.clear();
}

}
