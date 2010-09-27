// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_LOCKS_LOCKSCHEDULER_H__
#define DTXN_LOCKS_LOCKSCHEDULER_H__

#include <vector>

#include "base/circularbuffer.h"
#include "base/unordered_set.h"
#include "dtxn/transactionscheduler.h"

namespace dtxn {

class ExecutionEngine;
class Lock;
class Transaction;
class LockTransaction;

class LockScheduler : public TransactionScheduler {
public:
    /** Does not own engine. */
    LockScheduler(ExecutionEngine* engine);

    virtual ~LockScheduler();

    virtual Transaction* begin(void* external_state, const std::string& initial_work_unit,
            bool last_fragment, bool multiple_partitions);
    virtual void workUnitAdded(Transaction* transaction);
    virtual void commit(Transaction* transaction);
    virtual void abort(Transaction* transaction);
    virtual Transaction* getWorkUnitResult();
    virtual bool idle();
    virtual void backupApplyTransaction(const std::vector<std::string>& work_units);

    /** Returns transactions that form a lock cycle detection. If there is no cycle, the result
    is an empty vector. */
    std::vector<LockTransaction*> detectDeadlock() const;

private:
    typedef base::unordered_set<LockTransaction*> BlockedSet;

    bool depthFirstSearch(BlockedSet* visited, LockTransaction* start,
            std::vector<LockTransaction*>* cycle) const;
    void finishTransaction(LockTransaction* transaction);
    void executeWork(LockTransaction* transaction);

    ExecutionEngine* engine_;

    CircularBuffer<Transaction*> queued_results_;
    BlockedSet blocked_transactions_;

    int active_count_;

    // Number of blocked transactions at the last call to idle(). Used for deadlock detection.
    int last_blocked_;

    CircularBuffer<LockTransaction*> execute_queue_;
};

}  // namespace dtxn

#endif
