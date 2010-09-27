// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_OCCSCHEDULER_H__
#define DTXN_OCCSCHEDULER_H__

#include "base/circularbuffer.h"
#include "dtxn/transactionscheduler.h"

namespace dtxn {

class ExecutionEngine;
class OCCTransaction;

class OCCScheduler : public TransactionScheduler {
public:
    // Does not own engine.
    OCCScheduler(ExecutionEngine* engine) : engine_(engine) {
        assert(engine_ != NULL);
    }
    virtual ~OCCScheduler();

    virtual Transaction* begin(void* external_state, const std::string& initial_work_unit,
            bool last_fragment, bool multiple_partitions);
    virtual void workUnitAdded(Transaction* transaction);
    virtual void commit(Transaction* transaction);
    virtual void abort(Transaction* transaction);
    virtual Transaction* getWorkUnitResult();
    virtual bool idle();
    virtual void backupApplyTransaction(const std::vector<std::string>& work_units);

private:
    ExecutionEngine* engine_;

    // Returns true if transaction is independent of all transactions in independent_queue_.
    bool isIndependent(OCCTransaction* transaction) const;

    // Returns true if transaction can be committed before transactions in speculative_queue_.
    bool safeToCommitBefore(OCCTransaction* transaction) const;

    void removeFromIndependentQueue(OCCTransaction* transaction);
    void executeWorkUnit(OCCTransaction* transaction);

    typedef CircularBuffer<OCCTransaction*> QueueType;
    // Queue of transactions that do not depend on each other: can all be committed/aborted
    QueueType independent_queue_;

    // Queue of transactions that depend on previous transactions in some way, and thus cannot be
    // committed until the previous transaction finishes.
    QueueType speculative_queue_;

    // Contains transactions with results can be released to the world via getWorkUnitResult.
    QueueType result_queue_;

    QueueType execute_queue_;
};

}  // namespace dtxn

#endif
