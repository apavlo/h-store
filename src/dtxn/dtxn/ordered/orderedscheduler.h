// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_ORDEREDSCHEDULER_H__
#define DTXN_ORDEREDSCHEDULER_H__

#include "base/circularbuffer.h"
#include "dtxn/scheduler.h"

namespace dtxn {

class ExecutionEngine;

class OrderedScheduler : public Scheduler {
public:
    enum ConcurrencyControl {
        CC_BLOCK,
        CC_SPECULATIVE,
    };

    // Does not own engine.
    OrderedScheduler(ExecutionEngine* engine) :
            engine_(engine), cc_mode_(CC_BLOCK), batch_mps_(true), last_batch_id_(-1) {
        assert(engine_ != NULL);
    }

    // Set the concurrency control mode.
    void cc_mode(ConcurrencyControl mode);

    // Set the batching mode.
    void set_batching(bool enabled) { batch_mps_ = enabled; }

    virtual void fragmentArrived(FragmentState* transaction);
    virtual void order(std::vector<FragmentState*>* queue);
    virtual void execute(FragmentState* transaction);
    virtual void commit(TransactionState* transaction);
    virtual void abort(TransactionState* transaction);
    virtual FragmentState* executed();
    virtual bool idle();

    /** The number of single partition transactions that will be executed
    before returning and letting libevent process more network messages. */
    // NOTE: On gigabit ethernet, the round trip time between sending the result of a one shot TXN
    // and receiving the decision was 130/132 us for 2 participants.
    // The htableclientperf transactions execute in ~60 us. Therefore, 2 or 3 transactions is
    // probably close to the right value. In my experiments, 3 transactions performs slightly
    // better than 2 or 4, but this probably needs auto-tuning for different workloads.
    static const int MAX_TXNS_PER_LOOP = 3;

private:
    // Tries to execute as many items from the blocked queue as possible.
    void executeQueue();

    // Actually executes transaction. Returns true if it was executed. If it returns false, the
    // transaction should be placed on the blocked queue.
    bool realExecute(FragmentState* transaction);

    ExecutionEngine* engine_;
    ConcurrencyControl cc_mode_;

    typedef CircularBuffer<FragmentState*> QueueType;

    // Contains single partition transactions waiting to be executed.
    QueueType single_partition_queue_;

    // Contains multipartition transactions waiting to be executed.
    QueueType multi_partition_queue_;

    // Contains transactions to be executed
    QueueType blocked_queue_;

    // Contains transactions with results can be released to the world via getWorkUnitResult.
    // TODO: Each scheduler ends up implementing queue management crap. Better system?
    QueueType result_queue_;

    // Contains transactions that are pending decision. The head is *actually* being executed.
    // The others are speculative.
    CircularBuffer<TransactionState*> unfinished_queue_;

    // If true, the scheduler will "batch" multi-partition transactions from the same client
    bool batch_mps_;

    // The transaction id of the last transaction in the dependency chain
    int last_batch_id_;
};

}  // namespace dtxn

#endif
