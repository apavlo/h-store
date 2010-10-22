// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN2_BLOCKINGSCHEDULER_H__
#define DTXN2_BLOCKINGSCHEDULER_H__

#include "base/circularbuffer.h"
#include "dtxn2/scheduler.h"

namespace dtxn {
class ExecutionEngine;
}

namespace dtxn2 {

class BlockingScheduler : public Scheduler {
public:
    BlockingScheduler(dtxn::ExecutionEngine* engine) :
            engine_(engine), current_(NULL) {}
    virtual ~BlockingScheduler();

    virtual void fragmentArrived(TransactionState* transaction);
    virtual void decide(TransactionState* transaction, bool commit, const std::string& payload);
    virtual bool doWork(SchedulerOutput* output);

private:
    dtxn::ExecutionEngine* engine_;

    CircularBuffer<TransactionState*> unreplicated_queue_;
    CircularBuffer<TransactionState*> execute_queue_;

    TransactionState* current_;
};

}  // namespace dtxn2

#endif
