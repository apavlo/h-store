// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN2_MOCKSCHEDULER_H__
#define DTXN2_MOCKSCHEDULER_H__

#include "dtxn2/scheduler.h"
#include "dtxn2/transactionstate.h"

namespace dtxn2 {

class MockTransactionState : public TransactionState {
public:
    void addFragment(const dtxn::Fragment& fragment) {
        TransactionState::addFragment(fragment);
    }
};

class MockSchedulerOutput : public SchedulerOutput {
public:
    MockSchedulerOutput() : replicate_count_(0) {
        reset();
    }

    virtual void replicate(TransactionState* transaction) {
        last_replicate_ = transaction;
        replicate_count_ += 1;
    }

    virtual void executed(FragmentState* fragment) {
        last_executed_ = fragment;
    }

    void reset() {
        last_replicate_ = NULL;
        last_executed_ = NULL;
    }

    TransactionState* last_replicate_;
    FragmentState* last_executed_;

    int replicate_count_;
};

}  // namespace dtxn2

#endif
