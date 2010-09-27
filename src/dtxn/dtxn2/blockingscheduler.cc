// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn2/blockingscheduler.h"

#include "base/stlutil.h"
#include "dtxn/executionengine.h"
#include "dtxn2/transactionstate.h"

using dtxn::ExecutionEngine;
using dtxn::Fragment;
using dtxn::FragmentResponse;

namespace dtxn2 {

BlockingScheduler::~BlockingScheduler() {}

void BlockingScheduler::fragmentArrived(TransactionState* transaction) {
    assert(transaction->last_fragment().response().status == ExecutionEngine::INVALID);
    if (!execute_queue_.empty() && transaction == execute_queue_.front()) {
        // nothing to do here
    } else {
        assert(!base::contains(unreplicated_queue_, transaction));
        assert(!base::contains(execute_queue_, transaction));
        unreplicated_queue_.push_back(transaction);
        execute_queue_.push_back(transaction);
    }
}

void BlockingScheduler::decide(TransactionState* transaction, bool commit) {
    assert(transaction == execute_queue_.front());
    // TODO: Support aborting unprepared transactions
    assert(!commit || transaction->last_fragment().request().last_fragment);
    assert(!commit || transaction->last_fragment().response().status == ExecutionEngine::OK);
    // Free the undo buffer
    void* undo = transaction->scheduler_state();
    if (undo != NULL) {
        if (commit) {
            engine_->freeUndo(undo);
        } else {
            engine_->applyUndo(undo);
        }
    }
    transaction->scheduler_state(NULL);

    if (!transaction->last_fragment().request().last_fragment) {
        assert(unreplicated_queue_.front() == transaction);
        unreplicated_queue_.pop_front();
    } else {
        assert(!base::contains(unreplicated_queue_, transaction));
    }
    execute_queue_.pop_front();
}

bool BlockingScheduler::doWork(SchedulerOutput* output) {
    // Replicate all "finished" transactions
    while (!unreplicated_queue_.empty() &&
            unreplicated_queue_.front()->last_fragment().request().last_fragment) {
        TransactionState* transaction = unreplicated_queue_.dequeue();
        // this must be a multi-round transaction that just finished OR
        assert(execute_queue_.front() == transaction ||
            // This transaction should not have been executed yet
            (base::contains(execute_queue_, transaction) &&
            transaction->last_fragment().response().status == ExecutionEngine::INVALID));
        output->replicate(transaction);
    }

    // execute until we reach an executed multi-partition transaction
    while (!execute_queue_.empty() &&
            execute_queue_.front()->last_fragment().response().status == ExecutionEngine::INVALID) {
        TransactionState* transaction = execute_queue_.front();
        FragmentState* fragment = transaction->mutable_last();
        FragmentResponse* response = fragment->mutable_response();

        void** undo_pointer = NULL;
        void* undo = transaction->scheduler_state();
        if (undo != NULL || fragment->request().multiple_partitions) {
            undo_pointer = &undo;
        } else {
            assert(undo == NULL && !fragment->request().multiple_partitions);
        }

        if (fragment->request().last_fragment && fragment->request().transaction.empty()) {
            // This is a "prepare" for an unfinished transaction: do not actually execute anything
            response->status = ExecutionEngine::OK;
        } else {
            assert(!fragment->request().transaction.empty());
            assert(response->status == ExecutionEngine::INVALID);
            response->status = engine_->tryExecute(
                    fragment->request().transaction, &response->result, undo_pointer, NULL);
            assert(response->status == ExecutionEngine::OK ||
                    response->status == ExecutionEngine::ABORT_USER);
            assert(undo == NULL || undo_pointer != NULL);
            transaction->scheduler_state(undo);
        }

        if (!fragment->request().multiple_partitions) {
            // finishing single partition transaction: clean up any undo buffer. This is possible
            // if this was speculative or downgraded from a multi-partition txn.
            BlockingScheduler::decide(transaction, response->status == ExecutionEngine::OK);
        }

        // Return results at the end to avoid touching deleted memory
        output->executed(fragment);
    }
    assert(execute_queue_.empty() ||
            execute_queue_.front()->last_fragment().request().multiple_partitions);

    return false;
}

}  // namespace dtxn2
