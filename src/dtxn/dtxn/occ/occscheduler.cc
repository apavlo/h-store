// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/occ/occscheduler.h"

#include "base/assert.h"
#include "base/stlutil.h"
#include "dtxn/occ/occtransaction.h"

using std::string;

namespace dtxn {

OCCScheduler::~OCCScheduler() {
    STLDeleteElements(&independent_queue_);
    STLDeleteElements(&speculative_queue_);
    // No need to STLDeleteElements: These are included in the conflict/no conflict sets
    result_queue_.clear();
}

Transaction* OCCScheduler::begin(void* external_state, const std::string& initial_work_unit,
        bool last_fragment, bool multiple_partitions) {
    OCCTransaction* state = new OCCTransaction(
            external_state, initial_work_unit, last_fragment, multiple_partitions);

    execute_queue_.push_back(state);
    return state;
}

void OCCScheduler::workUnitAdded(Transaction* transaction) {
    assert(transaction->multiple_partitions());
    assert(transaction->last_status() == ExecutionEngine::INVALID);
    assert(false);

    // record this work unit's read/write set in a new set
    // execute the work unit
    // it must be possible to move this work unit to the same place as the transaction
    // if not, we must abort everything that is in the way and re-execute.

    // TODO: This is not supported mostly because if we screw up, undoing and re-executing
    // sucks, and we probably want to be able to undo just one particular work unit.
    ::abort();
}

void OCCScheduler::commit(Transaction* transaction) {
    assert(transaction->last_status() == ExecutionEngine::OK);

    if (!transaction->multiple_partitions()) {
        // nothing to do on commit of single partition transactions except delete
        delete transaction;
        return;
    }

    // This transaction must be in the independent set
    removeFromIndependentQueue((OCCTransaction*) transaction);

    // Free the undo buffer and the transaction state
    if (*transaction->undo() != NULL) {
        // PAVLO
        engine_->freeUndo(*transaction->undo(), std::string("OCCScheduler::commit-1"));
    }
    delete transaction;
    transaction = NULL;

    // Take the first transaction in the conflict queue and try to add it to the no conflict set
    // TODO: We could try other transactions, assuming they can be reordered with each other
    // as well. Thus, this is pessimistic but simple
    while (!speculative_queue_.empty()) {
        OCCTransaction* speculative = speculative_queue_.front();
        if (!isIndependent(speculative)) {
            break;
        }
        speculative_queue_.pop_front();
        if (speculative->multiple_partitions()) {
            independent_queue_.push_back(speculative);
        } else if (*speculative->undo() != NULL) {
            if (speculative->last_status() == ExecutionEngine::ABORT_USER) {
                engine_->applyUndo(*speculative->undo(), std::string("OCCScheduler::commit-2"));
            } else {
                assert(speculative->last_status() == ExecutionEngine::OK);
                // PAVLO
                engine_->freeUndo(*speculative->undo(), std::string("OCCScheduler::commit-3"));
            }
            *speculative->undo() = NULL;
        }
        result_queue_.push_back(speculative);
    }
}

void OCCScheduler::abort(Transaction* txn) {
    if (!txn->multiple_partitions()) {
        // nothing to do on commit of single partition transactions except delete
        delete txn;
        return;
    }

    OCCTransaction* transaction = (OCCTransaction*) txn;
    // remove the transaction from the independent set
    removeFromIndependentQueue(transaction);

    // Cascade the abort
    // TODO: For now, we re-execute *all* transactions in the speculative queue. This is
    // pessimistic but simple. We should only kill the actual dependent transactions, but
    // this is tough because it must be done in reverse order.
    while (!speculative_queue_.empty()) {
        OCCTransaction* speculative = speculative_queue_.back();
        speculative_queue_.pop_back();
        if (*speculative->undo() != NULL) {
            engine_->applyUndo(*speculative->undo(), std::string("OCCScheduler::abort-1"));
        }
        speculative->prepareReExecute();
        execute_queue_.push_front(speculative);
    }

    // Look for the transaction in the output queue: can happen with unusual abort timing
    // Add the following check before the loop? if (!transaction->results_sent()) {}
    result_queue_.eraseValue(transaction);

    // abort the actual transaction
    if (*transaction->undo() != NULL) {
        engine_->applyUndo(*transaction->undo(), std::string("OCCScheduler::abort-2"));
    }
    delete transaction;
}
 
 Transaction* OCCScheduler::getWorkUnitResult() {
    if (result_queue_.empty()) {
        return NULL;
    }
    return result_queue_.dequeue();
}

bool OCCScheduler::idle() {
    int count = 0;
    while (!execute_queue_.empty() && count < 5) {
        executeWorkUnit(execute_queue_.dequeue());
        count += 1;
    }

    return !execute_queue_.empty();
}

void OCCScheduler::backupApplyTransaction(const std::vector<std::string>& work_units) {
    // Apply each fragment without undo or locks
    string out;
    for (int i = 0; i < work_units.size(); ++i) {
        out.clear();
        ExecutionEngine::Status status = engine_->tryExecute(work_units[i], &out, NULL, NULL, NULL);
        CHECK(status == ExecutionEngine::OK);
    }
}

bool OCCScheduler::isIndependent(OCCTransaction* transaction) const {
    for (size_t i = 0; i < independent_queue_.size(); ++i) {
        // TODO: This definition of "dependency" may violate the "causality" rule. That is, it is
        // possible to observe commits in an order that is different from the sequential order:
        // issue two transactions, t1 reads x, t2 writes x. The client can observe t2 commit first,
        // even though it executed *after* t1. Given all other possible sources of re-ordering in
        // a distributed system, I don't think this is worth worrying about.
        // TODO: For locking, the same thing is dropping read locks on prepare. We should do that.
        if (transaction->dependsOn(*independent_queue_.at(i))) {
            return false;
        }
    }
    return true;
}

bool OCCScheduler::safeToCommitBefore(OCCTransaction* transaction) const {
    for (size_t i = 0; i < speculative_queue_.size(); ++i) {
        if (!transaction->canReorder(*speculative_queue_.at(i))) {
            return false;
        }
    }
    return true;
}

void OCCScheduler::removeFromIndependentQueue(OCCTransaction* transaction) {
    bool found = false;
    found = independent_queue_.eraseValue(transaction);
    // TODO: We don't support timeout aborts, so this MUST crash if we get here
    CHECK(found);
}

void OCCScheduler::executeWorkUnit(OCCTransaction* transaction) {
    // Execute the transaction to collect the read/write set
    ExecutionEngine::Status status = engine_->tryExecute(
            transaction->last_work_unit(), transaction->output(), transaction->undo(), transaction, NULL);
    assert(status == ExecutionEngine::OK || status == ExecutionEngine::ABORT_USER);
    transaction->setWorkUnitStatus(status);

    // If this transaction does not conflict with anything in the speculative queue, and does not
    // depend on any "prepared" transactions, we can expose the results
    if (!safeToCommitBefore(transaction) || !isIndependent(transaction)) {
        // TODO: We could do something more "fine grained" like inserting it in the queue
        // behind the conflicting transaction, or just putting safeToCommit transaction in front?
        speculative_queue_.push_back(transaction);
        return;
    }

    // no conflicts: safe to add to the no conflict set and expose to the world
    // TODO: Auto-commit single partition transactions?
    if (transaction->multiple_partitions()) {
        independent_queue_.push_back(transaction);
    } else if (*transaction->undo() != NULL) {
        if (transaction->last_status() == ExecutionEngine::ABORT_USER) {
            engine_->applyUndo(*transaction->undo(), std::string("OCCScheduler::executeWorkUnit-1"));
        } else {
            assert(transaction->last_status() == ExecutionEngine::OK);
            // PAVLO
            engine_->freeUndo(*transaction->undo(), std::string("OCCScheduler::executeWorkUnit-2"));
        }
        *transaction->undo() = NULL;
    }
    result_queue_.push_back(transaction);
}

}  // namespace dtxn
