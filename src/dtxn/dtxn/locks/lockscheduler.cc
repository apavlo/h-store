// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/locks/lockscheduler.h"

#include <algorithm>

#include "base/assert.h"
#include "base/cast.h"
#include "base/stlutil.h"
#include "dtxn/executionengine.h"
#include "dtxn/locks/lock.h"
#include "dtxn/locks/locktransaction.h"

using std::pair;
using std::string;
using std::vector;

namespace dtxn {

LockScheduler::LockScheduler(ExecutionEngine* engine) : engine_(engine), active_count_(0),
        last_blocked_(0) {
    assert(engine_ != NULL);
    assert(engine_->supports_locks());
}

LockScheduler::~LockScheduler() {
    while (!queued_results_.empty()) {
        delete queued_results_.front();
        queued_results_.pop_front();
    }
}

Transaction* LockScheduler::begin(void* external_state, const std::string& initial_work_unit,
        bool last_fragment, bool multiple_partitions) {
    LockTransaction* transaction = new LockTransaction(
            external_state, initial_work_unit, last_fragment, multiple_partitions);
    execute_queue_.push_back(transaction);
    return transaction;
}

void LockScheduler::workUnitAdded(Transaction* txn) {
    execute_queue_.push_back(reinterpret_cast<LockTransaction*>(txn));
}

void LockScheduler::executeWork(LockTransaction* transaction) {
    // Single partition transaction with nothing active: no locks or undo
    ExecutionEngine::Status status;
    if (!transaction->multiple_partitions() && active_count_ == 0) {
        // TODO: Single partition transactions might still need undo?
        status = engine_->tryExecute(transaction->last_work_unit(), transaction->output(), NULL, NULL, NULL);
        assert(status != ExecutionEngine::BLOCKED);
    } else {
        status = engine_->tryExecute(
                transaction->last_work_unit(), transaction->output(), transaction->undo(), transaction, NULL);
        if (transaction->last_status() == ExecutionEngine::INVALID) {
            // Transaction is starting: it should have acquired SOME locks, or else it did not
            // access the database.
            // TODO: if there is true read-only data that only is updated while taking a
            // partition-wide lock, this might not be valid. But we don't have a partition-wide
            // lock yet.
            assert(transaction->has_locks());
            active_count_ += 1;
        }
    }
    transaction->setWorkUnitStatus(status);
    if (status == ExecutionEngine::BLOCKED) {
        assert(transaction->blocked());
    }

    if (active_count_ > 0 && !transaction->multiple_partitions() && status != ExecutionEngine::BLOCKED) {
        // This is a single partition transaction that is not blocked. Drop its locks
        // immediately to avoid unneeded contention. finishTransaction does extra unneeded
        // stuff, but is at least safe.
        // TODO: We can't abort a commit single partition transaction any more. If the code
        // does, it is a bug, but we have no checks for this condition.
        finishTransaction(transaction);
    }

    if (status == ExecutionEngine::OK) {
        queued_results_.push_back(transaction);
    } else if (status == ExecutionEngine::BLOCKED) {
        // Hold on to blocked transactions for cycle detection
        pair<BlockedSet::iterator, bool> result = blocked_transactions_.insert(transaction);
        assert(transaction->blocked());
        assert(result.second);  // false means this transaction was already blocked!
    } else if (status == ExecutionEngine::ABORT_USER) {
        // User abort: add the results to the queue
        queued_results_.push_back(transaction);
        // TODO: Automatically abort the transaction here? This may improve performance. However,
        // the server currently checks queued_results_ in order to see if it can commit/abort
        // transactions immediately *anyway*.
    } else {
        CHECK(false);
    }
}

void LockScheduler::commit(Transaction* txn) {
    LockTransaction* transaction = reinterpret_cast<LockTransaction*>(txn);
    assert(!transaction->blocked());
    finishTransaction(transaction);
    delete transaction;
}

void LockScheduler::abort(Transaction* txn) {
    LockTransaction* transaction = reinterpret_cast<LockTransaction*>(txn);

    // Look for the transaction in the "to be executed" queue: also can happen
    for (size_t i = 0; i < execute_queue_.size(); ++i) {
        if (execute_queue_.at(i) == transaction) {
            execute_queue_.erase(i);
            break;
        }
    }

    // Look for the transaction in the output queue: can happen with unusual abort timing
    // Add the following check before the loop? if (!transaction->results_sent()) {}
    queued_results_.eraseValue(transaction);

    // HACK: This forces finishTransaction to apply the undo buffer
    transaction->setWorkUnitStatus(ExecutionEngine::ABORT_USER);
    finishTransaction(transaction);
    delete transaction;
}

Transaction* LockScheduler::getWorkUnitResult() {
    Transaction* result = NULL;
    if (!queued_results_.empty()) {
        result = queued_results_.dequeue();
    }
    return result;
}

/* Deadlock detection heuristics:

- idle() is called at the bottom of the libevent loop. Don't want to cycle detect on each call,
  since we could have an abort() waiting for us in the network buffers.
- If we have >= 2 transactions, we might have deadlock.
- Deadlock can only happen if # of blocked transactions increases or is constant.
- If we do cycle detection and find nothing, we can safely block waiting for messages as there is
  no deadlock.

TODO: We might need to rate-limit cycle detection using a timeout or similar?
*/
bool LockScheduler::idle() {
    int count = 0;
    while (!execute_queue_.empty() && count < 5) {
        executeWork(execute_queue_.dequeue());
        count += 1;
    }
    if (count > 0) {
        // Do not perform deadlock detection if we did work. NOTE: If we are constantly busy, this
        // won't work.
        return !execute_queue_.empty() || blocked_transactions_.size() >= 2;
    }
    assert(count == 0 && execute_queue_.empty());

    // TODO: prioritize multi-partition transactions
    if (last_blocked_ >= 2 && blocked_transactions_.size() >= last_blocked_) {
        // Perform cycle detection if we have had 2 or more blocked transactions for 2 polls
        // TODO: Can we use a smarter heuristic to avoid excess deadlock detection?
        vector<LockTransaction*> cycle = detectDeadlock();
        if (!cycle.empty()) {
            // choose the first single partition transaction we find to abort
            LockTransaction* target = NULL;
            for (int i = 0; i < cycle.size(); ++i) {
                if (!cycle[i]->multiple_partitions()) {
                    target = cycle[i];
                    break;
                }
            }
            if (target == NULL) {
                // TODO: Select the MP transaction that has performed the least work?
                target = cycle[0];
            }

            // break the cycle and output the transaction
            // TODO: Integrate this with the abort() method: the client will call abort() on this
            // transaction, which seems weird.
            // TODO: Automatically re-execute single partition transactions? Seems logical to me.
            finishTransaction(target);
            assert(target->last_status() == ExecutionEngine::BLOCKED);
            assert(target->output()->empty());
            target->setWorkUnitStatus(ExecutionEngine::ABORT_DEADLOCK);
            queued_results_.push_back(target);
        } else {
            // no possibility of deadlock: block
            return false;
        }
    }

    // Poll if we might have deadlock: requires at least 2 blocked transactions
    last_blocked_ = assert_range_cast<int>(blocked_transactions_.size());
    return blocked_transactions_.size() >= 2;
}

void LockScheduler::backupApplyTransaction(const std::vector<std::string>& work_units) {
    // Cannot have any active transactions
    CHECK(active_count_ == 0);

    // Apply each fragment without undo or locks
    string out;
    for (int i = 0; i < work_units.size(); ++i) {
        out.clear();
        ExecutionEngine::Status status = engine_->tryExecute(work_units[i], &out, NULL, NULL, NULL);
        CHECK(status == ExecutionEngine::OK);
    }
}

vector<LockTransaction*> LockScheduler::detectDeadlock() const {
    BlockedSet visited;
    vector<LockTransaction*> cycle;

    // Iterate through all the blocked transactions.
    const BlockedSet::const_iterator end = blocked_transactions_.end();
    for (BlockedSet::const_iterator i = blocked_transactions_.begin(); i != end; ++i) {
        assert((*i)->blocked());

        if (visited.find(*i) == visited.end()) {
            // unvisited transaction: visit!
            if (depthFirstSearch(&visited, *i, &cycle)) {
                return cycle;
            }
        }
    }
    return cycle;
}

bool LockScheduler::depthFirstSearch(BlockedSet* visited, LockTransaction* start,
        vector<LockTransaction*>* cycle) const {
    // Begin a new depth-first search
    assert(visited->find(start) == visited->end());
    vector<vector<LockTransaction*>* > visit_stack;
    visit_stack.push_back(new vector<LockTransaction*>(1, start));

    while (!visit_stack.empty()) {
        // Take the next transaction, record that we have visited it
        // NOTE: It seems like we should be able to prune earlier, by checking if visited before
        // adding to the queue. However, this runs into problems when following a path like:
        // abcb or acbc. This "early pruning" will stop at abc or acb, not finding the cycle.
        vector<LockTransaction*>* path = visit_stack.back();
        visit_stack.pop_back();
        assert(path->back()->blocked());
        pair<BlockedSet::iterator, bool> result = visited->insert(path->back());
        if (!result.second) {
            // we have already visited this, probably on some other path: discard
            delete path;
            continue;
        }

        // Find all the transactions holding the lock this transaction wants
        const Lock::TransactionSet& waits_for_set = path->back()->blocked_lock()->holders();
        assert(!waits_for_set.empty());

        // Visit all the "waits for" transactions
        const Lock::TransactionSet::const_iterator end = waits_for_set.end();
        for (Lock::TransactionSet::const_iterator waits_for_it = waits_for_set.begin();
                waits_for_it != end; ++waits_for_it) {
            // If we are waiting for ourself, this *must* be a lock upgrade
            LockTransaction* waits_for = reinterpret_cast<LockTransaction*>(*waits_for_it);
            if (waits_for == path->back()) {
                // do not add a waits for edge to ourself for upgrades.
                assert(path->back()->blocked_lock()->state() == Lock::LOCKED_READ);
                assert(waits_for_set.size() > 1);
                continue;
            }

            if (!waits_for->blocked()) {
                // Not blocked: do not visit
                continue;
            }

            vector<LockTransaction*>::iterator on_path =
                    std::find(path->begin(), path->end(), waits_for);
            if (on_path != path->end()) {
                // copy the cycle to the output
                while (on_path != path->end()) {
                    cycle->push_back(*on_path);
                    ++on_path;
                }

                // Clean up and return
                delete path;
                STLDeleteElements(&visit_stack);
                return true;
            }

            if (visited->find(waits_for) == visited->end()) {
                // Another unvisited blocked transaction: visit it
                vector<LockTransaction*>* visit_path = new vector<LockTransaction*>(*path);
                visit_path->push_back(waits_for);
                visit_stack.push_back(visit_path);
            }
        }
        
        delete path;
    }
    return false;
}

void LockScheduler::finishTransaction(LockTransaction* transaction) {
    // apply or free undo
    if (*transaction->undo() != NULL) {
        if (transaction->last_status() != ExecutionEngine::OK) {
            engine_->applyUndo(*transaction->undo(), std::string("LockScheduler::finishTransaction"));
        } else {
            // PAVLO
            engine_->freeUndo(*transaction->undo(), std::string("LockScheduler::finishTransaction"));
        }
        *transaction->undo() = NULL;
    }

    // remove this transaction from the blocked transactions list
    if (transaction->blocked()) {
        size_t count = blocked_transactions_.erase(transaction);
        ASSERT(count == 1);
    } else {
        assert(blocked_transactions_.find(transaction) == blocked_transactions_.end());
    }

    // Update the number of "active transactions"
    if (transaction->has_locks()) {
        assert(active_count_ > 0);
        active_count_ -= 1;
    }

    vector<LockTransaction*> granted_transactions;
    transaction->dropLocks(&granted_transactions);

    // Re-execute work units for transactions that were granted locks
    for (int i = 0; i < granted_transactions.size(); ++i) {
        size_t count = blocked_transactions_.erase(granted_transactions[i]);
        ASSERT(count == 1);
        granted_transactions[i]->lockGranted();
        workUnitAdded(granted_transactions[i]);
    }
}

}  // namespace dtxn
