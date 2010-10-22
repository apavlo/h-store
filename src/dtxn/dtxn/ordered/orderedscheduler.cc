// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/ordered/orderedscheduler.h"

#include <cstdlib>
#include <string>

#include "base/assert.h"
#include "base/debuglog.h"
#include "base/stlutil.h"
#include "dtxn/executionengine.h"
#include "dtxn/transactionstate.h"

using std::string;
using std::vector;

namespace dtxn {

void OrderedScheduler::cc_mode(ConcurrencyControl mode) {
    assert(mode == CC_BLOCK || mode == CC_SPECULATIVE);

    // Check that there is no work underway
    assert(unfinished_queue_.empty());
    cc_mode_ = mode;
}

void OrderedScheduler::fragmentArrived(FragmentState* fragment) {
    if (fragment->multiple_partitions()) {
        // Special: if this is work for a transaction being executed, put it at the front
        if (!unfinished_queue_.empty() && unfinished_queue_.front() == fragment->transaction()) {
            assert(unfinished_queue_.size() == 1);
            multi_partition_queue_.push_front(fragment);
        } else {
            multi_partition_queue_.push_back(fragment);
        }
    } else {
        single_partition_queue_.push_back(fragment);
    }
}

void OrderedScheduler::commit(TransactionState* transaction) {
    assert(transaction->last_fragment().response().status == ExecutionEngine::OK);
    assert(transaction->last_fragment().multiple_partitions() ||
            transaction == unfinished_queue_.front());

    // We can only commit the front of the unfinished queue.
    assert(unfinished_queue_.front() == transaction);
    unfinished_queue_.pop_front();

    // PAVLO
    LOG_DEBUG("Called commit with a TransactionState that has payload '%s'", transaction->payload().c_str());
    if (*transaction->undo() != NULL) {
        engine_->freeUndo(*transaction->undo(), transaction->payload());
    }

    // Speculative execution was correct: output transactions
    while (!unfinished_queue_.empty()) {
        TransactionState* speculative = unfinished_queue_.front();
        // If the result was already exposed (distributed speculation), don't expose it again
        const Fragment& speculative_request = speculative->last_fragment().request();
        bool results_already_exposed = batch_mps_ &&
                speculative->last_fragment().response().dependency == transaction->last_fragment().response().id;
        assert(!results_already_exposed ||
                (speculative_request.client_id == transaction->last_fragment().request().client_id));
        if (!results_already_exposed) {
            result_queue_.push_back(speculative->mutable_last());
        }

        if (!speculative->last_fragment().multiple_partitions()) {
            // finish single partition transactions, freeing any undo buffers they may have
            unfinished_queue_.pop_front();
            if (*speculative->undo() != NULL) {
                engine_->freeUndo(*speculative->undo(), speculative->payload());
            }
        } else {
            if (batch_mps_ && !results_already_exposed) {
                // check for any subsequent mp transactions that we can now release
                last_batch_id_ = speculative_request.id;
                for (int i = 1; i < unfinished_queue_.size(); ++i) {
                    const Fragment& fragment = unfinished_queue_.at(i)->last_fragment().request();
                    if (!fragment.multiple_partitions) continue;

                    if (fragment.client_id == speculative_request.client_id) {
                        // This mp txn has the same coordinator: release it speculatively
                        unfinished_queue_.at(i)->mutable_last()->mutable_response()->dependency =
                                speculative_request.id;
                        last_batch_id_ =
                                unfinished_queue_.at(i)->last_fragment().response().id;
                        result_queue_.push_back(unfinished_queue_.at(i)->mutable_last());
                    } else {
                        // This comes from a different coordinator: breaks the dependency chain
                        last_batch_id_ = -1;
                        break;
                    }
                }
            }

            // Only output the first chunk of a multi-partition transaction that was speculated
            break;
        }
    }

    executeQueue();
}

void OrderedScheduler::abort(TransactionState* transaction) {
    assert(transaction->last_fragment().multiple_partitions() ||
            transaction == unfinished_queue_.front());
    // TODO: Support aborting unordered transactions
    assert(!unfinished_queue_.empty());

    assert(transaction->last_fragment().response().status != ExecutionEngine::INVALID);
    // Look for the transaction in the speculative queue
    if (transaction->last_fragment().response().status != ExecutionEngine::INVALID) {
        bool found_speculative = false;
        for (size_t i = 0; i < unfinished_queue_.size(); ++i) {
            if (unfinished_queue_.at(i) == transaction) {
                // Speculative execution incorrect: put transactions back on the queue
                // and apply undo in LIFO order
                while (unfinished_queue_.size() - 1 > i) {
                    TransactionState* speculative = unfinished_queue_.back();
                    unfinished_queue_.pop_back();
                    // undo can be null if it speculatively aborted or if it does no writes
                    if (*speculative->undo() != NULL) {
                        engine_->applyUndo(*speculative->undo(), speculative->payload());
                        *speculative->undo() = NULL;
                    }
                    speculative->mutable_last()->mutable_response()->result.clear();
                    speculative->mutable_last()->mutable_response()->dependency = -1;
                    blocked_queue_.push_front(speculative->mutable_last());
                }

                assert(unfinished_queue_.back() == transaction);
                unfinished_queue_.pop_back();
                if (*transaction->undo() != NULL) {
                    engine_->applyUndo(*transaction->undo(), transaction->payload());
                }

                // Adjust the last batch id to the correct value
                if (transaction->last_fragment().response().dependency != -1) {
                    last_batch_id_ = transaction->last_fragment().response().dependency;
                }

                found_speculative = true;
                break;
            }
        }
        assert(found_speculative);
    }

    executeQueue();
}

FragmentState* OrderedScheduler::executed() {
    if (result_queue_.empty()) {
        return NULL;
    }
    return result_queue_.dequeue();
}

void OrderedScheduler::order(vector<FragmentState*>* queue) {
    assert(queue->empty());

    // Queue multi-partition transactions with high priority. This also "continues" unfinished
    // multi-partition transactions because they are placed at the front of the queue
    // TODO: Tune the number of "pre-replicated" transactions?
    while (queue->size() < MAX_TXNS_PER_LOOP && !multi_partition_queue_.empty()) {
        queue->push_back(multi_partition_queue_.dequeue());
    }

    // Queue single partition transactions in any left over space. Limit number of queued
    // single partition transactions in order to provide low-latency multi-partition transactions
    while (blocked_queue_.size() + queue->size() < MAX_TXNS_PER_LOOP &&
            !single_partition_queue_.empty()) {
        queue->push_back(single_partition_queue_.dequeue());
    }
}

void OrderedScheduler::execute(FragmentState* state) {
    if (!realExecute(state)) {
        // could not execute: queue for later
        blocked_queue_.push_back(state);
    } else if (cc_mode_ == CC_SPECULATIVE && state->request().multiple_partitions && state->request().last_fragment) {
        // This is the last fragment of a multi-partition transaction: try to speculate if we can
        assert(blocked_queue_.empty() || state->transaction()->fragments().size() > 1);
        executeQueue();
    }
}

void OrderedScheduler::executeQueue() {
    while (!blocked_queue_.empty()) {
        FragmentState* fragment = blocked_queue_.front();
        if (!realExecute(fragment)) {
            // this fragment is blocked: leave it at the front
            break;
        }

        // executed! remove from queue
        blocked_queue_.pop_front();
    }
}

bool OrderedScheduler::realExecute(FragmentState* state) {
    FragmentResponse* response = state->mutable_response();
    // if we have pending transactions, but this is a different transaction ...
    if (!unfinished_queue_.empty() && state->transaction() != unfinished_queue_.front()) {
        if (cc_mode_ == CC_SPECULATIVE &&
                unfinished_queue_.back()->last_fragment().request().last_fragment) {
            // speculate with undo
            response->status = engine_->tryExecute(state->request().transaction, &response->result,
                    state->transaction()->undo(), NULL, state->transaction()->payload());
            unfinished_queue_.push_back(state->transaction());

            // Look to see if this has the same coordinator as the previously "released"
            // multi-partition transaction.
            if (batch_mps_ && last_batch_id_ != -1 && state->multiple_partitions()) {
                int last_released_client =
                        unfinished_queue_.front()->last_fragment().request().client_id;
                if (last_released_client == state->request().client_id) {
                    // TODO: Does this need to be part of the if statement?
                    CHECK(state->multiple_partitions());
                    // Same coordinator: safe to release as long as it is marked as dependent
                    // TODO: If we have a cascading abort, we need to search the result queue :(
                    response->dependency = last_batch_id_;
                    assert(response->dependency != response->id);
                    last_batch_id_ = response->id;
                    result_queue_.push_back(state);
                } else {
                    // The first transaction from another client breaks the dependency chain
                    last_batch_id_ = -1;
                }
            }

            return true;
        } else {
            // this transaction is blocked, but we might be retrying to see if we can execute it
            assert(!base::contains(blocked_queue_, state) || state == blocked_queue_.front());
            assert(!base::contains(unfinished_queue_, state->transaction()));
            return false;
        }
        CHECK(false);
    }

    // If this is a "real" single partition transaction, we can execute without undo
    // If this is a multi-round sp txn that is committing, it will have an undo buffer
    if (!state->multiple_partitions() && unfinished_queue_.empty()) {
        // Execute without undo
        assert(unfinished_queue_.empty());
        response->status = engine_->tryExecute(
                state->request().transaction, &response->result, NULL, NULL, std::string("OrderedScheduler::realExecute-1"));
    } else {
        assert(state->multiple_partitions() || state->transaction() == unfinished_queue_.front());
        assert(unfinished_queue_.empty() || unfinished_queue_.front() == state->transaction());
        if (unfinished_queue_.empty()) {
            unfinished_queue_.push_back(state->transaction());
        }

        // Execute with undo
        response->status = engine_->tryExecute(state->request().transaction, &response->result,
                state->transaction()->undo(), NULL, std::string("OrderedScheduler::realExecute-2"));
        if (batch_mps_) {
            last_batch_id_ = response->id;
        }
        if (!state->multiple_partitions()) {
            // clean up the undo as appropriate
            // TODO: Should dtxnserver call this on our behalf?
            if (response->status == ExecutionEngine::OK) {
                commit(state->transaction());
            } else {
                abort(state->transaction());
            }
        }
    }

    assert(response->status == ExecutionEngine::OK ||
            response->status == ExecutionEngine::ABORT_USER);
    result_queue_.push_back(state);
    return true;
}

bool OrderedScheduler::idle() {
    if (!unfinished_queue_.empty()) {
        if (cc_mode_ == CC_BLOCK) {
            // multi-partition txn pending: block if the blocked queue is too large
            return !multi_partition_queue_.empty() ||
                    (blocked_queue_.size() < MAX_TXNS_PER_LOOP && !single_partition_queue_.empty());
        } else {
            if (!multi_partition_queue_.empty()) return true;
            if (unfinished_queue_.back()->last_fragment().request().last_fragment) {
                // can speculate any number of single partition transactions
                return !single_partition_queue_.empty();
            }
            return blocked_queue_.size() < MAX_TXNS_PER_LOOP && !single_partition_queue_.empty();
        }
    } else {
        // no multi-partition txns pending: poll if we have more txns
        assert(multi_partition_queue_.empty());
        assert(blocked_queue_.empty());
        return !single_partition_queue_.empty();
    }
}

}  // namespace dtxn
