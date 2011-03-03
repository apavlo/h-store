// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/ordered/ordereddtxnmanager.h"

#include <sys/time.h>

#include <algorithm>
#include <tr1/functional>

#include "base/stlutil.h"
#include "base/unordered_map.h"
#include "base/debuglog.h"
#include "dtxn/distributedtransaction.h"
#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "io/libeventloop.h"
#include "messageconnection.h"
#include "net/messageserver.h"
#include "strings/utils.h"

using std::string;
using std::vector;

namespace dtxn {

// Contains the state for a pending transaction.
class OrderedDtxnManager::TransactionState {
public:
    TransactionState(DistributedTransaction* transaction, int32_t manager_id) :
            transaction_(transaction),
            manager_id_(manager_id),
            timer_(NULL),
            manager_(NULL) {
        assert(manager_id_ >= 0);
    }

    ~TransactionState() {
        if (timer_ != NULL) {
            manager_->event_loop()->cancelTimeOut(timer_);
        }
    }

    void setCallback(const std::tr1::function<void()>& callback) {
        assert(callback_ == NULL);
        callback_ = callback;
    }

    int32_t manager_id() const { return manager_id_; }

    void startResponseTimer(OrderedDtxnManager* manager, int timeout_ms) {
        manager_ = manager;

        if (timer_ == NULL) {
            timer_ = manager_->event_loop()->createTimeOut(timeout_ms, timerCallback, this);
        } else {
            manager_->event_loop()->resetTimeOut(timer_, timeout_ms);
        }
    }

    // Mark that we depend on transaction_id
    void dependsOn(int transaction_id, int partition_id) {
        // we can get this multiple times for the same transaction
        assert(transaction_id >= 0);
        assert(transaction_id < manager_id_);
        assert(transaction_->isParticipant(partition_id));
        
        std::pair<DependencyMap::iterator, bool> result = 
                dependencies_.insert(std::make_pair(transaction_id, vector<int>()));
        assert(!base::contains(result.first->second, partition_id));
        result.first->second.push_back(partition_id);
    }

    bool hasDependencyOn(int transaction_id) const {
        return dependencies_.find(transaction_id) != dependencies_.end();
    }

    // transaction_id has committed
    void resolveDependency(int transaction_id) {
        size_t count = dependencies_.erase(transaction_id);
        ASSERT(count == 1);
    }

    bool removeDependency(int transaction_id, int partition_id) {
        // Find the partition in the dependency map
        DependencyMap::iterator it = dependencies_.find(transaction_id);
        if (it == dependencies_.end()) {
            // This can happen because we try to remove (txn, partition) for 
            // (dependents) x (involved partitions), so we might have already removed
            // this dependency
            return false;
        }
        vector<int>::iterator partition_it =
                std::find(it->second.begin(), it->second.end(), partition_id);
        if (partition_it == it->second.end()) {
            // No dependency for this partition
            return false;
        }

        // Remove the record of the dependency
        it->second.erase(partition_it);
        if (it->second.empty()) {
            dependencies_.erase(transaction_id);
        }

        // Remove the fragment
        transaction_->removeResponse(partition_id);
        return true;
    }

    const vector<int>& dependentPartitions(int transaction_id) const {
        DependencyMap::const_iterator it = dependencies_.find(transaction_id);
        assert(it != dependencies_.end());
        return it->second;
    }

    void addDependent(TransactionState* other) {
        // we can get this multiple times for the same transaction
        assert(other != this);
        dependents_.insert(other);
    }

    bool dependenciesResolved() const {
        return dependencies_.empty();
    }

    void finishedRound() {
        // Clear the sent messages from the transaction and call the callback
        bool all_done = !transaction_->multiple_partitions() && transaction_->isAllDone();
        transaction_->removePrepareResponses();
        transaction_->readyNextRound();
        std::tr1::function<void()> temp_callback = callback_;
        callback_ = NULL;
        if (all_done) {
            // Prevent accidental use of transaction_: the callback might delete it
            transaction_ = NULL;
        }
        temp_callback();
    }

    DistributedTransaction* transaction() { return transaction_; }

    typedef std::tr1::unordered_set<TransactionState*> DependentSet;
    DependentSet* dependents() { return &dependents_; }

private:
    static void timerCallback(void* argument) {
        TransactionState* transaction = reinterpret_cast<TransactionState*>(argument);
        transaction->manager_->responseTimeout(transaction);
    }

    DistributedTransaction* transaction_;
    int32_t manager_id_;
    std::tr1::function<void()> callback_;

    // Used to time out this transaction if a round takes too long to complete
    void* timer_;
    OrderedDtxnManager* manager_;

    // Map of transaction ids -> partition indicies. The keys are the transactions this transaction
    // depends on, while the values are the partitions that depend on that particular tranaction.
    typedef base::unordered_map<int, vector<int> > DependencyMap;
    DependencyMap dependencies_;

    // Set of transactions that depend on this one
    DependentSet dependents_;
};

OrderedDtxnManager::OrderedDtxnManager(io::EventLoop* event_loop, net::MessageServer* msg_server,
        const vector<net::ConnectionHandle*>& partitions) :
        partitions_(partitions),
        last_partition_commit_(partitions.size(), -1),
        partition_next_send_txn_id_(partitions.size(), 0),
        event_loop_(event_loop),
        msg_server_(msg_server) {
    assert(!partitions_.empty());
    assert(event_loop_ != NULL);

    msg_server_->addCallback(&OrderedDtxnManager::responseReceived, this);
}

OrderedDtxnManager::~OrderedDtxnManager() {
    // Close all partition connections
    for (size_t i = 0; i < partitions_.size(); ++i) {
        msg_server_->closeConnection(partitions_[i]);
    }

    // Clean up queued messages
    for (size_t i = queue_.firstIndex(); i < queue_.nextIndex(); ++i) {
        // TODO: is this the "correct" way to clean up queued messages?
        if (queue_.at(i) != NULL) {
            delete queue_.at(i);
        }
    }

    msg_server_->removeCallback(&OrderedDtxnManager::responseReceived);
}

void OrderedDtxnManager::execute(DistributedTransaction* transaction,
        const std::tr1::function<void()>& callback) {
    assert(!transaction->sent().empty());
    // TODO: Verify that the request ids are being generated correctly?

    // Queue and/or dispatch the transaction
    TransactionState* state = (TransactionState*) transaction->state();
    if (state == NULL) {
        state = new TransactionState(
                transaction, assert_range_cast<int32_t>(queue_.nextIndex()));
        transaction->state(state);
        queue_.push_back(state);
    } else {
        // This should be a "continuation" of an existing transaction
        assert(!transaction->received().empty());
    }
    state->setCallback(callback);
    assert(queue_.at(state->manager_id()) == state);

    // check if we can send out this round of messages
    trySendRound(state);

    // Check uninvolved partitions to see if we can send fragments of the next transactions
    // TODO: Only check the actual changed partitions? Some of this checking is redundant with
    // checks in trySendRound
    for (int i = 0; i < partitions_.size(); ++i) {
        if (transaction->isFinished(i) && partition_next_send_txn_id_[i] == state->manager_id()) {
            advanceSendId(i, state->manager_id());
        }
    }
}

void OrderedDtxnManager::trySendRound(TransactionState* state) {
    assert(!state->transaction()->sent().empty());

    const DistributedTransaction::MessageList& messages = state->transaction()->sent();
    if (state->transaction()->multiple_partitions()) {
        for (int i = 0; i < messages.size(); ++i) {
            int partition_index = messages[i].first;

            assert(partition_next_send_txn_id_[partition_index] <= state->manager_id());
            if (partition_next_send_txn_id_[partition_index] < state->manager_id()) {
                // There is some *other* transaction that still needs to finish, it could have JUST
                // finished and we are re-checking, so we can't assert() on it
                return;
            }
        }
    }

    sendFragments(state);

    // check the done state of any sent fragments
    // we can only advanced the counter for sent fragments because we need to make sure the
    // partition has started this transaction before we send the next ones!
    for (int i = 0; i < messages.size(); ++i) {
        int partition_index = messages[i].first;
        if (state->transaction()->isDone(partition_index) &&
                partition_next_send_txn_id_[partition_index] == state->manager_id()) {
            advanceSendId(partition_index, state->manager_id());
        }
    }
}

void OrderedDtxnManager::advanceSendId(int partition_index, int current_send_id) {
    int send_id = partition_next_send_txn_id_[partition_index];
    assert(send_id == current_send_id);
    assert(send_id < queue_.firstIndex() || queue_.at(send_id) == NULL ||
            queue_.at(send_id)->transaction()->isDone(partition_index));

    // increment the send_id until we get to the end or find a transaction that needs sending
    TransactionState* state = NULL;
    bool is_finished_txn = false;
    do {
        send_id += 1;
        partition_next_send_txn_id_[partition_index] = send_id;

        state = NULL;
        is_finished_txn = false;
        if (send_id < queue_.firstIndex()) {
            // transaction must be finished because we no longer have a record of it
            is_finished_txn = true;
        } else if (send_id < queue_.nextIndex()) {
            state = queue_.at(send_id);
            if (state == NULL) {
                is_finished_txn = true;
            } else if (!state->transaction()->multiple_partitions()) {
                is_finished_txn = true;
            } else {
                // is this multi-partition transaction finished at this partition?
                assert(state->transaction()->multiple_partitions());
                is_finished_txn = state->transaction()->isFinished(partition_index);
            }
        }
        // continue incrementing while there are more transactions and we have a finished transaction
    } while (send_id < queue_.nextIndex() && is_finished_txn);

    if (state == NULL) {
        assert(send_id == queue_.nextIndex());
        return;
    }
    assert(state->transaction()->multiple_partitions());
    assert(state->transaction()->isParticipant(partition_index) ||
            state->transaction()->isUnknown(partition_index));
    if (state->transaction()->isParticipant(partition_index)) {
        // try to send this round out
        trySendRound(state);
    }
}

void OrderedDtxnManager::finish(DistributedTransaction* transaction, bool commit,
        const string& payload, const std::tr1::function<void()>& callback) {
    CHECK(transaction->multiple_partitions());
    CHECK(transaction->status() == DistributedTransaction::OK);
    TransactionState* state = (TransactionState*) transaction->state();
    assert(state->transaction() == transaction);
    
    // PAVLO: Stick our payload in the TransactionState
    LOG_DEBUG("Got finish request from beyond the ether...");
    if (transaction->has_payload() == false) {
        LOG_DEBUG("Attaching payload '%s' to DistributedTransaction", payload.c_str());
        state->transaction()->set_payload(payload);
    }
    
    if (!transaction->isAllDone() && commit) {
        // Need a "prepare" round
        transaction->setAllDone();
        assert(!transaction->sent().empty());

        // Create a callback that will call this again once prepared
        state->setCallback(std::tr1::bind(
                &OrderedDtxnManager::verifyPrepareRound, this, transaction, callback));
                
        LOG_DEBUG("Going to call sendFragments to perform Canadian Voodoo Magic!!");
        sendFragments(state);

        // check if finishing this transaction has unblocked any rounds
        for (int i = 0; i < partitions_.size(); ++i) {
            if (partition_next_send_txn_id_[i] == state->manager_id()) {
                advanceSendId(i, state->manager_id());
            }
        }
    } else {
        finishTransaction(state, commit);
        delete state;
        // TODO: In the future this will be async due to replication/log flush
        callback();
    }
}

void OrderedDtxnManager::verifyPrepareRound(DistributedTransaction* transaction,
        const std::tr1::function<void()>& callback) {
    assert(transaction->isAllDone());
    assert(transaction->received().empty());
    // Can only call finish for multi-partition transactions
    if (transaction->multiple_partitions()) {
        // You're killing me here Evan...
        TransactionState* state = (TransactionState*) transaction->state();
        assert(state->transaction() == transaction);
        LOG_DEBUG("verifyPrepareRound() is also sending payload '%s'", state->transaction()->payload().c_str());
        
        finish(transaction, true, state->transaction()->payload(), callback);
    } else {
        // This is a single partition prepare that has completed: we are all done here
        assert(transaction->state() == NULL);
        callback();
    }
}

void OrderedDtxnManager::responseReceived(net::ConnectionHandle* connection,
        const FragmentResponse& response) {
    // response must be for the current transaction, or the previous transaction if it aborted
    if (response.id < assert_range_cast<int32_t>(queue_.firstIndex())) {
        // ignore this response: it is for an old transaction
        // TODO: Verify that we aborted this transaction due to a timeout?
        return;
    }
    TransactionState* state = queue_.at(response.id);
    assert(-1 <= response.dependency && response.dependency < response.id);

    // Find the partition index
    int partition_index = -1;
    for (partition_index = 0; partition_index < partitions_.size(); ++partition_index) {
        if (partitions_[partition_index] == connection) break;
    }
    assert(0 <= partition_index && partition_index < partitions_.size());
    // this must either be a single partition transaction (order doesn't matter),
    // the "current" transaction for this partition, or a "finished" transaction for this partition
    assert(!state->transaction()->multiple_partitions() ||
            partition_next_send_txn_id_[partition_index] == state->manager_id() ||
            (partition_next_send_txn_id_[partition_index] > state->manager_id() &&
                    state->transaction()->isDone(partition_index)));

    state->transaction()->receive(
            partition_index, response.result, (DistributedTransaction::Status) response.status);

    // track dependencies
    if (response.dependency != -1) {
        assert(response.dependency >= 0);
        // look for the transaction we depend on
        TransactionState* other = NULL;
        if (response.dependency >= queue_.firstIndex()) {
            other = queue_.at(response.dependency);
        }

        if (other != NULL) {
            if (other->transaction()->hasResponse(partition_index)) {
                // The dependency is valid: track the relationship between the transactions
                state->dependsOn(response.dependency, partition_index);
                other->addDependent(state);
            } else {
                // The dependency is not valid: this is part of an abort chain
                state->transaction()->removeResponse(partition_index);
            }
        } else {
            // TODO: record the state of the last transaction to check if it aborted.
            if (response.dependency > last_partition_commit_[partition_index]) {
                // this depends on a transaction that aborted: need to ignore this message
                state->transaction()->removeResponse(partition_index);
            } else {
                assert(response.dependency == last_partition_commit_[partition_index]);
            }
        }
    }

    if (state->transaction()->receivedAll() && state->dependenciesResolved()) {
        nextRound(state);
    }
}

void OrderedDtxnManager::nextRound(TransactionState* state) {
    // TODO: It would be nice if we could speculatively return results to the client, since it
    // would reduce latency. However it would complicate aborts significantly.
    assert(state->transaction()->receivedAll() && state->dependenciesResolved());

    // The transaction is completed if this is an abort or if it is single partition
    // TODO: Would it be simpler to not special case this?
    bool finished = state->transaction()->status() != DistributedTransaction::OK ||
            !state->transaction()->multiple_partitions();
    if (finished) {
        finishTransaction(state, state->transaction()->status() == DistributedTransaction::OK);
    }
    state->finishedRound();
    if (finished) {
        delete state;
    }
}

//~ void OrderedDtxnManager::sendRound(DistributedTransaction* transaction) {
    //~ TransactionState* state = static_cast<TransactionState*>(transaction);
    //~ if (!state->multiple_partitions() &&
            //~ (!state->transaction()->isAllDone() || state->sent().size() > 1)) {
        //~ // for the first round, set the state of a multi-partition transaction
        //~ // TODO: We could optimize multi-round single partition transactions.
        //~ state->setMultiplePartitions();
    //~ }
    //~ processFragments(state);
//~ }

void OrderedDtxnManager::responseTimeout(TransactionState* state) {
    assert(base::contains(queue_, state));

    // If the transaction times out, we abort it unconditionally: should indicate deadlock
    // TODO: Indicate a specific timeout code or message?
    //~ state->abort("");
    CHECK(false);
    finishTransaction(state, false);
}

void OrderedDtxnManager::sendFragments(TransactionState* state) {
    assert(!state->transaction()->sent().empty());

    // Send out messages to partitions
    Fragment request;
    request.id = state->manager_id();
    request.multiple_partitions = state->transaction()->multiple_partitions();

    // PAVLO: Pull the payload from the TransactionState's inner Transaction
    if (state->transaction()->has_payload()) {
        request.payload.assign(state->transaction()->payload());
        LOG_DEBUG("Yes! Attaching payload from TransactionState to a Fragment that we're sending off! Evan hates children! [%s]", request.payload.c_str());
    }
        
    const DistributedTransaction::MessageList& messages = state->transaction()->sent();
    for (int i = 0; i < messages.size(); ++i) {
        int partition_index = messages[i].first;
        assert(state->transaction()->isParticipant(partition_index));
        assert(!state->transaction()->multiple_partitions() ||
                partition_next_send_txn_id_[partition_index] == state->manager_id());

        request.transaction = messages[i].second;
        request.last_fragment = !state->transaction()->isActive(partition_index);
        bool success = msg_server_->send(partitions_[partition_index], request);
        ASSERT(success);
    }

    // start the deadlock timer for multi-partition transactions
    // TODO: Don't do this for the "ordered request" mode?
    if (request.multiple_partitions) {
        // TODO: Does adding a small random variation reduce probability of simultaneous aborts?
        // TODO: Re-enable this when it actually "works." A fixed value probably is not the answer.
        //~ state->startResponseTimer(this, 200);
    }
    state->transaction()->sentMessages();
}

bool OrderedDtxnManager::removeDependency(
        TransactionState* transaction, int transaction_id, int partition_id) {
    bool removed_dependency = transaction->removeDependency(transaction_id, partition_id);
    if (removed_dependency) {
        // We removed the dependency: do this recursively for all dependents
        TransactionState::DependentSet* dependents = transaction->dependents();
        typedef TransactionState::DependentSet::iterator SetIterator;
        for (SetIterator i = dependents->begin(); i != dependents->end();) {
            TransactionState* dep_txn = *i;
            bool removed = removeDependency(dep_txn, transaction->manager_id(), partition_id);
            SetIterator last = i;  // supports erasing
            ++i;
            if (removed && !dep_txn->hasDependencyOn(transaction->manager_id())) {
                // we removed the last dependency from *i to transaction: forget it
                dependents->erase(last);
            }
        }
    }
    return removed_dependency;
}

void OrderedDtxnManager::finishTransaction(TransactionState* state, bool commit) {
    assert(state->dependenciesResolved());
    assert(state->transaction()->isAllDone() || !commit);

    LOG_DEBUG("In finishTransaction() but we're out of control!!");
    
    if (state->transaction()->multiple_partitions()) {
        CommitDecision decision;
        decision.id = state->manager_id();
        decision.commit = commit;
        // PAVLO
        if (state->transaction()->has_payload()) {
            LOG_DEBUG("Setting CommitDecision.payload [%s]", state->transaction()->payload().c_str());
            decision.payload.assign(state->transaction()->payload());
        }

        vector<int> participants = state->transaction()->getParticipants();
        assert(!participants.empty());
        for (int i = 0; i < participants.size(); ++i) {
            int index = participants[i];
            assert(state->transaction()->isPrepared(index) || !commit);
            msg_server_->send(partitions_[index], decision);
            assert(decision.id > last_partition_commit_[index]);
            if (decision.commit) last_partition_commit_[index] = decision.id;
        }

        const TransactionState::DependentSet& dependents = *state->dependents();
        typedef TransactionState::DependentSet::const_iterator SetIterator;
        if (!decision.commit) {
            for (int i = 0; i < participants.size(); ++i) {
                // remove the dependency for all partitions on all dependent transactions
                for (SetIterator it = dependents.begin(); it != dependents.end(); ++it) {
                    removeDependency(*it, state->manager_id(), participants[i]);
                }
            }
        } else {
            for (SetIterator i = dependents.begin(); i != dependents.end(); ++i) {
                (*i)->resolveDependency(state->manager_id());
                if ((*i)->transaction()->receivedAll() && (*i)->dependenciesResolved()) {
                    nextRound(*i);
                }
            }
        }
    } else {
        assert(state->dependents()->empty());
        // single partition: decision must agree with the engine's response
        assert(commit == (state->transaction()->status() == DistributedTransaction::OK));
    }

    // "disconnect" the state to ensure we don't screw up
    assert(state->transaction()->state() == state);
    state->transaction()->state(NULL);

    // Remove the request from the queue
    assert(queue_.at(state->manager_id()) == state);
    queue_.at(state->manager_id()) = NULL;
    while (!queue_.empty() && queue_.front() == NULL) {
        queue_.pop_front();
    }

    // If this is an abort the done state has probably changed.
    if (!commit) {
        for (int i = 0; i < partitions_.size(); ++i) {
            if (partition_next_send_txn_id_[i] == state->manager_id()) {
                advanceSendId(i, state->manager_id());
            }
        }
    }
}

}  // namespace dtxn
