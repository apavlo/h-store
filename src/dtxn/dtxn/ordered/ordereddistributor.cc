// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/ordered/ordereddistributor.h"

#include <netinet/in.h>
#include <sys/time.h>

#include <algorithm>

#include "base/stlutil.h"
#include "base/unordered_map.h"
#include "dtxn/configparser.h"
#include "dtxn/dtxndistributor.h"
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
class OrderedDistributor::TransactionState : public CoordinatedTransaction {
public:
    TransactionState(net::ConnectionHandle* client, size_t num_partitions,
            int32_t distributor_id, int32_t request_id, const string& initial_client_message) :
            CoordinatedTransaction(num_partitions, initial_client_message),

            client_(client),
            distributor_id_(distributor_id),
            request_id_(request_id),
            timer_(NULL),
            distributor_(NULL) {
        assert(client_ != NULL);
        assert(request_id_ >= 0);
    }

    ~TransactionState() {
        if (timer_ != NULL) {
            distributor_->event_loop()->cancelTimeOut(timer_);
        }
    }

    // Returns true if we have all the responses for this round
    bool responseReceived(int partition_index, const std::string& response,
            ExecutionEngine::Status status) {
        // TODO: Abort early?
        receive(partition_index, response, status);
        return receivedAll();
    }

    net::ConnectionHandle* client() { return client_; }
    void clearClient() { assert(client_ != NULL); client_ = NULL; }
    int32_t distributor_id() const { return distributor_id_; }
    int32_t request_id() const { return request_id_; }

    void startResponseTimer(OrderedDistributor* distributor, int timeout_ms) {
        distributor_ = distributor;

        if (timer_ == NULL) {
            timer_ = distributor_->event_loop()->createTimeOut(timeout_ms, timerCallback, this);
        } else {
            distributor_->event_loop()->resetTimeOut(timer_, timeout_ms);
        }
    }

    // Mark that we depend on transaction_id
    void dependsOn(int transaction_id, int partition_id) {
        // we can get this multiple times for the same transaction
        assert(transaction_id >= 0);
        assert(transaction_id < distributor_id_);
        assert(isPartitionInvolved(partition_id));
        
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
        removeResponse(partition_id);
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

    typedef std::tr1::unordered_set<TransactionState*> DependentSet;
    DependentSet* dependents() { return &dependents_; }

private:
    static void timerCallback(void* argument) {
        TransactionState* transaction = reinterpret_cast<TransactionState*>(argument);
        transaction->distributor_->responseTimeout(transaction);
    }

    net::ConnectionHandle* client_;
    int32_t distributor_id_;
    int32_t request_id_;

    // Used to time out this transaction if a round takes too long to complete
    void* timer_;
    OrderedDistributor* distributor_;

    // Map of transaction ids -> partition indicies. The keys are the transactions this transaction
    // depends on, while the values are the partitions that depend on that particular tranaction.
    typedef base::unordered_map<int, vector<int> > DependencyMap;
    DependencyMap dependencies_;

    // Set of transactions that depend on this one
    DependentSet dependents_;
};

OrderedDistributor::OrderedDistributor(const vector<net::ConnectionHandle*>& partitions,
        Coordinator* coordinator, io::EventLoop* event_loop, net::MessageServer* msg_server) :
        partitions_(partitions),
        last_partition_commit_(partitions.size(), -1),
        coordinator_(coordinator),
        event_loop_(event_loop),
        msg_server_(msg_server) {
    assert(!partitions_.empty());
    assert(coordinator_ != NULL);
    assert(event_loop_ != NULL);

    msg_server_->addCallback(&OrderedDistributor::requestReceived, this);
    msg_server_->addCallback(&OrderedDistributor::responseReceived, this);
    coordinator_->set_manager(this);
}

OrderedDistributor::~OrderedDistributor() {
    //~ STLDeleteElements(&partitions_);
    // Close all partition connections
    for (size_t i = 0; i < partitions_.size(); ++i) {
        msg_server_->closeConnection(partitions_[i]);
    }

    // Clean up queued messages
    for (size_t i = queue_.firstIndex(); i < queue_.nextIndex(); ++i) {
        // TODO: is this the "correct" way to clean up queued messages?
        if (queue_.at(i) != NULL) {
            coordinator_->done(queue_.at(i));
            delete queue_.at(i);
        }
    }

    msg_server_->removeCallback(&OrderedDistributor::requestReceived);
    msg_server_->removeCallback(&OrderedDistributor::responseReceived);
}

void OrderedDistributor::requestReceived(net::ConnectionHandle* connection,
        const Fragment& request) {
    // TODO: Verify that the request ids are being generated correctly?

    // Queue and/or dispatch the transaction
    bool last_done = queue_.empty() || queue_.back() == NULL ||
            queue_.back()->isAllDone();
    TransactionState* state = new TransactionState(
            connection, partitions_.size(), assert_range_cast<int32_t>(queue_.nextIndex()),
            request.id, request.transaction);
    queue_.push_back(state);
    assert(queue_.at(state->distributor_id()) == state);
    if (last_done) {
        dispatchTransaction(queue_.back());
    }
}

void OrderedDistributor::responseReceived(net::ConnectionHandle* connection,
        const FragmentResponse& response) {
    // response must be for the current transaction, or the previous transaction if it aborted
    if (response.id < assert_range_cast<int32_t>(queue_.firstIndex())) {
        // ignore this response: it is for an old transaction
        // TODO: Verify that we aborted this transaction due to a timeout?
        return;
    }
    TransactionState* state = queue_.at(response.id);
    assert(state != NULL);
    assert(-1 <= response.dependency && response.dependency < response.id);

    // Find the partition index
    int partition_index = -1;
    for (partition_index = 0; partition_index < partitions_.size(); ++partition_index) {
        if (partitions_[partition_index] == connection) break;
    }
    assert(0 <= partition_index && partition_index < partitions_.size());

    state->receive(partition_index, response.result,
            (ExecutionEngine::Status) response.status);

    // track dependencies
    if (response.dependency != -1) {
        assert(response.dependency >= 0);
        // look for the transaction we depend on
        TransactionState* other = NULL;
        if (response.dependency >= queue_.firstIndex()) {
            other = queue_.at(response.dependency);
        }

        if (other != NULL) {
            if (other->hasResponse(partition_index)) {
                // The dependency is valid: track the relationship between the transactions
                state->dependsOn(response.dependency, partition_index);
                other->addDependent(state);
            } else {
                // The dependency is not valid: this is part of an abort chain
                state->removeResponse(partition_index);
            }
        } else {
            // TODO: record the state of the last transaction to check if it aborted.
            if (response.dependency > last_partition_commit_[partition_index]) {
                // this depends on a transaction that aborted: need to ignore this message
                state->removeResponse(partition_index);
            } else {
                assert(response.dependency == last_partition_commit_[partition_index]);
            }
        }
    }

    if (state->receivedAll() && state->dependenciesResolved()) {
        nextRound(state);
    }
}

void OrderedDistributor::nextRound(TransactionState* state) {
    // TODO: This executes the coordinator code before sending messages. It would be nice if we
    // could speculate the coordinator code, since it would reduce latency. However it also
    // complicates the abort code.
    assert(state->receivedAll() && state->dependenciesResolved());

    if (state->status() != ExecutionEngine::OK) {
        // We are aborting: send an abort to the client because the coordinator has no choice
        // Select the first message non-empty message we received
        // TODO: We need to run "user code" to decide what to do about the message.
        const string* message = NULL;
        assert(!state->received().empty());
        for (size_t i = 0; i < state->received().size(); ++i) {
            message = &state->received()[i].second;
            if (!message->empty()) {
                break;
            }
        }
        state->abort(*message);
        processFragments(state);
    } else {
        // We have received all responses: ask the coordinator what to do
        state->readyNextRound();
        coordinator_->nextRound(state);
    }
}

void OrderedDistributor::sendRound(CoordinatedTransaction* transaction) {
    TransactionState* state = static_cast<TransactionState*>(transaction);
    if (!state->multiple_partitions() && (!state->isAllDone() || state->sent().size() > 1)) {
        // for the first round, set the state of a multi-partition transaction
        // TODO: We could optimize multi-round single partition transactions.
        state->setMultiplePartitions();
    }
    processFragments(state);
}

void OrderedDistributor::responseTimeout(TransactionState* state) {
    assert(base::contains(queue_, state));

    // If the transaction times out, we abort it unconditionally: should indicate deadlock
    // TODO: Indicate a specific timeout code or message?
    state->abort("");
    finishTransaction(state);
}

void OrderedDistributor::dispatchTransaction(TransactionState* state) {
    // every transaction except this one must be all done
#ifndef NDEBUG
    for (size_t i = queue_.firstIndex(); i < state->distributor_id(); ++i) {
        assert(queue_.at(i) == NULL || queue_.at(i)->isAllDone());
    }
#endif

    coordinator_->begin(state);
}

void OrderedDistributor::processFragments(TransactionState* state) {
    assert(!state->sent().empty());

    // If this transaction has completed ...
    if (state->haveClientResponse()) {
        // ... and there are no dependencies, send results to client and clean up
        if (state->dependenciesResolved()) {
            finishTransaction(state);
        }
        return;
    }

    // Send out messages to partitions
    Fragment request;
    request.id = state->distributor_id();
    request.multiple_partitions = state->multiple_partitions();
    const CoordinatedTransaction::MessageList& messages = state->sent();
    for (int i = 0; i < messages.size(); ++i) {
        int partition_index = messages[i].first;
        request.transaction = messages[i].second;
        request.last_fragment = state->isDone(partition_index);
        msg_server_->send(partitions_[partition_index], request);
    }

    // start the deadlock timer for multi-partition transactions
    // TODO: Don't do this for the "ordered request" mode?
    if (request.multiple_partitions) {
        // TODO: Does adding a small random variation reduce probability of simultaneous aborts?
        // TODO: Tune this better?
        state->startResponseTimer(this, 200);
    }
    state->sentMessages();

    // If this is the last round, dispatch the next transaction
    if (state->isAllDone() && state->distributor_id() +1 < queue_.nextIndex()) {
        dispatchTransaction(queue_.at(state->distributor_id() + 1));
    }
}

bool OrderedDistributor::removeDependency(
        TransactionState* transaction, int transaction_id, int partition_id) {
    bool removed_dependency = transaction->removeDependency(transaction_id, partition_id);
    if (removed_dependency) {
        // We removed the dependency: do this recursively for all dependents
        TransactionState::DependentSet* dependents = transaction->dependents();
        typedef TransactionState::DependentSet::iterator SetIterator;
        for (SetIterator i = dependents->begin(); i != dependents->end();) {
            TransactionState* dep_txn = *i;
            bool removed = removeDependency(dep_txn, transaction->distributor_id(), partition_id);
            SetIterator last = i;  // supports erasing
            ++i;
            if (removed && !dep_txn->hasDependencyOn(transaction->distributor_id())) {
                // we removed the last dependency from *i to transaction: forget it
                dependents->erase(last);
            }
        }
    }
    return removed_dependency;
}

void OrderedDistributor::finishTransaction(TransactionState* state) {
    assert(state->haveClientResponse() && state->dependenciesResolved());
    if (state->multiple_partitions()) {
        CommitDecision decision;
        decision.id = state->distributor_id();
        decision.commit = state->status() == ExecutionEngine::OK;

        vector<int> involved_partitions = state->involvedPartitions();
        for (int i = 0; i < involved_partitions.size(); ++i) {
            int index = involved_partitions[i];
            msg_server_->send(partitions_[index], decision);
            assert(decision.id > last_partition_commit_[index]);
            if (decision.commit) last_partition_commit_[index] = decision.id;
        }
    }

    // Return the results to the client
    FragmentResponse client_response;
    client_response.id = state->request_id();
    client_response.status = state->status();
    assert(state->sent()[0].first ==
            CoordinatedTransaction::CLIENT_PARTITION);
    client_response.result = state->sent()[0].second;
    msg_server_->send(state->client(), client_response);

    // Clean up this transaction, since we are DONE
    coordinator_->done(state);

    // Remove the request from the queue
    assert(queue_.at(state->distributor_id()) == state);
    queue_.at(state->distributor_id()) = NULL;
    while (!queue_.empty() && queue_.front() == NULL) {
        queue_.pop_front();
    }

    const TransactionState::DependentSet& dependents = *state->dependents();
    assert(dependents.empty() || state->multiple_partitions());
    typedef TransactionState::DependentSet::const_iterator SetIterator;
    if (state->status() != ExecutionEngine::OK) {
        vector<int> involved_partitions = state->involvedPartitions();
        for (size_t i = 0; i < involved_partitions.size(); ++i) {
            // remove the dependency for all partitions on all dependent transactions
            for (SetIterator it = dependents.begin(); it != dependents.end(); ++it) {
                removeDependency(*it, state->distributor_id(), involved_partitions[i]);
            }
        }
    } else {
        for (SetIterator i = dependents.begin(); i != dependents.end(); ++i) {
            (*i)->resolveDependency(state->distributor_id());
            if ((*i)->receivedAll() && (*i)->dependenciesResolved()) {
                nextRound(*i);
            }
        }
    }

    delete state;
}

}  // namespace dtxn
