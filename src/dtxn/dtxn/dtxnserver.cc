// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/dtxnserver.h"

#include <cstdlib>
#include <string>

#include "base/stlutil.h"
#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "dtxn/scheduler.h"
#include "dtxn/transactionstate.h"
#include "io/eventloop.h"
#include "net/messageserver.h"
#include "replication/faulttolerantlog.h"

using std::make_pair;
using std::pair;
using std::string;
using std::vector;

namespace dtxn {

/** Stores the state of the transaction that the server must track. */
class ServerTransactionState : public TransactionState {
public:
    ServerTransactionState(net::ConnectionHandle* client) : client_(client) {}

    net::ConnectionHandle* client() { return client_; }

private:
    net::ConnectionHandle* client_;
};

class DtxnServer::QueuedWork {
public:
    QueuedWork() { clear(); }

    void clear() {
        transaction_ = NULL;
        fragment_ = NULL;
    }

    void set(ServerTransactionState* transaction) {
        assert(transaction->last_fragment().multiple_partitions());
        transaction_ = transaction;
        assert(fragment_ == NULL);
    }

    void set(FragmentState* fragment) {
        assert(fragment != NULL);
        fragment_ = fragment;
        assert(transaction_ == NULL);
    }

    FragmentState* fragment() const {
        assert(fragment_ != NULL);
        return fragment_;
    }

    ServerTransactionState* transaction() const {
        assert(transaction_ != NULL);
        return transaction_;
    }

    bool isDecision() const {
        return transaction_ != NULL;
    }

private:
    ServerTransactionState* transaction_;
    FragmentState* fragment_;
};

DtxnServer::DtxnServer(Scheduler* scheduler, io::EventLoop* event_loop,
        net::MessageServer* msg_server, replication::FaultTolerantLog* log) :

        scheduler_(scheduler), event_loop_(event_loop), msg_server_(msg_server), log_(log) {
    assert(scheduler_ != NULL);
    assert(event_loop_ != NULL);
    msg_server_->addCallback(&DtxnServer::fragmentReceived, this);
    msg_server_->addCallback(&DtxnServer::decisionReceived, this);
    event_loop_->addIdleCallback(&idleCallback, this);

    if (log_ != NULL) {
        log_->entry_callback(this);
    }
}

DtxnServer::~DtxnServer() {
    // Unregister message callbacks
    msg_server_->removeCallback(&DtxnServer::fragmentReceived);
    msg_server_->removeCallback(&DtxnServer::decisionReceived);

    // Unregister the idle callback so it does not get called if the event loop is still used
    event_loop_->removeIdleCallback(&idleCallback);

    STLDeleteValues(&transaction_id_map_);

    delete scheduler_;

    // TODO: If we exit while there are transactions pending in the fault tolerant log we will
    // leak them, because we are relying on the callback to "find" them again. Do we care?
}

static int64_t makeTxnKey(int32_t client_id, int32_t id) {
    return (int64_t) client_id << 32 | (int64_t) id;
}

void DtxnServer::fragmentReceived(
        net::ConnectionHandle* connection, const Fragment& fragment) {
    // TODO: Fix this hack! We need real client ids
    const_cast<Fragment&>(fragment).client_id = (int32_t) reinterpret_cast<intptr_t>(connection);

    // Look up or create the transaction state; queue work in the scheduler
    ServerTransactionState* transaction = findOrCreateTransaction(connection, fragment);
    assert(!transaction->has_fragments() || transaction->last_fragment().response().status == ExecutionEngine::OK);
    FragmentState* frag_state = transaction->addFragment(fragment);
    scheduler_->fragmentArrived(frag_state);
}

void DtxnServer::decisionReceived(
        net::ConnectionHandle* connection, const CommitDecision& decision) {
    // TODO: Fix this hack! We need real client ids
    const_cast<CommitDecision&>(decision).client_id =
            (int32_t) reinterpret_cast<intptr_t>(connection);

    // Record the decision: it will be acted upon later
    ServerTransactionState* transaction = findTransaction(decision);
    // TODO: if this is an abort for a transaction in the "to execute" queue, we need to abort it
    CHECK(transaction->last_fragment().readyToSend());
    transaction->decide(decision.commit);
    execute_queue_.add()->set(transaction);

    if (log_ != NULL) {
        // Replication: clean up the transaction when executed and replicated. This allows
        // any pending "fragment is replicated" callbacks to not crash or need modification.
        log_->submit(decision, transaction);
    } else {
        transaction->decisionReplicated();
    }
}

ServerTransactionState* DtxnServer::findTransaction(const CommitDecision& decision) {
    int64_t key = makeTxnKey(decision.client_id, decision.id);
    TransactionIdMap::iterator it = transaction_id_map_.find(key);
    assert(it != transaction_id_map_.end());
    return it->second;
}

void DtxnServer::nextLogEntry(int sequence, const std::string& serialized_entry, void* argument) {
    assert(log_ != NULL);

    // TODO: This is a disgusting hack that breaks whenever we change the messages. Fix.
    static const int COMMIT_DECISION_SERIALIZED_SIZE = 9;

    if (log_->isPrimary()) {
        if (serialized_entry.size() > COMMIT_DECISION_SERIALIZED_SIZE) {
            // This is a transaction fragment that has been replicated
            FragmentState* fragment = (FragmentState*) argument;
            fragment->setReplicated();
            if (fragment->readyToSend()) {
                sendResponse(fragment);
            }
        } else {
            // TODO: We need to ACK decisions so the coordinator can forget about the transaction
            ServerTransactionState* transaction = (ServerTransactionState*) argument;
            transaction->decisionReplicated();
            if (transaction->finished()) {
                cleanUpTransaction(transaction);
            }
        }
    } else {
        assert(argument == NULL);
        if (serialized_entry.size() > COMMIT_DECISION_SERIALIZED_SIZE) {
            // Transaction fragment: process it
            // TODO: Recycle?
            Fragment request;
            request.parseFromString(serialized_entry);
            ServerTransactionState* transaction = findOrCreateTransaction(NULL, request);
            FragmentState* fragment = transaction->addFragment(request);
            fragment->setReplicated();
            execute_queue_.add()->set(fragment);
        } else {
            // Decisions
            CommitDecision decision;
            decision.parseFromString(serialized_entry);
            ServerTransactionState* transaction = findTransaction(decision);
            transaction->decide(decision.commit);
            transaction->decisionReplicated();
            execute_queue_.add()->set(transaction);
        }
    }
}

void DtxnServer::pollForResults() {
    const bool is_primary = log_ == NULL || log_->isPrimary();
    FragmentState* fragment = NULL;
    while ((fragment = scheduler_->executed()) != NULL) {
        fragment->setHasResults();
        if (is_primary) {
            if (fragment->readyToSend()) sendResponse(fragment);
        } else {
            if (fragment->transaction()->finished()) {
                cleanUpTransaction((ServerTransactionState*) fragment->transaction());
            }
        }
    }
}

void DtxnServer::sendResponse(FragmentState* fragment) {
    assert(fragment->readyToSend());
    assert(log_ == NULL || log_->isPrimary());
    if (fragment->multiple_partitions()) {
        realSendResponse(fragment);
    } else {
        sp_pending_sends_.push_back(fragment);
    }
}

void DtxnServer::realSendResponse(FragmentState* fragment) {
    assert(fragment->readyToSend());
    msg_server_->send(
            ((ServerTransactionState*) fragment->transaction())->client(), fragment->response());

    // If this is a single partition transaction result, we are done: delete it
    if (!fragment->multiple_partitions()) {
        cleanUpTransaction((ServerTransactionState*) fragment->transaction());
    }
}

ServerTransactionState* DtxnServer::findOrCreateTransaction(
        net::ConnectionHandle* connection, const Fragment& fragment) {
    // Look up or create the transaction state
    int64_t key = makeTxnKey(fragment.client_id, fragment.id);
    pair<TransactionIdMap::iterator, bool> result = transaction_id_map_.insert(
            make_pair(key, (ServerTransactionState*) NULL));

    if (result.second) {
        // We did an insert: start a new transactioon
        assert(result.first->second == NULL);
        // TODO: Recycle state objects?
        result.first->second = new ServerTransactionState(connection);
    } else {
        // This had better be another message for an in-progress multiple partition transaction
        assert(result.first->second != NULL);
        assert(result.first->second->last_fragment().multiple_partitions());
        assert(fragment.multiple_partitions || fragment.last_fragment);
    }

    return result.first->second;
}

void DtxnServer::cleanUpTransaction(ServerTransactionState* transaction) {
    int64_t key = makeTxnKey(transaction->client_id(), transaction->id());
    size_t count = transaction_id_map_.erase(key);
    ASSERT(count == 1);
    delete transaction;
}

bool DtxnServer::idle() {
    // Get the next batch of fragments to replicate and execute
    vector<FragmentState*> next_execute;
    scheduler_->order(&next_execute);

    if (log_ != NULL) {
        assert(log_->isPrimary() || next_execute.empty());
        // Start replicating the messages and queue work
        for (size_t i = 0; i < next_execute.size(); ++i) {
            log_->submit(next_execute[i]->request(), next_execute[i]);
            execute_queue_.add()->set(next_execute[i]);
        }

        // flush any buffered replication messages
        // TODO: Use a more intelligent policy?        
        log_->flush();
    } else {
        // fake replication and queue work
        for (size_t i = 0; i < next_execute.size(); ++i) {
            next_execute[i]->setReplicated();
            execute_queue_.add()->set(next_execute[i]);
        }
    }

    // Poll for results at least once, in case of asynchronous responses
    // TODO: Keep an "active count" to check when we should do this?
    if (execute_queue_.empty()) {    
        pollForResults();
    }

    // Execute from the queue
    // TODO: Apply a time limit to ensure fast response time?
    while (!execute_queue_.empty()) {
        const QueuedWork& work = execute_queue_.front();
        if (work.isDecision()) {
            ServerTransactionState* transaction = work.transaction();
            assert(transaction->undo() != NULL);
            if (transaction->decision()) {
                scheduler_->commit(transaction);
            } else {
                scheduler_->abort(transaction);
            }
            transaction->decisionExecuted();
            if (transaction->finished()) {
                cleanUpTransaction(transaction);
            }
        } else {
            scheduler_->execute(work.fragment());
        }
        execute_queue_.pop_front();
        pollForResults();
    }

    // Send any queued SP transactions
    while (!sp_pending_sends_.empty()) {
        realSendResponse(sp_pending_sends_.dequeue());
    }

    // TODO: Let the scheduler decide if we should poll?
    return scheduler_->idle();
}

bool DtxnServer::idleCallback(void* argument) {
    DtxnServer* server = reinterpret_cast<DtxnServer*>(argument);
    return server->idle();
}

}  // namespace dtxn
