// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn2/partitionserver.h"

#include "dtxn/executionengine.h"
#include "dtxn/messages.h"
#include "dtxn2/transactionstate.h"
#include "io/eventloop.h"
#include "net/messageserver.h"

using std::make_pair;
using std::pair;

using dtxn::CommitDecision;
using dtxn::ExecutionEngine;
using dtxn::Fragment;

namespace dtxn2 {

class TransactionStateMachine {
public:
    TransactionStateMachine() { reset(); }
    void reset() { state_ = UNFINISHED; }

    void setExecuted() {
        if (state_ == DECIDED_REPLICATED) {
            state_ = DONE;
        } else if (state_ == FINISHED_REPLICATED) {
            state_ = PREPARED;
        } else if (state_ == UNFINISHED_FRAGMENT) {
            state_ = UNFINISHED;
        } else if (state_ == FINISHED_FRAGMENT) {
            state_ = FINISHED_EXECUTED;
        } else {
            assert(false);
        }
    }

    void setSinglePartitionDone() {
        if (state_ == PREPARED) {
            state_ = DONE;
        } else {
            assert(false);
        }
    }

    void setDecided() {
        if (state_ == PREPARED) {
            state_ = DECIDED_EXECUTED;
        } else {
            assert(false);
        }
    }

    // This is a hack to support aborting from any state without adding explicit edges.
    // TODO: Add explicit edges?
    void setAbort() {
        assert(state_ != DONE);
        state_ = DONE;
    }

    void setReplicated() {
        if (state_ == DECIDED_EXECUTED) {
            state_ = DONE;
        } else if (state_ == PREPARED) {
            state_ = DECIDED_REPLICATED;
        } else if (state_ == FINISHED_EXECUTED) {
            state_ = PREPARED;
        } else if (state_ == FINISHED_FRAGMENT) {
            state_ = FINISHED_REPLICATED;
        } else {
            assert(false);
        }
    }

    void setUnfinishedFragment() {
        if (state_ == UNFINISHED) {
            state_ = UNFINISHED_FRAGMENT;
        } else {
            assert(false);
        }
    }

    void setFinishedFragment() {
        if (state_ == UNFINISHED) {
            state_ = FINISHED_FRAGMENT;
        } else {
            assert(false);
        }
    }

    bool isUnfinished() const { return state_ == UNFINISHED; }
    bool isDecidedExecuted() const { return state_ == DECIDED_EXECUTED; }
    bool isDecidedReplicated() const { return state_ == DECIDED_REPLICATED; }
    bool isPrepared() const { return state_ == PREPARED; }
    bool isDone() const { return state_ == DONE; }
    bool isFinishedReplicated() const { return state_ == FINISHED_REPLICATED; }
    bool isFinishedExecuted() const { return state_ == FINISHED_EXECUTED; }
    bool isUnfinishedFragment() const { return state_ == UNFINISHED_FRAGMENT; }
    bool isFinishedFragment() const { return state_ == FINISHED_FRAGMENT; }

private:
    enum State {
        UNFINISHED,
        DECIDED_EXECUTED,
        DECIDED_REPLICATED,
        PREPARED,
        DONE,
        FINISHED_REPLICATED,
        FINISHED_EXECUTED,
        UNFINISHED_FRAGMENT,
        FINISHED_FRAGMENT,
    } state_;
};


class ServerTransactionState : public TransactionState {
public:
    ServerTransactionState(net::ConnectionHandle* connection) :
            connection_(connection),
            commit_(false) {}

    void addFragment(const dtxn::Fragment& fragment) {
        if (fragment.last_fragment) {
            state_.setFinishedFragment();
        } else {
            state_.setUnfinishedFragment();
        }
        TransactionState::addFragment(fragment);
    }

    void setExecuted(FragmentState* fragment, net::MessageServer* msg_server) {
        // TODO: This will need to change to support speculation and re-execution
        assert(fragment->transaction() == this);
        assert(mutable_last() == fragment);

        state_.setExecuted();
        if (state_.isFinishedExecuted() && 
                !last_fragment().request().multiple_partitions &&
                last_fragment().response().status != ExecutionEngine::OK) {
            // HACK: If this is a single partition transaction that is aborting, do not replicate,
            // just finish
            state_.setAbort();
            msg_server->send(connection_, fragment->response());
        } else if (state_.isUnfinished()) {
            msg_server->send(connection_, fragment->response());
        } else if (state_.isPrepared()) {
            enterPrepared(msg_server);
        }
    }

    void setReplicated(net::MessageServer* msg_server) {
        state_.setReplicated();
        if (state_.isPrepared()) {
            enterPrepared(msg_server);
        }
    }

    void decide(bool commit) {
        assert(!commit || last_fragment().response().status == ExecutionEngine::OK);
        // This is a hack to support aborting from any state.
        if (state_.isPrepared() || state_.isDecidedReplicated()) {
            state_.setDecided();
        } else {
            assert(!commit);
            state_.setAbort();
        }
        commit_ = commit;
    }

    bool isDone() const { return state_.isDone(); }

private:
    void enterPrepared(net::MessageServer* msg_server) {
        msg_server->send(connection_, last_fragment().response());

        if (!last_fragment().request().multiple_partitions) {
            // single partition! we are done
            state_.setSinglePartitionDone();
        }
    }

    net::ConnectionHandle* connection_;

    TransactionStateMachine state_;
    bool commit_;
};

PartitionServer::PartitionServer(Scheduler* scheduler, io::EventLoop* event_loop,
        net::MessageServer* msg_server, replication::FaultTolerantLog* log) :

        scheduler_(scheduler) , event_loop_(event_loop), msg_server_(msg_server), log_(log) {
    assert(scheduler_ != NULL);
    msg_server_->addCallback(&PartitionServer::fragmentReceived, this);
    msg_server_->addCallback(&PartitionServer::decisionReceived, this);
    event_loop_->addIdleCallback(&idleCallback, this);
    log_->entry_callback(this);
}

PartitionServer::~PartitionServer() {
    // Unregister message callbacks
    msg_server_->removeCallback(&PartitionServer::fragmentReceived);
    msg_server_->removeCallback(&PartitionServer::decisionReceived);

    // Unregister the idle callback so it does not get called if the event loop is still used
    event_loop_->removeIdleCallback(&idleCallback);
}

void PartitionServer::fragmentReceived(
        net::ConnectionHandle* connection, const dtxn::Fragment& fragment) {
    //~ printf("fragment %d %d\n", fragment.id, fragment.last_fragment);
    // TODO: Fix this hack! We need real client ids
    const_cast<Fragment&>(fragment).client_id = (int32_t) reinterpret_cast<intptr_t>(connection);

    // Look up or create the transaction state; queue work in the scheduler
    ServerTransactionState* transaction = findOrCreateTransaction(connection, fragment);
    //~ assert(!transaction->has_fragments() || transaction->last_fragment().response().status == ExecutionEngine::OK);
    transaction->addFragment(fragment);
    scheduler_->fragmentArrived(transaction);
}

void PartitionServer::decisionReceived(
        net::ConnectionHandle* connection, const dtxn::CommitDecision& decision) {
    //~ printf("decision %d %d\n", decision.id, decision.commit);
    // TODO: Fix this hack! We need real client ids
    const_cast<CommitDecision&>(decision).client_id =
            (int32_t) reinterpret_cast<intptr_t>(connection);

    ServerTransactionState* transaction = findTransaction(decision);
    // TODO: if this is an abort for a transaction in the "to execute" queue, we need to abort it
    assert(!decision.commit ||
            (transaction->last_fragment().request().last_fragment &&
            transaction->last_fragment().response().status != ExecutionEngine::INVALID));
    transaction->decide(decision.commit);
    scheduler_->decide(transaction, decision.commit, decision.payload);
    if (transaction->last_fragment().request().last_fragment) {
        log_->submit(decision, transaction);
    } else if (transaction->isDone()) {
        assert(!decision.commit);
        cleanUpTransaction(transaction);
    }
}

void PartitionServer::nextLogEntry(int sequence, const std::string& entry, void* argument) {
    // TODO: This is a disgusting hack that breaks whenever we change the messages. Fix.
    //~ static const int COMMIT_DECISION_SERIALIZED_SIZE = 9;

    if (log_->isPrimary()) {
        ServerTransactionState* transaction = (ServerTransactionState*) argument;
        transaction->setReplicated(msg_server_);
        if (transaction->isDone()) {
            // TODO: We should ACK this to the coordinator, so it can clean up commit records
            cleanUpTransaction(transaction);
        }
    } else {
        assert(false);
    }
        //~ transaction->setReplicated();
        //~ if (!transaction->isDecided()) {
            //~ // Send the result of the final fragment back to the coordinator
            //~ if (transaction->readyToSend()) {
                //~ sendResponse(transaction->mutable_last());
            //~ }
        //~ } else {
            //~ assert(transaction->readyToSend());
            //~ transaction->setDoneRound();
            //~ assert(transaction->isDone());
            //~ // TODO: We need to ACK decisions so the coordinator can forget about the transaction
            //~ ServerTransactionState* transaction = (ServerTransactionState*) argument;
            //~ if (transaction->isDone()) {
                //~ cleanUpTransaction(transaction);
            //~ }
        //~ }
    //~ } else {
        //~ assert(argument == NULL);
        //~ if (serialized_entry.size() > COMMIT_DECISION_SERIALIZED_SIZE) {
            //~ // Transaction fragment: process it
            //~ // TODO: Recycle?
            //~ Fragment request;
            //~ request.parseFromString(serialized_entry);
            //~ ServerTransactionState* transaction = findOrCreateTransaction(NULL, request);
            //~ FragmentState* fragment = transaction->addFragment(request);
            //~ fragment->setReplicated();
            //~ execute_queue_.add()->set(fragment);
        //~ } else {
            //~ // Decisions
            //~ CommitDecision decision;
            //~ decision.parseFromString(serialized_entry);
            //~ ServerTransactionState* transaction = findTransaction(decision);
            //~ transaction->decide(decision.commit);
            //~ transaction->decisionReplicated();
            //~ execute_queue_.add()->set(transaction);
        //~ }
    //~ }
}

void PartitionServer::replicate(TransactionState* transaction) {
    assert(transaction->last_fragment().request().last_fragment);
    log_->submit(transaction->last_fragment().request(), transaction);
}

void PartitionServer::executed(FragmentState* fragment) {
    assert(fragment->response().status == ExecutionEngine::OK ||
           fragment->response().status == ExecutionEngine::ABORT_USER ||
           fragment->response().status == ExecutionEngine::ABORT_MISPREDICT);
    ServerTransactionState* transaction = (ServerTransactionState*) fragment->transaction();
    transaction->setExecuted(fragment, msg_server_);
    if (transaction->isDone()) {
        cleanUpTransaction(transaction);
    }
}

bool PartitionServer::idle() {
    //~ printf("idle\n");
    // Tell the scheduler to do work
    return scheduler_->doWork(this);
}

bool PartitionServer::idleCallback(void* argument) {
    PartitionServer* server = reinterpret_cast<PartitionServer*>(argument);
    return server->idle();
}

static int64_t makeTxnKey(int32_t client_id, int32_t id) {
    return (int64_t) client_id << 32 | (int64_t) id;
}

ServerTransactionState* PartitionServer::findTransaction(const CommitDecision& decision) {
    int64_t key = makeTxnKey(decision.client_id, decision.id);
    TransactionIdMap::iterator it = transaction_id_map_.find(key);
    assert(it != transaction_id_map_.end());
    return it->second;
}

void PartitionServer::cleanUpTransaction(ServerTransactionState* transaction) {
    //~ printf("clean up id %p %d\n", transaction, 
    int64_t key = makeTxnKey(transaction->last_fragment().request().client_id,
            transaction->last_fragment().request().id);
    size_t count = transaction_id_map_.erase(key);
    ASSERT(count == 1);
    delete transaction;
}

ServerTransactionState* PartitionServer::findOrCreateTransaction(
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
        // This must be another message for an in-progress multiple partition transaction
        assert(result.first->second != NULL);
        assert(result.first->second->last_fragment().request().multiple_partitions);
    }

    return result.first->second;
}

}  // namespace dtxn2
