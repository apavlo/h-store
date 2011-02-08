// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_TRANSACTIONSTATE_H__
#define DTXN_TRANSACTIONSTATE_H__

#include <vector>

#include "dtxn/messages.h"

namespace dtxn {

class TransactionState;

class ReplicationStateMachine {
public:
    ReplicationStateMachine() { clear(); }

    // Resets the state machine to its initial state.
    void clear() { state_ = START; }

    void setReplicated() {
        if (state_ == START) {
            state_ = REPLICATED;
        } else {
            assert(state_ == EXECUTED);
            state_ = DONE;
        }
    }

    void setExecuted() {
        if (state_ == START) {
            state_ = EXECUTED;
        } else {
            assert(state_ == REPLICATED);
            state_ = DONE;
        }
    }

    bool done() const { return state_ == DONE; }
    bool initial_state() const { return state_ == START; }
    bool isExecuted() const { return state_ == EXECUTED || state_ == DONE; }

private:
    enum {
        START,
        REPLICATED,
        EXECUTED,
        DONE
    } state_;
};

class FragmentState {
public:
    // TODO: Avoid the copy of the fragment somehow?
    FragmentState(TransactionState* transaction, const Fragment& fragment)
            : transaction_(transaction), fragment_(fragment) {
        response_.id = fragment_.id;
    }

    const Fragment& request() const { return fragment_; }
    const FragmentResponse& response() const { return response_; }
    FragmentResponse* mutable_response() { return &response_; }
    TransactionState* transaction() { return transaction_; }

    bool multiple_partitions() const { return fragment_.multiple_partitions; }

    void setReplicated() { state_.setReplicated(); }
    void setHasResults() {
        // This can be called more than once in case of a dependency abort that gets re-executed.
        if (state_.isExecuted()) {
            assert(request().multiple_partitions);
        } else {
            state_.setExecuted();
        }
    }
    
    bool readyToSend() const { return state_.done(); }

private:
    TransactionState* transaction_;
    Fragment fragment_;
    FragmentResponse response_;

    ReplicationStateMachine state_;
};

/** Represents the state machine for a transaction. */
class TransactionState {
public:
    TransactionState() : undo_(NULL), commit_(false) {}
    ~TransactionState();

    FragmentState* addFragment(const Fragment& fragment);
    const FragmentState& last_fragment() const { return *fragments_.back(); }
    FragmentState* mutable_last() { return fragments_.back(); }
    bool has_fragments() const { return !fragments_.empty(); }

    void** undo() { return &undo_; }

    // Reset the transaction to empty.
    void clear();

    void decide(bool commit) {
        assert(decision_state_.initial_state());
        commit_ = commit;
    }
    bool decision() const { return commit_; }
    void decisionExecuted() { decision_state_.setExecuted(); }
    void decisionReplicated() { decision_state_.setReplicated(); }

    int32_t client_id() const { return last_fragment().request().client_id; }
    int32_t id() const { return last_fragment().request().id; }

    bool finished() const;

    const std::vector<FragmentState*>& fragments() const {
        return fragments_;
    }
    
    // PAVLO
    bool has_payload() const { return !payload_.empty(); }
    void set_payload(const std::string& payload) {
        assert(!payload.empty());
        payload_ = payload;
    }
    const std::string& payload() const {
        assert(!payload_.empty());
        return payload_;
	}

private:
    std::vector<FragmentState*> fragments_;

    // Scheduler/execution engine uses this to track the undo buffer for a transaction.
    void* undo_;

    ReplicationStateMachine decision_state_;
    bool commit_;

    // PAVLO: Let things attach payload data that we can send around during the finish process
    std::string payload_;
};

}  // namespace dtxn
#endif
