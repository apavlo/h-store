// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN2_TRANSACTIONSTATE_H__
#define DTXN2_TRANSACTIONSTATE_H__

#include <vector>

#include "base/stlutil.h"
#include "dtxn/messages.h"

namespace dtxn2 {

class TransactionState;

class FragmentState {
public:
    FragmentState(TransactionState* transaction, const dtxn::Fragment& fragment) :
            transaction_(transaction), request_(fragment) {
        response_.id = request_.id;
    }

    const dtxn::Fragment& request() const { return request_; }
    const dtxn::FragmentResponse& response() const { return response_; }
    dtxn::FragmentResponse* mutable_response() { return &response_; }
    TransactionState* transaction() { return transaction_; }

private:
    TransactionState* transaction_;
    dtxn::Fragment request_;
    dtxn::FragmentResponse response_;
};

class TransactionState {
public:
    TransactionState() : scheduler_state_(NULL) {}

    ~TransactionState() {
        STLDeleteElements(&fragments_);
    }

    const FragmentState& last_fragment() const { return *fragments_.back(); }
    FragmentState* mutable_last() { return fragments_.back(); }

    void* scheduler_state() {
        return scheduler_state_;
    }

    void scheduler_state(void* state) {
        scheduler_state_ = state;
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

protected:
    void addFragment(const dtxn::Fragment& fragment) {
        assert(fragments_.empty() || !fragments_.back()->request().last_fragment);
        fragments_.push_back(new FragmentState(this, fragment));
    }

private:
    std::vector<FragmentState*> fragments_;
    void* scheduler_state_;

    // PAVLO: Let things attach payload data that we can send around during the finish process
    std::string payload_;
};

}  // namespace dtxn
#endif
