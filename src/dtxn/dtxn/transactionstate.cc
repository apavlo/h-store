// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/transactionstate.h"

#include "base/stlutil.h"
#include "dtxn/executionengine.h"

namespace dtxn {

TransactionState::~TransactionState() {
    STLDeleteElements(&fragments_);
}

FragmentState* TransactionState::addFragment(const Fragment& fragment) {
    assert(fragment.last_fragment || fragment.multiple_partitions);
    assert(fragments_.empty() || fragment.client_id == fragments_.back()->request().client_id);
    assert(fragments_.empty() || fragment.id == fragments_.back()->request().id);
    assert(fragments_.empty() ||
            //~ (fragment.multiple_partitions && fragments_.back()->request().multiple_partitions));
            fragments_.back()->request().multiple_partitions);
    assert(fragments_.empty() || !fragments_.back()->request().last_fragment);
    // cannot add to an aborted transaction. invalid is possible on backups
    assert(fragments_.empty() || 
            (fragments_.back()->response().status == ExecutionEngine::OK ||
            fragments_.back()->response().status == ExecutionEngine::INVALID));

    // TODO: Avoid copy somehow?
    FragmentState* frag = new FragmentState(this, fragment);
    fragments_.push_back(frag);
    return frag;
}

void TransactionState::clear() {
    STLDeleteElements(&fragments_);
    undo_ = NULL;
    decision_state_.clear();
}

bool TransactionState::finished() const {
    if (!last_fragment().multiple_partitions()) {
        return last_fragment().readyToSend();
    }

    // multi-partition: must be decided and finished executing
    return decision_state_.done() && last_fragment().readyToSend();
}

}  // namespace dtxn
