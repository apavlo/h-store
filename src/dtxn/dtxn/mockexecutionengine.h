// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_MOCKEXECUTIONENGINE_H__
#define DTXN_MOCKEXECUTIONENGINE_H__

#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

#include "dtxn/executionengine.h"

namespace dtxn {

class MockExecutionEngine : public ExecutionEngine {
public:
    MockExecutionEngine() : execute_count_(0), commit_(true), undo_count_(0), free_count_(0),
            null_undo_(false) {}

    virtual Status tryExecute(const std::string& fragment, std::string* result, void** undo,
            Transaction* transaction) {
        assert(transaction == NULL);
        assert(result->empty());
        result->assign(result_);
        execute_count_ += 1;
        if (undo != NULL && !null_undo_) {
            if (*undo != NULL) {
                // this is an existing undo buffer: it must be on top
                assert(!undo_stack_.empty() && undo_stack_.back() == *undo);
                assert(*(int*) *undo == 0);
            } else {
                // We need to provide an undo buffer
                assert(*undo == NULL);
                undo_stack_.push_back(new int(0));
                *undo = undo_stack_.back();
            }
        }

        if (commit_) return OK;
        else return ABORT_USER;
    }

    virtual bool supports_locks() const { return false; }

    virtual void applyUndo(void* undo) {
        assert(undo == undo_stack_.back());
        delete undo_stack_.back();
        undo_stack_.pop_back();
        undo_count_ += 1;
    }

    virtual void freeUndo(void* undo) {
        std::vector<int*>::iterator i = std::find(undo_stack_.begin(), undo_stack_.end(), undo);
        assert(i != undo_stack_.end());

        delete *i;
        undo_stack_.erase(i);
        free_count_ += 1;
    }

    int execute_count_;
    bool commit_;
    int undo_count_;
    int free_count_;
    std::string result_;
    std::vector<int*> undo_stack_;

    // If true, it will not store anything in the undo pointer
    bool null_undo_;
};

}  // namespace dtxn

#endif
