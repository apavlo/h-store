// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_LOCKS_LOCKINGCOUNTER_H__
#define DTXN_LOCKS_LOCKINGCOUNTER_H__

#include <cstring>
#include <string>

#include "base/array.h"
#include "dtxn/executionengine.h"
#include "dtxn/locks/lock.h"
#include "dtxn/locks/locktransaction.h"

namespace dtxn {

static const std::string OP_ADD("a");
static const std::string OP_GET("b");
static const std::string OP_ABORT("c");

/** Defines a counter with a lock. Used for testing. */
class LockingCounter : public ExecutionEngine {
public:
    LockingCounter() : execute_count_(0), undo_allocated_(0), undo_count_(0), free_count_(0),
            last_with_locks_(false), counter_(1) {}

    virtual ~LockingCounter() {}

    virtual Status tryExecute(const std::string& work_unit, std::string* output, void** undo_state,
            Transaction* transaction){
        last_with_locks_ = transaction != NULL;
        execute_count_ += 1;
        if (work_unit == OP_ADD) {
            if (transaction != NULL && !transaction->tryWriteLock(&lock_)) return BLOCKED;
            // Save the value of this counter if it has not already been saved so we can undo later
            if (undo_state != NULL && *undo_state == 0) {
                undo_allocated_ += 1;
                *undo_state = reinterpret_cast<void*>(counter_);
            }

            counter_ += 1;
        } else if (work_unit == OP_GET) {
            if (transaction != NULL && !transaction->tryReadLock(&lock_)) return BLOCKED;

            // Nothing to do: we read the value below
        } else if (work_unit == OP_ABORT) {
            // No output from an abort
            return ABORT_USER;
        } else {
            assert(false);
            abort();
        }
        output->assign(reinterpret_cast<char*>(&counter_), sizeof(counter_));

        return OK;
    }

    virtual bool supports_locks() const { return true; }

    void releaseUndo(void* undo_state) {
        assert(undo_state != NULL);
        undo_allocated_ -= 1;
        assert(undo_allocated_ >= 0);
    }

    virtual void freeUndo(void* undo_state) {
        releaseUndo(undo_state);
        free_count_ += 1;
    }

    virtual void applyUndo(void* undo_state) {
        releaseUndo(undo_state);
        if (undo_state != 0) {
            counter_ = reinterpret_cast<intptr_t>(undo_state);
        }
        undo_count_ += 1;
    }

    static intptr_t toValue(const std::string& output) {
        intptr_t result;
        assert(sizeof(result) == output.size());
        memcpy(&result, output.data(), sizeof(result));
        return result;
    }

    intptr_t counter() const { return counter_; }

    int execute_count_;
    int undo_allocated_;
    int undo_count_;
    int free_count_;
    // True if the last work unit executed with locks
    bool last_with_locks_;

private:
    intptr_t counter_;
    Lock lock_;
};

class MultiLockingCounter : public ExecutionEngine {
public:
    MultiLockingCounter(int size) : counters_(size) {
        assert(size > 0);
        assert(counters_.size() == size);
    }

    virtual ~MultiLockingCounter() {}

    virtual Status tryExecute(
            const std::string& work_unit,
            std::string* result,
            void** undo_state,
            Transaction* transaction) {
        void** undo_states = *(void***) undo_state;
        if (undo_states == NULL) {
            // C++-ism to default initialize the array (0 or nullptr)
            undo_states = new void*[counters_.size()]();
            *undo_state = undo_states;
        }

        // Split the work into index and operation
        int index = static_cast<int>(work_unit.at(0));
        std::string operation(work_unit, 1);
        assert(makeWork(index, operation) == work_unit);

        return counters_[index].tryExecute(operation, result, &undo_states[index], transaction);
    }

    virtual bool supports_locks() const { return true; }

    virtual void freeUndo(void* undo_state) {
        void** undo_states = (void**) undo_state;
        for (int i = 0; i < counters_.size(); ++i) {
            if (undo_states[i] != NULL) counters_[i].freeUndo(undo_states[i]);
        }
        delete[] undo_states;
    }

    virtual void applyUndo(void* undo_state) {
        void** undo_states = (void**) undo_state;
        for (int i = 0; i < counters_.size(); ++i) {
            if (undo_states[i] != NULL) counters_[i].applyUndo(undo_states[i]);
        }
        delete[] undo_states;
    }

    static std::string makeWork(int index, const std::string& operation) {
        std::string output;
        output.push_back(static_cast<char>(index));
        output.append(operation);
        return output;
    }

    const LockingCounter& counter(int index) const { return counters_[index]; }

private:
    base::Array<LockingCounter> counters_;
};

}  // namespace dtxn

#endif
