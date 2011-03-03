// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_TRANSACTION_H__
#define DTXN_TRANSACTION_H__

#include <cassert>
#include <cstdlib>
#include <vector>

#include "dtxn/executionengine.h"  // for ExecutionEngine::Status
#include "dtxn/messages.h"

namespace dtxn {

class Lock;

/** Abstract class used to represent a single transaction by the TransactionScheduler. */
class Transaction {
public:
    Transaction(void* external_state, const std::string& initial_work_unit, bool last_fragment,
            bool multiple_partitions) :

            last_fragment_(last_fragment),
            result_sent_(false),
            last_status_(ExecutionEngine::INVALID),
            undo_(NULL), external_state_(external_state) {
        log_entry_.fragments.push_back(initial_work_unit);
        log_entry_.multiple_partitions = multiple_partitions;
        // for sp TXNs, last_fragment_ must be true
        // TODO: Change this to an enum? We really have 3 types: SP, MP_NORMAL, MP_LAST
        assert(log_entry_.multiple_partitions || last_fragment_);
    }

    virtual ~Transaction() {}

    /** Acquires a read lock on Lock, if supported. */
    virtual bool tryReadLock(Lock* lock) = 0;

    /** Acquires a write lock on Lock, if supported. */
    virtual bool tryWriteLock(Lock* lock) = 0;

    /** Adds the next work unit to this transaction. */
    void addWorkUnit(const std::string& work_unit, bool last_fragment) {
        assert(multiple_partitions());
        assert(!last_fragment_);
        assert(last_status_ == ExecutionEngine::OK);
        assert(result_sent_);

        last_status_ = ExecutionEngine::INVALID;
        log_entry_.fragments.push_back(work_unit);
        last_fragment_ = last_fragment;
        result_sent_ = false;
    }

    void setWorkUnitStatus(ExecutionEngine::Status status) {
        assert(status != ExecutionEngine::INVALID);
        assert(status != ExecutionEngine::INVALID && (last_status_ == ExecutionEngine::INVALID ||
                last_status_ == ExecutionEngine::BLOCKED || status == ExecutionEngine::ABORT_USER));
        assert(!result_sent_ || status == ExecutionEngine::ABORT_USER);
        if (status == ExecutionEngine::ABORT_DEADLOCK) {
            // no output permitted for deadlock aborts
            assert(output_.empty());
        }
        last_status_ = status;
    }

    void resultSent() {
        assert(last_status_ == ExecutionEngine::OK ||
                last_status_ == ExecutionEngine::ABORT_USER ||
                last_status_ == ExecutionEngine::ABORT_DEADLOCK);
        assert(!result_sent_);
        if (last_status_ == ExecutionEngine::ABORT_DEADLOCK) {
            // no output permitted for deadlock aborts
            assert(output_.empty());
        }
        output_.clear();
        result_sent_ = true;
    }

    /** Marks that we are going to re-execute for this transaction. It clears the undo buffer and
    resets the status. */
    void prepareReExecute() {
        assert(last_status_ == ExecutionEngine::OK || last_status_ == ExecutionEngine::ABORT_USER);
        assert(!result_sent_);
        undo_ = NULL;
        output_.clear();
        last_status_ = ExecutionEngine::INVALID;
    }

    /** Gets the opaque state pointer for this transaction. */ 
    void* external_state() { return external_state_; }

    const std::string& last_work_unit() const { return log_entry_.fragments.back(); }
    const std::vector<std::string>& work_units() const { return log_entry_.fragments; }
    std::string* output() { return &output_; }
    void** undo() { return &undo_; }
    ExecutionEngine::Status last_status() const { return last_status_; }

    bool multiple_partitions() const { return log_entry_.multiple_partitions; }
    bool last_fragment() const { return last_fragment_; }
    bool result_sent() const { return result_sent_; }

    const LogEntry& log_entry() { return log_entry_; }

    // PAVLO
    bool has_payload() const { return !payload_.empty(); }
    void set_payload(const std::string& payload) {
        assert(!payload.empty());
        payload_ = payload;
    }
    const ::std::string& payload() const {
        assert(!payload_.empty());
        return payload_;
    }

private:
    // Stores the work units and multi-partition status. Work units are stored in order to
    // re-execute blocked transactions and for replicating transactions after they commit.
    dtxn::LogEntry log_entry_;

    bool last_fragment_;
    bool result_sent_;

    // Stores the last output used for re-executing blocked transactions.
    std::string output_;
    // The last status of executing a transaction.
    ExecutionEngine::Status last_status_;

    void* undo_;

    // Stores an arbitrary pointer for connecting the manager state to the external state.
    void* external_state_;
    
    // PAVLO: Let things attach payload data that we can send around during the finish process
    std::string payload_;
};

/** An implementation that does not support locks. */
class NoLockTransaction : public Transaction {
public:
    NoLockTransaction(void* external_state, const std::string& initial_work_unit,
            bool last_fragment, bool multiple_partitions) :
            Transaction(external_state, initial_work_unit, last_fragment, multiple_partitions) {}
    virtual ~NoLockTransaction() {}

    virtual bool tryReadLock(Lock* lock) { assert(false); abort(); }
    virtual bool tryWriteLock(Lock* lock) { assert(false); abort(); }
};

class MockLockTransaction : public dtxn::Transaction {
public:
    MockLockTransaction(void* external_state, const std::string& initial_work_unit,
            bool last_fragment, bool multiple_partitions) :
            Transaction(external_state, initial_work_unit, last_fragment, multiple_partitions),
            last_lock_(NULL) {}
    virtual ~MockLockTransaction() {}

    virtual bool tryReadLock(dtxn::Lock* lock) { last_lock_ = lock; return true; }
    virtual bool tryWriteLock(dtxn::Lock* lock) { last_lock_ = lock; return true; }

    dtxn::Lock* last_lock_;
};

}  // namespace dtxn

#endif
