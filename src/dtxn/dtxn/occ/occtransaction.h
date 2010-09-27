// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_OCC_OCCTRANSACTION_H__
#define DTXN_OCC_OCCTRANSACTION_H__

#include "base/unordered_map.h"
#include "dtxn/transaction.h"

namespace dtxn {

class OCCTransaction : public Transaction {
public:
    OCCTransaction(void* external_state, const std::string& initial_work_unit,
            bool last_fragment, bool multiple_partitions) :
            Transaction(external_state, initial_work_unit, last_fragment, multiple_partitions) {}
    virtual ~OCCTransaction() {}

    virtual bool tryReadLock(Lock* lock) {
        // Ignore return value: don't care if it is already in the set
        read_write_set_.insert(std::make_pair(lock, false));
        return true;
    }

    virtual bool tryWriteLock(Lock* lock) {
        // Replace any existing value with true
        read_write_set_[lock] = true;
        return true;
    }

    /** Returns true if these two transactions can be reordered. This is reflexive. */
    bool canReorder(const OCCTransaction& other) const {
        // We can't change the relative order of unfinished transactions because they could
        // read/write anything in the future!
        assert(last_fragment() && other.last_fragment());

        // Intersect the two sets looking for read/write and write/write conflicts
        // read -> write: a read something, b overwrote it. Can't reorder.
        // write -> read: a wrote something, b read it. Can't reorderd.
        // write -> write: a wrote something, b overwrote it. Can't reorder.
        ReadWriteType::const_iterator i = read_write_set_.begin();
        for (; i != read_write_set_.end(); ++i) {
            ReadWriteType::const_iterator o = other.read_write_set_.find(i->first);
            if (o != other.read_write_set_.end() && (i->second || o->second)) {
                // we only need to find one conflict!
                return false;
            }
        }
        return true;
    }

    /** Returns true if this transaction depends on other. This assumes that other was executed
    before this transaction. If true, then if other is aborted, this must also be aborted. */
    bool dependsOn(const OCCTransaction& other) const {
        assert(last_fragment() && other.last_fragment());

        // Intersect the two sets looking for write -> read and write -> write conflicts
        // read -> write is not a conflict, because the earlier transaction did not modify anything
        ReadWriteType::const_iterator o = other.read_write_set_.begin();
        for (; o != read_write_set_.end(); ++o) {
            if (o->second) {
                // only check writes: earlier reads don't matter
                ReadWriteType::const_iterator i = read_write_set_.find(o->first);
                if (i != read_write_set_.end()) {
                    // we only need to find one conflict
                    return true;
                }
            }
        }
        return false;
    }

private:
    typedef base::unordered_map<Lock*, bool> ReadWriteType;

    // Value is true if it is written, false if only read
    ReadWriteType read_write_set_;
};

}  // namespace dtxn
#endif
