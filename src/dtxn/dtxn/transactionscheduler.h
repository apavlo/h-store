// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_TRANSACTIONSCHEDULER_H__
#define DTXN_TRANSACTIONSCHEDULER_H__

#include <string>
#include <vector>

namespace dtxn {

class Transaction;

/** Interface for scheduling the execution of work units. It gives work units to the execution
engine in an order that guarantees serializability. It can achieve this through different
mechanisms, depending on the specific implementation. */
class TransactionScheduler {
public:
    virtual ~TransactionScheduler() {}

    /** Returns an object that contains the state of a new transaction.
    @arg multiple_partitions true if this is a multi-partition transaction.
    */
    virtual Transaction* begin(void* external_state, const std::string& initial_work_unit,
            bool last_fragment, bool multiple_partitions) = 0;

    /** Indicates that transaction has a new work unit to be executed. */
    virtual void workUnitAdded(Transaction* transaction) = 0;

    /** Commit transaction. The transaction must not be blocked. */
    virtual void commit(Transaction* transaction) = 0;

    /** Abort transaction, causing its effects to be undone. It can be in any state. */
    virtual void abort(Transaction* transaction) = 0;

    /** Returns a transaction with a completed work unit, if one exists. */
    virtual Transaction* getWorkUnitResult() = 0;
 
    /** Called when there are no new messages. Returns true if the network should be polled for
    more events, which will call idle() again very soon. Returns false if the network should
    block. This is used to prioritize multi-partition transactions over single partition
    transactions. The locking implementation uses this to detect deadlock. */
    virtual bool idle() = 0;

    /** work_units should be directly applied as a single transaction to the execution engine.
    It must be directly executed without concurrency control. It can also be applied without undo,
    as the primary has executed it already. This is used to roll forward from the log.
    TODO: Should the implementation be in dtxnserver.cc? Seems like they will all be identical?
    TODO: What about the "replicate before execution" policy? We might need to undo. */    
    virtual void backupApplyTransaction(const std::vector<std::string>& work_units) = 0;
};

}  // namespace dtxn

#endif
