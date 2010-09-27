// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_SCHEDULER_H__
#define DTXN_SCHEDULER_H__

#include <string>
#include <vector>

namespace dtxn {

class TransactionState;
class FragmentState;

/** Interface for scheduling the execution of work units. It gives work units to the execution
engine in an order that guarantees serializability. It can achieve this through different
mechanisms, depending on the specific implementation. */
class Scheduler {
public:
    virtual ~Scheduler() {}

    // Queue transaction fragment belonging to transaction.
    virtual void fragmentArrived(FragmentState* fragment) = 0;

    // Fill queue with transactions with fragments to be replicated and executed.
    virtual void order(std::vector<FragmentState*>* queue) = 0;

    /** A fragment that was returned by order is be executed. This may also be called to execute it
    at replicas. */
    virtual void execute(FragmentState* fragment) = 0;

    // Commit transaction. It must be a prepared multi-partition transaction. This makes the changes
    // permanent.
    virtual void commit(TransactionState* transaction) = 0;

    // Abort transaction. It must be a multi-partition transaction that may be in any state. The
    // writes performed by the transaction must be undone.
    virtual void abort(TransactionState* transaction) = 0;

    // Returns a fragment that has been executed.
    virtual FragmentState* executed() = 0;

    // Perform work at the bottom of the event loop. Return true if we should poll the network.
    // False will cause it to block.
    virtual bool idle() = 0;
};

}  // namespace dtxn

#endif
