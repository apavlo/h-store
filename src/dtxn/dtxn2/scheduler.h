// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN2_SCHEDULER_H__
#define DTXN2_SCHEDULER_H__

namespace dtxn2 {

class TransactionState;
class FragmentState;


/** Interface for indicating how to replicate transactions, and for returning results. */
class SchedulerOutput {
public:
    virtual ~SchedulerOutput() {}

    /** Start replicating transaction as the next transaction in the log. */
    virtual void replicate(TransactionState* transaction) = 0;

    /** Indicates that the last fragment in transaction has been replicated. */
    virtual void executed(FragmentState* fragment) = 0;
};


/** Interface for scheduling the execution of work units. It gives work units to the execution
engine in an order that guarantees serializability. It can achieve this through different
mechanisms, depending on the specific implementation. */
class Scheduler {
public:
    virtual ~Scheduler() {}

    // A work fragment for transaction has arrived.
    virtual void fragmentArrived(TransactionState* transaction) = 0;

    // Commit or abort transaction. It must be a multi-partition transaction. This makes
    // the changes permanent or undoes the effects.
    virtual void decide(TransactionState* transaction, bool commit, const std::string& payload) = 0;

    // The Scheduler should perform as much work as it decides, calling replicate() and executed()
    // to return results. When this returns, the server will block waiting for network input
    // unless doWork returns false, in which case it will poll the network.
    virtual bool doWork(SchedulerOutput* output) = 0;
};

}  // namespace dtxn2

#endif
