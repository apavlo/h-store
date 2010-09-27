// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_ORDEREDDTXNMANAGER_H__
#define DTXN_ORDEREDDTXNMANAGER_H__

#include <vector>

#include "base/slidingarray.h"
#include "dtxn/dtxnmanager.h"

namespace io {
class EventLoop;
}

namespace net {
class ConnectionHandle;
class MessageServer;
}

namespace dtxn {

class FragmentResponse;

class OrderedDtxnManager : public DtxnManager {
public:
    // Owns partitions. Does not own event_loop or msg_server.
    OrderedDtxnManager(io::EventLoop* event_loop, net::MessageServer* msg_server,
            const std::vector<net::ConnectionHandle*>& partitions);

    virtual ~OrderedDtxnManager();

    virtual void execute(DistributedTransaction* transaction,
            const std::tr1::function<void()>& callback);

    virtual void finish(DistributedTransaction* transaction, bool commit,
            const std::tr1::function<void()>& callback);

    // INTERNAL: response has been received from connection.
    void responseReceived(net::ConnectionHandle* connection, const FragmentResponse& response);

    io::EventLoop* event_loop() { return event_loop_; }

private:
    class TransactionState;

    void verifyPrepareRound(DistributedTransaction* transaction,
            const std::tr1::function<void()>& callback);

    // Called when we have not received all the responses for transaction.
    void responseTimeout(TransactionState* transaction);

    // Start at the transaction after transaction_id, and see if any can be dispatched
    void unblockTransactions(int transaction_id);

    // Begin the next round when all fragments have been received and dependencies are resolved.
    void nextRound(TransactionState* transaction);

    // Sends the current round of messages.
    void sendFragments(TransactionState* transaction);

    // Sends the decision to all partitions and sends the response to the client
    void finishTransaction(TransactionState* transaction, bool commit);

    // Recursively removes dependencies for partition_id. The dependency on transaction_id is
    // removed from transaction. If one exists, it will recursively try to remove the dependency
    // from other transactions.
    bool removeDependency(TransactionState* transaction, int transaction_id, int partition_id);

    std::vector<net::ConnectionHandle*> partitions_;
    // Records the last commit id for a partition. Used for dependency tracking.
    std::vector<int> last_partition_commit_;

    typedef base::SlidingArray<TransactionState*> QueueType;
    QueueType queue_;

    // Transaction id of the first unfinished transaction. -1 if there is none
    int first_unfinished_id_;
    static const int NO_UNFINISHED_ID = -1;

    io::EventLoop* event_loop_;
    net::MessageServer* msg_server_;
};

}  // namespace dtxn
#endif
