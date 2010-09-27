// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_ORDEREDDISTRIBUTOR_H
#define DTXN_ORDEREDDISTRIBUTOR_H

#include <vector>

#include "base/slidingarray.h"
#include "dtxn/dtxndistributor.h"

namespace io {
class EventLoop;
}

namespace net {
class ConnectionHandle;
class MessageServer;
}

namespace dtxn {

class Coordinator;
class Fragment;
class FragmentResponse;
class Partition;

class OrderedDistributor : public TransactionManager {
public:
    // Owns partitions. Does not delete coordinator, event_loop or msg_server.
    OrderedDistributor(const std::vector<net::ConnectionHandle*>& partitions,
            Coordinator* coordinator, io::EventLoop* event_loop, net::MessageServer* msg_server);

    virtual ~OrderedDistributor();

    virtual void sendRound(CoordinatedTransaction* transaction);

    // INTERNAL: request has been received from connection.
    void requestReceived(net::ConnectionHandle* connection, const Fragment& request);

    // INTERNAL: response has been received from connection.
    void responseReceived(net::ConnectionHandle* connection, const FragmentResponse& response);

    io::EventLoop* event_loop() { return event_loop_; }

private:
    class TransactionState;

    // Called when we have not received all the responses for transaction.
    void responseTimeout(TransactionState* transaction);

    void dispatchTransaction(TransactionState* transaction);

    // Begin the next round when all fragments have been received and dependencies are resolved.
    void nextRound(TransactionState* transaction);

    // Processes the round of messages returned by the coordinator.
    void processFragments(TransactionState* transaction);

    // Sends the decision to all partitions and sends the response to the client
    void finishTransaction(TransactionState* transaction);

    // Recursively removes dependencies for partition_id. The dependency on transaction_id is
    // removed from transaction. If one exists, it will recursively try to remove the dependency
    // from other transactions.
    bool removeDependency(TransactionState* transaction, int transaction_id, int partition_id);

    std::vector<net::ConnectionHandle*> partitions_;
    // Records the last commit id for a partition. Used for dependency tracking.
    std::vector<int> last_partition_commit_;
    Coordinator* coordinator_;

    typedef base::SlidingArray<TransactionState*> QueueType;
    QueueType queue_;

    io::EventLoop* event_loop_;
    net::MessageServer* msg_server_;
};

}  // namespace dtxn

#endif
