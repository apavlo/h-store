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

/* DtxnManager implementation that strictly orders all transactions.
This approach prevents deadlocks by being "pessimistic" about transactions. The policy is:

- distributed transactions implicitly "lock" all partitions.
- partitions are "unlocked" when a transaction marks them as "done"
- a round of a transaction is sent out when all partitions that need to receive messages are
    done their previous transaction (unlocked). this avoids starting a transaction at a partition
    until all participants can execute it, which should reduce the blocking time.

Thus, when a distributed transaction is sent to a partition, all transactions before it are known
to be "done" at this partition. This permits speculation while reducing blocking and preventing
deadlocks.
*/
class OrderedDtxnManager : public DtxnManager {
public:
    // Owns partitions. Does not own event_loop or msg_server.
    OrderedDtxnManager(io::EventLoop* event_loop, net::MessageServer* msg_server,
            const std::vector<net::ConnectionHandle*>& partitions);

    virtual ~OrderedDtxnManager();

    virtual void execute(DistributedTransaction* transaction,
            const std::tr1::function<void()>& callback);

    virtual void finish(DistributedTransaction* transaction, bool commit, const std::string& payload,
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

    // Begin the next round when all fragments have been received and dependencies are resolved.
    void nextRound(TransactionState* transaction);

    // Check if a transaction can be started. If so, send out the fragments for it.
    void trySendRound(TransactionState* transaction);

    // Advances the partition_next_send_txn_id_ field for partition_index. The current transaction
    // being processed must be current_send_id.
    void advanceSendId(int partition_index, int current_send_id);

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

    // Records the next transaction id that may want to use this partition. Any txn id <= this
    // value has either already sent a chunk of work to the partition and has thus reserved its
    // place in the order, or has declared that it will not use this partition. Thus, a txn with
    // id = this value is safe to be sent out.
    std::vector<int> partition_next_send_txn_id_;

    typedef base::SlidingArray<TransactionState*> QueueType;
    QueueType queue_;

    io::EventLoop* event_loop_;
    net::MessageServer* msg_server_;
};

}  // namespace dtxn
#endif
