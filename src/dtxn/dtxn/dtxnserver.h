// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_DTXNSERVER_H__
#define DTXN_DTXNSERVER_H__

#include <tr1/functional>  // for std::tr1::hash
#include <vector>

#include "base/cachedcircularbuffer.h"
#include "base/circularbuffer.h"
#include "base/slidingarray.h"
#include "base/unordered_map.h"
#include "replication/faulttolerantlog.h"

namespace io {
class EventLoop;
}

namespace net {
class ConnectionHandle;
class MessageServer;
}

namespace replication {
class FaultTolerantLog;
}

namespace dtxn {

class CommitDecision;
class Fragment;
class FragmentState;
class Scheduler;
class ServerTransactionState;

    
/** Provides the network interface to a single partition.

NOTE: This no longer manages client connections itself. The disadvantage is that we no longer
crash when a multi-partition transaction coordinator connection closes. We need to fix this with
timeouts in the future, plus the replicated 2PC log service.
*/
class DtxnServer : public replication::FaultTolerantLogCallback {
public:
    // Owns scheduler and log. Does not own event_loop, msg_server or log. log is optional.
    // TODO: Does this ownership policy make sense? Maybe this should own everything?
    DtxnServer(Scheduler* scheduler, io::EventLoop* event_loop,
            net::MessageServer* msg_server, replication::FaultTolerantLog* log);
    ~DtxnServer();

    // fragment arrives from connection. Used to deliver new transactions to this server.
    void fragmentReceived(net::ConnectionHandle* connection, const Fragment& fragment);

    // A commit/abort decision arrives from connection. A multi-partition transaction must be
    // active.
    void decisionReceived(net::ConnectionHandle* connection, const CommitDecision& decision);

    virtual void nextLogEntry(int sequence, const std::string& entry, void* argument);

private:
    // Sends the results of transaction back to the client.
    void sendResponse(FragmentState* fragment);
    void realSendResponse(FragmentState* fragment);

    // Checks if the scheduler has any results. If so, they get returned.
    void pollForResults();

    // Called when there are no more network events to process.
    bool idle();

    ServerTransactionState* findOrCreateTransaction(
            net::ConnectionHandle* handle, const Fragment& fragment);

    ServerTransactionState* findTransaction(const CommitDecision& decision);

    void cleanUpTransaction(ServerTransactionState* transaction);

    // Callback used by the event loop to notify that all network events have been processed.
    static bool idleCallback(void* argument);

    Scheduler* scheduler_;
    // TODO: We should be able to replace event_loop_ with msg_server_
    io::EventLoop* event_loop_;
    net::MessageServer* msg_server_;
    replication::FaultTolerantLog* log_;

    typedef base::unordered_map<int64_t, ServerTransactionState*> TransactionIdMap;
    TransactionIdMap transaction_id_map_;

    class QueuedWork;
    base::CachedCircularBuffer<QueuedWork> execute_queue_;
    CircularBuffer<FragmentState*> sp_pending_sends_;
};

}  // namespace dtxn

#endif
