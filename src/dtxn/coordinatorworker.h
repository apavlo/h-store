// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef COORDINATORWORKER_H__
#define COORDINATORWORKER_H__

#include <vector>

namespace dtxn {
class Coordinator;
class Fragment;
class FragmentResponse;
class OrderedDistributor;
class Partition;
}

namespace io {
class LibEventLoop;
}

namespace net {
class ConnectionHandle;
class MessageServer;
}

// Runs a distributor and a coordinator locally.
class CoordinatorWorker {
public:
    // Owns coordinator.
    CoordinatorWorker(
            const std::vector<dtxn::Partition>& partitions, dtxn::Coordinator* coordinator);
    virtual ~CoordinatorWorker();

protected:
    virtual void response(const void* buffer, int length) = 0;

    void requestReceived(const dtxn::Fragment& request);
    //~ dtxn::OrderedDistributor* distributor() { return distributor_; }
    //~ net::ConnectionHandle* connection_handle() { return connection_handle_; }
    io::LibEventLoop* event_loop() { return event_loop_; }

private:
    class MessageConnectionProxy;

    MessageConnectionProxy* connection_;
    net::ConnectionHandle* connection_handle_;

    io::LibEventLoop* event_loop_;
    net::MessageServer* msg_server_;

    dtxn::OrderedDistributor* distributor_;
    dtxn::Coordinator* coordinator_;
};

#endif
