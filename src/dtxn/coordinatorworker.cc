// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "coordinatorworker.h"

#include "dtxn/configparser.h"
#include "dtxn/dtxndistributor.h"
#include "dtxn/messages.h"
#include "dtxn/ordered/ordereddistributor.h"
#include "io/libeventloop.h"
#include "libevent/include/event2/event.h"
#include "messageconnection.h"
#include "net/messageconnectionutil.h"
#include "net/messageserver.h"

class CoordinatorWorker::MessageConnectionProxy : public MessageConnection {
public:
    MessageConnectionProxy(CoordinatorWorker* worker) : worker_(worker) {}

protected:
    virtual bool flush() {
        const void* read;
        int length;
        out_buffer_.readBuffer(&read, &length);
        worker_->response(read, length);

        out_buffer_.readBuffer(&read, &length);
        CHECK(length == 0);
        return true;
    }

private:
    CoordinatorWorker* worker_;
};

CoordinatorWorker::CoordinatorWorker(
        const std::vector<dtxn::Partition>& partitions, dtxn::Coordinator* coordinator) :

        connection_(new MessageConnectionProxy(this)),
        event_loop_(new io::LibEventLoop()),
        msg_server_(new net::MessageServer()),
        coordinator_(coordinator) {
    assert(!partitions.empty());
    assert(coordinator_ != NULL);

    std::vector<net::ConnectionHandle*> connections = msg_server_->addConnections(
            net::createConnections(event_loop_, dtxn::primaryAddresses(partitions)));
    distributor_ = new dtxn::OrderedDistributor(connections, coordinator_, event_loop_, msg_server_);
    // MessageServer owns connections, so connection is a "proxy" to this
    connection_handle_ = msg_server_->addConnection(connection_);
}

CoordinatorWorker::~CoordinatorWorker() {
    delete coordinator_;
    delete distributor_;
    delete msg_server_;
    delete event_loop_;
}

void CoordinatorWorker::requestReceived(const dtxn::Fragment& request) {
    distributor_->requestReceived(connection_handle_, request);
}
