// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn2/common.h"

#include "dtxn/configparser.h"
#include "dtxn2/partitionserver.h"
#include "dtxn2/protopartitionserver.h"
#include "io/libeventloop.h"
#include "net/messageconnectionutil.h"
#include "net/messageserver.h"
#include "protorpc/protoserver.h"
#include "replication/nulllog.h"
#include "replication/primarybackuplistener.h"
#include "base/debuglog.h"

using std::string;
using std::vector;

namespace dtxn2 {

void runServer(io::EventLoop* event_loop, Scheduler* scheduler, const dtxn::Partition& partition,
        int replica_index) {
    assert(0 <= replica_index && replica_index < partition.numReplicas());

    net::MessageServer msg_server;

    // Create a replicated group, if needed
    replication::PrimaryBackupListener* replicated_listener = NULL;
    replication::NullLog null_log;
    replication::FaultTolerantLog* log = NULL;
    if (partition.numReplicas() > 1) {
        replicated_listener = new replication::PrimaryBackupListener(event_loop, &msg_server);
        log = replicated_listener->replica();
        replicated_listener->buffered(true);
        if (replica_index == 0) {
            replicated_listener->setPrimary(
                    net::createConnections(event_loop, partition.backups()));
        } else {
            replicated_listener->setBackup();
        }
    } else {
        log = &null_log;
    }

    // Create the server
    PartitionServer replica(scheduler, event_loop, &msg_server, log);

    MessageListener listener(((io::LibEventLoop*) event_loop)->base(), &msg_server);
    if (!listener.listen(partition.replica(replica_index).port())) {
        // TODO: How to deal with errors?
        LOG_ERROR("Error binding to port %d", partition.replica(replica_index).port());
        return;
    }
    LOG_DEBUG("Listening on port %d", listener.port());

    // TODO: Eventually convert to this RPC protocol
    int proto_port = partition.replica(replica_index).port() + 1;
    ProtoPartitionServer proto_partition(&replica, &msg_server);
    protorpc::ProtoServer proto_server(event_loop);
    proto_server.registerService(&proto_partition);
    proto_server.listen(proto_port);

    // Exit when SIGINT is caught
    ((io::LibEventLoop*) event_loop)->exitOnSigInt(true);

    // Run the event loop
    event_loop->run();

    delete replicated_listener;
}

}  // namespace dtxn2
