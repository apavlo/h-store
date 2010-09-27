// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/common.h"

#include "dtxn/configparser.h"
#include "dtxn/dtxnserver.h"
#include "dtxn/ordered/ordereddistributor.h"
#include "io/libeventloop.h"
#include "net/messageconnectionutil.h"
#include "net/messageserver.h"
#include "replication/primarybackuplistener.h"
#include "strings/utils.h"

using std::string;
using std::vector;

namespace dtxn {

void runServer(io::EventLoop* event_loop, Scheduler* scheduler, const Partition& partition,
        int replica_index) {
    assert(0 <= replica_index && replica_index < partition.numReplicas());

    net::MessageServer msg_server;

    // Create a replicated group, if needed
    replication::PrimaryBackupListener* replicated_listener = NULL;
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
    }

    // Create the server
    DtxnServer replica(scheduler, event_loop, &msg_server, log);

    // Allocated dynamically so we can destroy it before event_loop
    MessageListener listener(((io::LibEventLoop*) event_loop)->base(), &msg_server);
    if (!listener.listen(partition.replica(replica_index).port())) {
        // TODO: How to deal with errors?
        fprintf(stderr, "Error binding to port\n");
        return;
    }
    printf("listening on port %d\n", listener.port());

    // Exit when SIGINT is caught
    ((io::LibEventLoop*) event_loop)->exitOnSigInt(true);

    // Run the event loop
    event_loop->run();

    delete replicated_listener;
}


void runDistributor(Coordinator* coordinator, int port, const char* configuration_file) {
    vector<Partition> partitions = parseConfigurationFromPath(configuration_file);

    io::LibEventLoop event_loop;
    net::MessageServer msg_server;
    vector<net::ConnectionHandle*> connections = msg_server.addConnections(
            net::createConnections(&event_loop, primaryAddresses(partitions)));
    OrderedDistributor master(connections, coordinator, &event_loop, &msg_server);

    MessageListener listener(event_loop.base(), &msg_server);
    if (!listener.listen(port)) {
        fprintf(stderr, "Error binding to port\n");
        // TODO: How to handle errors in the "main" routine?
        return;
    }
    printf("listening on port %d\n", listener.port());

    // Run the event loop
    event_loop.exitOnSigInt(true);
    event_loop.run();
}

}  // namespace dtxn
