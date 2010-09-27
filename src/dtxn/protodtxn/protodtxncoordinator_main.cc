// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/configparser.h"
#include "io/libeventloop.h"
#include "net/messageconnectionutil.h"
#include "net/messageserver.h"
#include "dtxn/ordered/ordereddtxnmanager.h"
#include "protodtxn/protodtxncoordinator.h"
#include "protorpc/protoserver.h"

using std::vector;

int main(int argc, const char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "protodtxncoordinator [listen port] [configuration file]\n");
        return 1;
    }
    int port = atoi(argv[1]);
    assert(port != 0);
    const char* const config_file = argv[2];

    vector<dtxn::Partition> partitions = dtxn::parseConfigurationFromPath(config_file);

    // Create the manager to serve DTXN stuff
    io::LibEventLoop event_loop;

    net::MessageServer msg_server;
    vector<net::ConnectionHandle*> connections = msg_server.addConnections(
            net::createConnections(&event_loop, primaryAddresses(partitions)));
    dtxn::OrderedDtxnManager manager(&event_loop, &msg_server, connections);
    protodtxn::ProtoDtxnCoordinator coordinator(&manager, (int) partitions.size());

    // Serve the coordinator via ProtoRPC
    protorpc::ProtoServer rpc_server(&event_loop);
    rpc_server.registerService(&coordinator);
    rpc_server.listen(port);

    event_loop.exitOnSigInt(true);
    event_loop.run();

    return 0;
}
