// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <netinet/in.h>

#include "base/assert.h"
#include "dtxn/configparser.h"
#include "dtxn2/blockingscheduler.h"
#include "dtxn2/common.h"
#include "io/libeventloop.h"
#include "net/messageconnectionutil.h"
#include "protodtxn/dtxn.pb.h"
#include "protodtxn/protodtxnengine.h"
#include "protorpc/protorpcchannel.h"
#include "base/debuglog.h"

using std::vector;

int main(int argc, const char* argv[]) {
    if (argc != 5) {
        LOG_ERROR("protodtxnengine [engine address] [configuration file] [partition index] [replica index]");
        return 1;
    }
    const char* const address = argv[1];
    const char* const config_file = argv[2];
    int partition_index = atoi(argv[3]);
    int replica_index = atoi(argv[4]);

    vector<dtxn::Partition> partitions = dtxn::parseConfigurationFromPath(config_file);
    assert(0 <= partition_index && partition_index < partitions.size());
    assert(0 <= replica_index && replica_index < partitions[partition_index].numReplicas());

    io::LibEventLoop event_loop;

    // Connect to the stub
    io::LibEventLoop stub_event_loop;
    vector<NetworkAddress> stub_address(1);
    bool success = stub_address[0].parse(address);
    CHECK(success);

    vector<TCPConnection*> connections = net::createConnectionsWithRetry(
            &event_loop, stub_address);
    if (connections.empty()) {
        LOG_ERROR("Connection to %s failed", address);
        return 1;
    }
    ASSERT(connections.size() == 1 && connections[0] != NULL);
    protorpc::ProtoRpcChannel channel(&stub_event_loop, connections[0]);
    connections.clear();
    protodtxn::ExecutionEngine::Stub service(&channel);

    // Run the engine
    protodtxn::ProtoDtxnEngine engine(&stub_event_loop, &service);
    dtxn2::BlockingScheduler scheduler(&engine);
    dtxn2::runServer(&event_loop, &scheduler, partitions[partition_index], replica_index);

    // Avoid valgrind warnings
#ifndef DEBUG
    google::protobuf::ShutdownProtobufLibrary();
#endif
    return 0;
}
