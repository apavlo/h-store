// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <netinet/in.h>

#include "base/assert.h"
#include "dtxn/configparser.h"
#include "dtxn2/blockingscheduler.h"
#include "dtxn2/common.h"
//~ #include "dtxn/ordered/orderedscheduler.h"
#include "io/libeventloop.h"
#include "protodtxn/dtxn.pb.h"
#include "protodtxn/protodtxnengine.h"
#include "protorpc/protorpcchannel.h"

using std::vector;

int main(int argc, const char* argv[]) {
    if (argc != 5) {
        fprintf(stderr, "protodtxnengine [engine address] [configuration file] [partition index] [replica index]\n");
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
    NetworkAddress stub_address;
    bool success = stub_address.parse(address);
    CHECK(success);
    int socket = connectTCP(stub_address.sockaddr());
    CHECK(socket >= 0);
    TCPConnection* connection = new TCPConnection(socket);
    protorpc::ProtoRpcChannel channel(&stub_event_loop, connection);
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
