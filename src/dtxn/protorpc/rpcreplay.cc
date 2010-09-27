// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstdio>

#include <netinet/in.h>

#include "base/assert.h"
#include "io/libeventloop.h"
#include "networkaddress.h"
#include "protorpc/null.pb.h"
#include "protorpc/Protocol.pb.h"
#include "protorpc/protorpcchannel.h"
#include "protorpc/protorpccontroller.h"
#include "protorpc/sequencereader.h"

using google::protobuf::Closure;
using google::protobuf::NewPermanentCallback;

using namespace protorpc;

int main(int argc, const char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "rpcreplay (target address) (log file)\n");
        return 1;
    }
    const char* const address = argv[1];
    const char* const log_path = argv[2];

    // Parse target address
    NetworkAddress target_address;
    bool success = target_address.parse(address);
    CHECK(success);

    // Connect
    io::LibEventLoop event_loop;
    int socket = connectTCP(target_address.sockaddr());
    CHECK(socket >= 0);
    TCPConnection* connection = new TCPConnection(socket);
    ProtoRpcChannel channel(&event_loop, connection);

    // Replay the requests
    RpcRequest temp_request;
    NullMessage temp_null;
    ProtoRpcController rpc;
    NullMessage response;
    Closure* done = NewPermanentCallback(&event_loop, &io::LibEventLoop::exit);
    int count = 0;
    for (SequenceReader reader(log_path); reader.hasValue(); reader.advance()) {
        bool success = temp_request.ParseFromString(reader.current());
        ASSERT(success);
        success = temp_null.ParseFromString(temp_request.request());
        ASSERT(success);

        channel.callMethodByName(temp_request.method_name(), &rpc, temp_null, &response, done);
        event_loop.run();
        assert(!rpc.Failed());

        count += 1;
    }
    printf("%d requests sent successfully\n", count);
    delete done;

    // Avoid valgrind warnings
#ifndef DEBUG
    google::protobuf::ShutdownProtobufLibrary();
#endif
    return 0;
}
