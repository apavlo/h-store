// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <sys/types.h>
#include <sys/socket.h>

#include "google/protobuf/stubs/common.h"
#include "io/libeventloop.h"
#include "protorpc/protoserver.h"
#include "stupidunit/stupidunit.h"

class RawProtoServerTest : public Test {
public:
    RawProtoServerTest() : rpc_server_(&event_loop_, &registry_) {
    }

    ::io::LibEventLoop event_loop_;
    protorpc::ServiceRegistry registry_;
    protorpc::RawProtoServer rpc_server_;
};

TEST_F(RawProtoServerTest, DeleteBeforeClose) {
    // A client connection arrives. The server destructor should clean it up.
    int s = socket(PF_INET, SOCK_STREAM, 0);
    assert(s >= 0);
    TCPConnection* connection = new TCPConnection(s);
    rpc_server_.connectionArrived(connection);
}

/*
TEST_F(ProtoServerTest, DefaultMethodFailure) {
    string empty_request;    
    rpc_server_.call(99, "protorpc.CounterService.Add", empty_request, this);
}
*/

int main() {
    int result = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
