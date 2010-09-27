// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <errno.h>
#include <unistd.h>

#include <cstdio>

#include "testtcpconnection.h"

#include "google/protobuf/descriptor.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/stubs/common.h"
#include "io/libeventloop.h"
#include "protorpc/Counter.pb.h"
#include "protorpc/protorpcchannel.h"
#include "protorpc/protorpccontroller.h"
#include "stupidunit/stupidunit.h"

using namespace google::protobuf;
using namespace google::protobuf::io;

using namespace protorpc;

class ProtoRpcChannelTest : public Test, public google::protobuf::Closure {
public:
    ProtoRpcChannelTest() : called_(false) {
        bool success = testconnect_.connect();
        ASSERT(success);
        channel_ = new ProtoRpcChannel(&testconnect_.event_loop_, testconnect_.connection_);
    }

    ~ProtoRpcChannelTest() {
        // Prevent the destructor from cleaning this up: ProtoRpcChannel does this
        int other = testconnect_.other_;
        testconnect_.detachConnection();

        delete channel_;
        int error = close(other);
        ASSERT(error == 0);
    }

    void Run() {
        testconnect_.event_loop_.exit();
    }

    void deserializeRequest(RpcRequest* request) {
        string buffer;
        buffer.resize(4096);
        int offset = 0;
        while (true) {
            assert(offset < buffer.size());
            size_t remaining = buffer.size() - offset;
            ssize_t bytes = read(testconnect_.other_, base::stringArray(&buffer) + offset, remaining);
            assert(bytes == -1 || bytes > 0);
            if (bytes == -1) {
                assert(errno == EAGAIN);
                bytes = 0;
            } else {
                assert(0 < bytes && bytes <= remaining);
            }
            offset += (int) bytes;

            if (offset < buffer.size()) {
                buffer.resize(offset);
                break;
            }
            assert(offset == buffer.size());
            buffer.resize(buffer.size() + 4096);
        }

        CodedInputStream in(reinterpret_cast<const uint8_t*>(buffer.data()), (int) buffer.size());
        int32_t size;
        bool success = in.ReadLittleEndian32(reinterpret_cast<uint32_t*>(&size));
        assert(success);
        assert(size == buffer.size() - sizeof(size));
        success = request->ParseFromCodedStream(&in);
        assert(success);
    }

    bool called_;
    TestTCPConnection testconnect_;
    ProtoRpcChannel* channel_;
};

TEST_F(ProtoRpcChannelTest, SendReusedResponse) {
    Value request;
    request.set_value(42);
    request.set_name("hello");

    // Set some values to ensure that this gets cleared
    Value response = request;

    const ServiceDescriptor* service = CounterService::descriptor();
    const MethodDescriptor* addMethod = service->FindMethodByName("Add");

    // Call the method: puts data in the connection
    ProtoRpcController controller;
    channel_->CallMethod(addMethod, &controller, &request, &response, this);

    // Verify that the data sent by the channel matches what we want
    RpcRequest rpc;
    deserializeRequest(&rpc);
    EXPECT_EQ(0, rpc.sequence_number());
    EXPECT_EQ("protorpc.CounterService.Add", rpc.method_name());
    EXPECT_EQ(request.SerializeAsString(), rpc.request());

    // Push back a response
    RpcResponse rpc_response;
    rpc_response.set_sequence_number(rpc.sequence_number());
    rpc_response.set_status(OK);
    Value other_response;
    other_response.set_value(5000);
    other_response.AppendToString(rpc_response.mutable_response());

    int32_t size = rpc_response.ByteSize();
    char* buffer = new char[size + sizeof(size)];
    memcpy(buffer, &size, sizeof(size));
    ASSERT_TRUE(rpc_response.SerializeToArray(buffer + sizeof(size), size));
    ssize_t bytes = write(testconnect_.other_, buffer, size + sizeof(size));
    EXPECT_EQ(size + sizeof(size), bytes);
    delete[] buffer;

    // Read the response which calls the callback
    testconnect_.event_loop_.run();
    EXPECT_FALSE(controller.Failed());
    EXPECT_EQ(response.SerializeAsString(), other_response.SerializeAsString());
}

TEST_F(ProtoRpcChannelTest, SendHugeRequest) {
    Value request;
    string data;
    data.resize(1000000);
    request.set_value(42);
    request.set_name(data);

    const ServiceDescriptor* service = CounterService::descriptor();
    const MethodDescriptor* addMethod = service->FindMethodByName("Add");

    // Call the method: puts data in the connection
    ProtoRpcController controller;
    Value response;
    channel_->CallMethod(addMethod, &controller, &request, &response, this);

    // TODO: It would be nice to actually verify that this works end to end
}

int main() {
    int result = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
