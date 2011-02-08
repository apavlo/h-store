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
    ProtoRpcChannelTest() :
            add_method_(CounterService::descriptor()->FindMethodByName("Add")),
            called_(false) {
        bool success = testconnect_.connect();
        ASSERT(success);
        channel_ = new ProtoRpcChannel(&testconnect_.event_loop_, testconnect_.connection_);

        request_.set_value(42);
        request_.set_name("hello");
        rpc_response_.set_status(OK);
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

    ProtoRpcController controller_;
    Value request_;
    Value response_;
    Value other_response_;

    const MethodDescriptor* add_method_;

    RpcRequest rpc_;
    RpcResponse rpc_response_;

    bool called_;
    TestTCPConnection testconnect_;
    ProtoRpcChannel* channel_;
};

TEST_F(ProtoRpcChannelTest, SendReusedResponse) {
    // Set some values to ensure that this gets cleared
    response_ = request_;

    // Call the method: puts data in the connection
    channel_->CallMethod(add_method_, &controller_, &request_, &response_, this);

    // Verify that the data sent by the channel matches what we want
    deserializeRequest(&rpc_);
    EXPECT_EQ(0, rpc_.sequence_number());
    EXPECT_EQ("protorpc.CounterService.Add", rpc_.method_name());
    EXPECT_EQ(request_.SerializeAsString(), rpc_.request());

    // Push back a response
    rpc_response_.set_sequence_number(rpc_.sequence_number());
    other_response_.set_value(5000);
    other_response_.SerializeToString(rpc_response_.mutable_response());

    int32_t size = rpc_response_.ByteSize();
    char* buffer = new char[size + sizeof(size)];
    memcpy(buffer, &size, sizeof(size));
    ASSERT_TRUE(rpc_response_.SerializeToArray(buffer + sizeof(size), size));
    ssize_t bytes = write(testconnect_.other_, buffer, size + sizeof(size));
    EXPECT_EQ(size + sizeof(size), bytes);
    delete[] buffer;

    // Read the response which calls the callback
    testconnect_.event_loop_.run();
    EXPECT_FALSE(controller_.Failed());
    EXPECT_EQ(response_.SerializeAsString(), other_response_.SerializeAsString());
}

TEST_F(ProtoRpcChannelTest, SendHugeRequest) {
    request_.mutable_name()->resize(1000000);

    // Call the method: puts data in the connection
    channel_->CallMethod(add_method_, &controller_, &request_, &response_, this);

    // TODO: It would be nice to actually verify that this works end to end
}

TEST_F(ProtoRpcChannelTest, SendReceiveMultiple) {
    // Call the method twice
    channel_->CallMethod(add_method_, &controller_, &request_, &response_, this);
    deserializeRequest(&rpc_);
    EXPECT_EQ(0, rpc_.sequence_number());

    ProtoRpcController other_controller;
    channel_->CallMethod(add_method_, &other_controller, &request_, &other_response_, this);
    deserializeRequest(&rpc_);
    EXPECT_EQ(1, rpc_.sequence_number());

    rpc_response_.set_sequence_number(0);
    response_.set_value(42);
    response_.SerializeToString(rpc_response_.mutable_response());

    int32_t size = rpc_response_.ByteSize();
    const int response_length = size + (int) sizeof(size);
    char* buffer = new char[response_length * 2];
    memcpy(buffer, &size, sizeof(size));
    ASSERT_TRUE(rpc_response_.SerializeToArray(buffer + sizeof(size), size));
    rpc_response_.set_sequence_number(1);
    response_.set_value(43);
    response_.SerializeToString(rpc_response_.mutable_response());
    memcpy(buffer + response_length, &size, sizeof(size));
    ASSERT_TRUE(rpc_response_.SerializeToArray(buffer + response_length + sizeof(size), size));

    ssize_t bytes = write(testconnect_.other_, buffer, response_length * 2);
    EXPECT_EQ(response_length * 2, bytes);
    delete[] buffer;

    // Read the response which calls the callback
    testconnect_.event_loop_.run();
    EXPECT_EQ(42, response_.value());
    EXPECT_EQ(43, other_response_.value());
}


int main() {
    int result = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
