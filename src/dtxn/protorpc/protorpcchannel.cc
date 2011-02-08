// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protorpc/protorpcchannel.h"

#include "base/assert.h"
#include "google/protobuf/descriptor.h"
#include "io/libeventloop.h"
#include "protorpc/protorpccontroller.h"

using google::protobuf::Closure;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::MethodDescriptor;
using google::protobuf::RpcController;
using std::string;

namespace protorpc {

ProtoRpcChannel::ProtoRpcChannel(io::EventLoop* event_loop, TCPConnection* connection) :
        connection_(connection) {
    connection_->setTarget(((io::LibEventLoop*) event_loop)->base(), this);
    rpc_request_.set_sequence_number(0);
}

ProtoRpcChannel::~ProtoRpcChannel() {
    delete connection_;
}

void ProtoRpcChannel::CallMethod(const MethodDescriptor* descriptor, RpcController* controller,
        const Message* request, Message* response, Closure* callback) {
    callMethodByName(descriptor->full_name(), (ProtoRpcController*) controller, *request, response,
            callback);
}

void ProtoRpcChannel::callMethodByName(const string& method_name, ProtoRpcController* rpc,
        const MessageLite& request, MessageLite* response, Closure* callback) {
    // Track the request
    rpc->setCall(response, callback);
    std::pair<PendingRpcMap::iterator, bool> result = pending_rpcs_.insert(
            std::make_pair(rpc_request_.sequence_number(), rpc));
    assert(result.second);

    // Fill the request info
    rpc_request_.set_method_name(method_name);
    // TODO: Change the protocol to avoid extra copies?
    request.SerializeToString(rpc_request_.mutable_request());

    // Try to write the request
    // TODO: Apply some sort of buffering/queuing here?
    buffer_.bufferMessage(rpc_request_);
    buffer_.tryWrite(connection_);

    // Increment the sequence number and clear fields
    rpc_request_.set_sequence_number(rpc_request_.sequence_number() + 1);
    rpc_request_.clear_method_name();
    rpc_request_.clear_request();
}

void ProtoRpcChannel::readAvailable(TCPConnection* connection) {
    assert(connection == connection_);
    // Read as much data as possible from connection
    int result = buffer_.readAllAvailable(connection);
    // TODO: Handle connection closed.
    CHECK(result != -1);

    // Parse all messages in buffer
    while (buffer_.readBufferedMessage(&rpc_response_)) {
        // Look up and remove the RPC from pending RPCs
        PendingRpcMap::iterator it = pending_rpcs_.find(rpc_response_.sequence_number());
        CHECK(it != pending_rpcs_.end());
        ProtoRpcController* rpc = it->second;
        pending_rpcs_.erase(it);

        // Trigger the callback
        rpc->finished(rpc_response_);
    }
}

void ProtoRpcChannel::writeAvailable(TCPConnection*) {
    buffer_.tryWrite(connection_);
}

}
