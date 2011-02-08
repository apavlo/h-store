// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protorpc/protoserver.h"

#include "base/assert.h"
#include "base/stlutil.h"
#include "io/libeventloop.h"
#include "protorpc/Protocol.pb.h"
#include "protorpc/protoconnectionbuffer.h"

namespace protorpc {

RawProtoServer::RawProtoServer(io::EventLoop* event_loop, RpcReceiver* receiver) :
        listener_(((io::LibEventLoop*)event_loop)->base(), this),
        receiver_(receiver) {
    CHECK(receiver_ != NULL);
}

RawProtoServer::~RawProtoServer() {
    STLDeleteElements(&connections_);
}

void RawProtoServer::listen(int port) {
    listener_.listen(port);
}

class ServerConnection : public TCPConnectionCallback, public RpcResponder {
public:
    // Owns connection
    ServerConnection(RawProtoServer* server, event_base* event_loop, TCPConnection* connection) :
            server_(server),
            connection_(connection),
            next_sequence_(0) {
        connection_->setTarget(event_loop, this);
    }

    virtual ~ServerConnection() {
        delete connection_;
    }

    virtual void readAvailable(TCPConnection* connection) {
        // Read as much data as possible from connection
        int result = buffer_.readAllAvailable(connection);
        if (result == -1) {
            // Connection closed
            server_->closed(this);
            return;
        }

        // Parse as many messages as possible from the buffer
        while (buffer_.readBufferedMessage(&rpc_request_)) {
            CHECK(rpc_request_.sequence_number() == next_sequence_);
            next_sequence_ += 1;

            server_->call(rpc_request_.sequence_number(), rpc_request_.method_name(),
                    rpc_request_.request(), this);
            rpc_request_.Clear();
        }
    }

    virtual void writeAvailable(TCPConnection* connection) {
        buffer_.tryWrite(connection_);
    }

    // TODO: BUG: If the client connection was closed and the method completes, this will be
    // called on a deleted object. Reference count it or something similar?
    virtual void reply(const RpcResponse& response) {
        // TODO: Verify that this sequence number is "pending"?
        assert(0 <= response.sequence_number() && response.sequence_number() < next_sequence_);

        // Send the response
        buffer_.bufferMessage(response);
        buffer_.tryWrite(connection_);
    }

private:
    RawProtoServer* server_;
    TCPConnection* connection_;
    int32_t next_sequence_;
    ProtoConnectionBuffer buffer_;
    RpcRequest rpc_request_;
};

void RawProtoServer::connectionArrived(TCPConnection* connection) {
    // Hold on to this so we can close and delete it when the RawProtoServer is deleted
    ServerConnection* server_connection =
            new ServerConnection(this, listener_.event_loop(), connection);
    std::pair<ConnectionSet::iterator, bool> result = connections_.insert(server_connection);
    assert(result.second);
}

void RawProtoServer::closed(ServerConnection* connection) {
    size_t count = connections_.erase(connection);
    ASSERT(count == 1);
    delete connection;
}

}
