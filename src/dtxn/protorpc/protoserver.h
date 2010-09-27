// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTORPC_PROTOSERVER_H__
#define PROTORPC_PROTOSERVER_H__

#include "base/unordered_set.h"
#include "protorpc/serviceregistry.h"
#include "tcplistener.h"

namespace io {
class EventLoop;
}

namespace protorpc {

class ServerConnection;

class RawProtoServer : public TCPListenerCallback {
public:
    RawProtoServer(io::EventLoop* event_loop, RpcReceiver* receiver);
    virtual ~RawProtoServer();

    void listen(int port);

    virtual void connectionArrived(TCPConnection* connection);

    // Used by ServerConnection when a connection read fails.
    void closed(ServerConnection* connection);

    // Used by ServerConnection to access the registry.
    void call(int32_t sequence_number, const std::string& method_name, const std::string& request,
            RpcResponder* responder) {
        receiver_->call(sequence_number, method_name, request, responder);
    }

private:
    TCPListener listener_;
    RpcReceiver* receiver_;

    typedef base::unordered_set<ServerConnection*> ConnectionSet;
    ConnectionSet connections_;
};

class ProtoServer {
public:
    ProtoServer(io::EventLoop* event_loop) : raw_server_(event_loop, &registry_) {}

    void listen(int port) {
        raw_server_.listen(port);
    }

    // Registers a service to be exported via this server. Does not own service.
    void registerService(google::protobuf::Service* service) {
        registry_.registerService(service);
    }

    // Unregisters a service that was registered before. Returns true if it works.
    void unregisterService(google::protobuf::Service* service) {
        registry_.unregisterService(service);
    }

private:
    ServiceRegistry registry_;
    RawProtoServer raw_server_;
};

}
#endif
