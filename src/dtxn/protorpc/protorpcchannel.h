// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTORPC_PROTORPCCHANNEL_H__
#define PROTORPC_PROTORPCCHANNEL_H__

#include <tr1/unordered_map>

#include "protorpc/Protocol.pb.h"
#include "protorpc/protoconnectionbuffer.h"
#include "tcplistener.h"
#include "google/protobuf/service.h"

namespace io {
class EventLoop;
}

namespace protorpc {

class ProtoRpcController;

class ProtoRpcChannel : public google::protobuf::RpcChannel, public TCPConnectionCallback  {
public:
    // Owns connection
    ProtoRpcChannel(io::EventLoop* event_loop, TCPConnection* connection);

    virtual ~ProtoRpcChannel();

    virtual void CallMethod(const google::protobuf::MethodDescriptor* descriptor,
            google::protobuf::RpcController* controller,
            const google::protobuf::Message* request,
            google::protobuf::Message* response,
            google::protobuf::Closure* callback);

    void callMethodByName(const std::string& method_name,
            ProtoRpcController* controller,
            const google::protobuf::MessageLite& request,
            google::protobuf::MessageLite* response,
            google::protobuf::Closure* callback);

    virtual void readAvailable(TCPConnection* connection);

    virtual void writeAvailable(TCPConnection*);

private:
    TCPConnection* connection_;
    ProtoConnectionBuffer buffer_;
    RpcRequest rpc_request_;
    RpcResponse rpc_response_;

    typedef std::tr1::unordered_map<int32_t, ProtoRpcController*> PendingRpcMap;
    PendingRpcMap pending_rpcs_;
};

}
#endif
