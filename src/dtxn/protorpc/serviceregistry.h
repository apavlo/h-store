// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTORPC_SERVICEREGISTRY_H__
#define PROTORPC_SERVICEREGISTRY_H__

#include <stdint.h>

#include <string>

#include "base/unordered_map.h"

namespace google { namespace protobuf {
class Service;
}}

namespace protorpc {

class MethodInvoker;
class RpcResponse;

// Interface for responding to RPCs via a callback.
// TODO: Use a Closure instead?
class RpcResponder {
public:
    virtual ~RpcResponder() {}

    virtual void reply(const RpcResponse& response) = 0;
};

// Interface for receiving RPC requests from clients.
class RpcReceiver {
public:
    virtual ~RpcReceiver() {}

    // RPC call received. This must be responded to by calling responder->reply().
    // TODO: For consistency, this should either take an RpcRequest, or RpcResponder should not
    // use RpcResponse.
    virtual void call(int32_t sequence_number,
            const std::string& method_name,
            const std::string& request,
            RpcResponder* responder) = 0;
};

class ServiceRegistry : public RpcReceiver {
public:
    virtual ~ServiceRegistry();

   // Registers a service to be exported via this server. Does not own service.
    void registerService(google::protobuf::Service* service);

    // Unregisters a service that was registered before. Returns true if it works.
    void unregisterService(google::protobuf::Service* service);

    virtual void call(int32_t sequence_number, const std::string& method_name,
            const std::string& request, RpcResponder* responder);

private:
    typedef base::unordered_map<std::string, MethodInvoker*> MethodMap;
    MethodMap methods_;
};

}
#endif
