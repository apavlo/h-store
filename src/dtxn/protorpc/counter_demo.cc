// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <tr1/unordered_map>

#include <netinet/in.h>

#include "base/assert.h"
#include "io/libeventloop.h"
#include "networkaddress.h"
#include "net/messageconnectionutil.h"
#include "protorpc/Counter.pb.h"
#include "protorpc/protoconnectionbuffer.h"
#include "protorpc/protorpcchannel.h"
#include "protorpc/protorpccontroller.h"
#include "protorpc/protoserver.h"
#include "tcplistener.h"

using google::protobuf::Closure;
using google::protobuf::Message;
using google::protobuf::MethodDescriptor;
using google::protobuf::RpcChannel;
using google::protobuf::RpcController;
using protorpc::GetRequest;
using protorpc::Value;
using std::string;
using std::vector;

class CounterServer : public protorpc::CounterService {
public:
    CounterServer() : counter_(0) {}

    virtual void Add(RpcController* controller,
            const Value* request,
            Value* response,
            Closure* done) {
        int result = counter_ + request->value();
        printf("add %d + %d = %d\n", counter_, request->value(), result);
        counter_ = result;

        response->set_value(result);
        done->Run();
    }

    virtual void Get(RpcController* controller,
            const GetRequest* request,
            Value* response,
            Closure* done) {
        printf("get = %d\n", counter_);
        response->set_value(counter_);
        done->Run();
    }

private:
    int counter_;
};


// Runs an EventLoop until called.
class EventLoopClosure : public Closure {
public:
    EventLoopClosure() : event_loop_(NULL) {}

    virtual void Run() {
        event_loop_->exit();
        event_loop_ = NULL;
    }

    // Runs event_loop until called.
    void loopUntilCalled(io::EventLoop* event_loop) {
        assert(event_loop_ == NULL);
        event_loop_ = event_loop;
        
        event_loop_->run();
        assert(event_loop_ == NULL);
    }

private:
    io::EventLoop* event_loop_;
};

static int client(int argc, const char* argv[]) {
    CHECK(argc == 3 || argc == 4);
    io::LibEventLoop event_loop;

    // Connect to the server
    NetworkAddress client_address;
    bool success = client_address.parse(argv[1]);
    CHECK(success);

    vector<NetworkAddress> addresses(1);
    success = addresses[0].parse(argv[1]);
    CHECK(success);

    vector<TCPConnection*> connections = net::createConnectionsWithRetry(
        &event_loop, addresses);
    if (connections.empty()) {
        fprintf(stderr, "Connection failed\n");
        return 1;
    }

    ASSERT(connections.size() == 1);
    protorpc::ProtoRpcChannel channel(&event_loop, connections[0]);
    connections.clear();

    // Create the stub
    protorpc::CounterService::Stub service(&channel);
    protorpc::ProtoRpcController controller;
    EventLoopClosure waiter;
    protorpc::Value response;

    if (argc == 3) {
        protorpc::GetRequest get;
        get.set_name(argv[2]);
        service.Get(&controller, &get, &response, &waiter);
    } else {
        assert(argc == 4);
        protorpc::Value add;
        add.set_name(argv[2]);
        add.set_value(atoi(argv[3]));
        service.Add(&controller, &add, &response, &waiter);
    }
    
    waiter.loopUntilCalled(&event_loop);
    CHECK(!controller.Failed());
    printf("value '%s' = %d\n", response.name().c_str(), response.value());
    return 0;
}

static int server(int argc, const char* argv[]) {
    io::LibEventLoop event_loop;

    CounterServer counter_server;
    protorpc::ProtoServer rpc_server(&event_loop);
    rpc_server.registerService(&counter_server);

    int port = atoi(argv[1]);
    rpc_server.listen(port);
    printf("listening on port %d\n", port);

    event_loop.exitOnSigInt(true);
    event_loop.run();

    // Not strictly required
    rpc_server.unregisterService(&counter_server);
    return 0;
}

int main(int argc, const char* argv[]) {
    if (argc == 3 || argc == 4) {
        return client(argc, argv);
    } else if (argc == 2) {
        return server(argc, argv);
    } else {
        fprintf(stderr, "counter_demo (port number)|(server address) (key name) [increment]\n");
        return 1;
    }
}
