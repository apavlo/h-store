// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstdio>
#include <netinet/in.h>
#include <fcntl.h>

#include "base/assert.h"
#include "io/libeventloop.h"
#include "networkaddress.h"
#include "protorpc/null.pb.h"
#include "protorpc/protorpcchannel.h"
#include "protorpc/protorpccontroller.h"
#include "protorpc/protoserver.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::FileOutputStream;
using std::string;
using std::vector;

using namespace protorpc;

class ProxyReceiver;

class ProxyRequest : public google::protobuf::Closure {
public:
    ProxyRequest(ProxyReceiver* receiver) : receiver_(receiver), responder_(NULL) {
        CHECK(receiver_ != NULL);
    }

    virtual ~ProxyRequest() {}

    void call(int32_t sequence_number,
            const std::string& method_name,
            const NullMessage& request,
            RpcResponder* responder,
            ProtoRpcChannel* target_channel) {
        final_response_.set_sequence_number(sequence_number);
        final_response_.set_status(protorpc::OK);
        CHECK(responder_ == NULL);
        responder_ = responder;

        target_channel->callMethodByName(method_name, &rpc_, request, &response_, this);
    }

    virtual void Run();

    void callback() {
        response_.SerializeToString(final_response_.mutable_response());
        responder_->reply(final_response_);
        responder_ = NULL;
    }

    int32_t sequence_number() const { return final_response_.sequence_number(); }
    const NullMessage& response() const { return response_; }

private:
    ProxyReceiver* receiver_;
    RpcResponder* responder_;

    ProtoRpcController rpc_;
    NullMessage response_;
    RpcResponse final_response_;
};


class ProxyHandler {
public:
    virtual ~ProxyHandler() {}

    virtual void call(int32_t sequence_number,
            const std::string& method_name,
            const std::string& request,
            const NullMessage& request_parsed) = 0;
    virtual void response(const ProxyRequest& request_wrapper) = 0;
};


class LogHandler : public ProxyHandler {
public:
    LogHandler(const string& file_path) : output_(NULL), count_(0) {
        if (!file_path.empty()) {
            int fd = open(file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
            CHECK(fd >= 0);
            output_ = new FileOutputStream(fd);
        }
    }

    ~LogHandler() {
        printf("%d requests logged\n", count_);
        if (output_ != NULL) {
            bool result = output_->Close();
            ASSERT(result);
            delete output_;
        }
    }

    virtual void call(int32_t sequence_number,
            const std::string& method_name,
            const std::string& request,
            const NullMessage& request_parsed) {
        printf("-> %d %s: %zd bytes\n", sequence_number, method_name.c_str(), request.size()); 
        count_ += 1;

        if (output_ != NULL) {
            // TODO: Cache this object? Take the raw RPC request?
            RpcRequest rpc_request;
            rpc_request.set_sequence_number(sequence_number);  // actually ignored, but required
            rpc_request.set_method_name(method_name);
            rpc_request.set_request(request);

            // TODO: Could be optimized to avoid virtual call and use cached sizes, but that
            // would involve copying code and little reward
            CodedOutputStream out(output_);
            out.WriteVarint32((int32_t) rpc_request.ByteSize());
            rpc_request.SerializeToCodedStream(&out);
            assert(!out.HadError());
        }
    }

    void response(const ProxyRequest& request_wrapper) {
        printf("<- %d: %d bytes\n",
                request_wrapper.sequence_number(), request_wrapper.response().ByteSize());
    }

private:
    FileOutputStream* output_;
    int count_;
};


class ProxyReceiver : public protorpc::RpcReceiver {
public:
    ProxyReceiver(ProtoRpcChannel* target_channel, ProxyHandler* handler) :
            target_channel_(target_channel),
            handler_(handler) {
        CHECK(target_channel_ != NULL);
        CHECK(handler_ != NULL);
    }

    virtual ~ProxyReceiver() {}

    virtual void call(int32_t sequence_number,
            const std::string& method_name,
            const std::string& request,
            RpcResponder* responder) {
        // Parse into a null message so that callMethodByName works
        bool success = null_.ParseFromString(request);
        ASSERT(success);

        handler_->call(sequence_number, method_name, request, null_);

        ProxyRequest* request_wrapper = new ProxyRequest(this);
        request_wrapper->call(sequence_number, method_name, null_, responder, target_channel_);
    }

    void response(ProxyRequest* request_wrapper) {
        handler_->response(*request_wrapper);

        request_wrapper->callback();
        delete request_wrapper;
    }

private:
    ProtoRpcChannel* target_channel_;
    ProxyHandler* handler_;

    ProtoRpcController rpc_;
    NullMessage null_;
};


void ProxyRequest::Run() {
    receiver_->response(this);
}


int main(int argc, const char* argv[]) {
    if (!(3 <= argc && argc <= 4)) {
        fprintf(stderr, "rpcproxy (target address) (listen port) [request log file]\n");
        return 1;
    }
    const char* const address = argv[1];
    int listen_port = atoi(argv[2]);
    CHECK(listen_port > 0);
    string log_path;
    if (argc >= 4) {
        log_path = argv[3];
    }

    io::LibEventLoop event_loop;

    // Connect to the target
    NetworkAddress target_address;
    bool success = target_address.parse(address);
    CHECK(success);
    int socket = connectTCP(target_address.sockaddr());
    CHECK(socket >= 0);
    TCPConnection* connection = new TCPConnection(socket);
    protorpc::ProtoRpcChannel channel(&event_loop, connection);

    // Start the proxy server
    LogHandler logger(log_path);
    ProxyReceiver proxy(&channel, &logger);
    protorpc::RawProtoServer rpc_server(&event_loop, &proxy);
    rpc_server.listen(listen_port);
    printf("listening on port %d\n", listen_port);

    event_loop.exitOnSigInt(true);
    event_loop.run();

    // Avoid valgrind warnings
#ifndef DEBUG
    google::protobuf::ShutdownProtobufLibrary();
#endif
    return 0;
}
