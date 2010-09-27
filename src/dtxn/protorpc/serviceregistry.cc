// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protorpc/serviceregistry.h"

#include "base/assert.h"
#include "base/stlutil.h"
#include "google/protobuf/descriptor.h"
#include "protorpc/Protocol.pb.h"
#include "protorpc/protorpccontroller.h"


using google::protobuf::Closure;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using google::protobuf::MethodDescriptor;
using google::protobuf::Service;
using google::protobuf::ServiceDescriptor;
using std::string;

namespace protorpc {

// Holds the objects for a single RPC call.
class MethodCallObjects : public Closure {
public:
    MethodCallObjects(MethodInvoker* invoker, const MessageLite& request_prototype,
            const MessageLite& response_prototype) :
            invoker_(invoker),
            request_(request_prototype.New()),
            response_(response_prototype.New()),
            sequence_(0),
            responder_(NULL) {
        assert(invoker_ != NULL);
    }

    ~MethodCallObjects() {
        delete request_;
        delete response_;
    }

    void clear() {
        CHECK(responder_ == NULL);
        controller_.Reset();
        request_->Clear();
        response_->Clear();
    }

    const MessageLite* setCall(int32_t sequence_number, const std::string& request,
            RpcResponder* responder) {
        CHECK(responder_ == NULL);
        responder_ = responder;
        sequence_ = sequence_number;
        assert(responder_ != NULL);
        
        bool success = request_->ParseFromString(request);
        CHECK(success);
        return request_;
    }

    ProtoRpcController* controller() { return &controller_; }
    MessageLite* response() { return response_; }

    // Closure implementation for responding to the RPC
    virtual void Run();

private:
    // These members are "permanent"
    MethodInvoker* const invoker_;
    ProtoRpcController controller_;
    MessageLite* const request_;
    MessageLite* const response_;

    // These members are reused per call
    int32_t sequence_;
    RpcResponder* responder_;
};

// Caches call objects for a single method.
class MethodInvoker {
public:
    MethodInvoker(Service* service, const MethodDescriptor* descriptor) :
            service_(service),
            descriptor_(descriptor) {
    }

    ~MethodInvoker() {
        STLDeleteElements(&recycler_);
    }

    void call(int32_t sequence_number, const std::string& request, RpcResponder* responder) {
        MethodCallObjects* objects = getObjects();
        const MessageLite* request_message = objects->setCall(sequence_number, request, responder);
        // TODO: Change the service interface to take MessageLite?
        service_->CallMethod(descriptor_, objects->controller(),
                reinterpret_cast<const Message*>(request_message),
                reinterpret_cast<Message*>(objects->response()), objects);
    }

    void done(MethodCallObjects* objects, RpcResponder* responder, int32_t sequence,
            const MessageLite& response) {
        // Fill the response message
        rpc_response_.set_sequence_number(sequence);
        bool success = response.SerializeToString(rpc_response_.mutable_response());
        CHECK(success);
        rpc_response_.set_status(protorpc::OK);

        // Dispose of the objects, send the response
        recycleObjects(objects);
        responder->reply(rpc_response_);
        rpc_response_.Clear();
    }

private:
    MethodCallObjects* getObjects() {
        if (recycler_.empty()) {
            return new MethodCallObjects(this,
                    service_->GetRequestPrototype(descriptor_),
                    service_->GetResponsePrototype(descriptor_));
        }

        MethodCallObjects* objects = recycler_.back();
        recycler_.pop_back();
        return objects;
    }

    void recycleObjects(MethodCallObjects* objects) {
        objects->clear();
        recycler_.push_back(objects);
    }

    Service* service_;
    const MethodDescriptor* descriptor_;
    std::vector<MethodCallObjects*> recycler_;

    // Cached response message reused by all calls to done
    RpcResponse rpc_response_;
};

void MethodCallObjects::Run() {
    RpcResponder* responder = responder_;
    responder_ = NULL;

    invoker_->done(this, responder, sequence_, *response_);
}

ServiceRegistry::~ServiceRegistry() {
    STLDeleteValues(&methods_);
}

void ServiceRegistry::registerService(Service* service) {
    const ServiceDescriptor* descriptor = service->GetDescriptor();
    for (int i = 0; i < descriptor->method_count(); ++i) {
        const MethodDescriptor* method = descriptor->method(i);
        const string& full_name = method->full_name();
        MethodInvoker* invoker = new MethodInvoker(service, method);
        std::pair<MethodMap::iterator, bool> result = methods_.insert(
                std::make_pair(full_name, invoker));
        // Ensure that we have not registered this service before.
        CHECK(result.second);
    }
}

void ServiceRegistry::unregisterService(Service* service) {
    const ServiceDescriptor* descriptor = service->GetDescriptor();
    for (int i = 0; i < descriptor->method_count(); ++i) {
        const string& full_name = descriptor->method(i)->full_name();
        MethodMap::iterator it = methods_.find(full_name);
        CHECK(it != methods_.end());
        delete it->second;
        methods_.erase(it);
    }
}

void ServiceRegistry::call(int32_t sequence_number, const std::string& method_name,
        const std::string& request,
        RpcResponder* responder) {
    MethodMap::iterator it = methods_.find(method_name);
    // TODO: Return an RPC error via responder.
    CHECK_M(it != methods_.end(), "Could not find method: %s\n", method_name.c_str());
    it->second->call(sequence_number, request, responder);
}

}  // namespace protorpc
