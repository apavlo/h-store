// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protorpc/Counter.pb.h"
#include "protorpc/Protocol.pb.h"
#include "protorpc/serviceregistry.h"
#include "stupidunit/stupidunit.h"

using namespace google::protobuf;
using namespace protorpc;

class CounterExample : public CounterService {
public:
    virtual void Get(RpcController* controller, const GetRequest* request, Value* response,
            Closure* done) {
        response->set_value(-77);
        done->Run();
    }
};

class ServiceRegistryTest : public Test, public RpcResponder {
public:
    ServiceRegistryTest() {}

    virtual void reply(const RpcResponse& response) {
        assert(response.status() == protorpc::OK);
        sequence_ = response.sequence_number();
        reply_ = response.response();
    }

    CounterExample counter_;
    ServiceRegistry registry_;

    int32_t sequence_;
    string reply_;
};

TEST_F(ServiceRegistryTest, RegisterUnregister) {
    EXPECT_DEATH(registry_.unregisterService(&counter_));
    registry_.registerService(&counter_);
    registry_.unregisterService(&counter_);
    EXPECT_DEATH(registry_.unregisterService(&counter_));

    // Re-register: the destructor should free this
    registry_.registerService(&counter_);
}

TEST_F(ServiceRegistryTest, CallResponse) {
    string empty_request;
    EXPECT_DEATH(registry_.call(99, "protorpc.CounterService.Get", empty_request, this));

    registry_.registerService(&counter_);
    registry_.call(99, "protorpc.CounterService.Get", empty_request, this);
    Value v;
    bool success = v.ParseFromString(reply_);
    EXPECT_TRUE(success);
    EXPECT_EQ(-77, v.value());
}

int main() {
    int result = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
