// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstdio>
#include <vector>

#include <netinet/in.h>
#include <fcntl.h>

#include "base/assert.h"
#include "base/stlutil.h"
#include "benchmark/benchmark.h"
#include "io/libeventloop.h"
#include "libevent/include/event2/event.h"
#include "networkaddress.h"
#include "protorpc/null.pb.h"
#include "protorpc/Protocol.pb.h"
#include "protorpc/protorpcchannel.h"
#include "protorpc/protorpccontroller.h"
#include "protorpc/sequencereader.h"
#include "randomgenerator.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

using google::protobuf::Closure;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::FileInputStream;
using std::string;
using std::vector;

using namespace protorpc;

class RawRpcRequest;

class RawRpcRequestCallback {
public:
    virtual ~RawRpcRequestCallback() {}

    virtual void requestDone(RawRpcRequest* request) = 0;
};

class RawRpcRequest : public google::protobuf::Closure {
public:
    RawRpcRequest() : done_(NULL) {}

    virtual ~RawRpcRequest() {}

    void call(const std::string& method_name,
            const NullMessage& request,
            ProtoRpcChannel* target_channel,
            RawRpcRequestCallback* done) {
        CHECK(done_ == NULL);
        done_ = done;
        CHECK(done_ != NULL);

        target_channel->callMethodByName(method_name, &rpc_, request, &response_, this);
    }

    virtual void Run() {
        RawRpcRequestCallback* done = done_;
        done_ = NULL;
        done->requestDone(this);
    }

    const NullMessage& response() const { return response_; }

private:
    RawRpcRequestCallback* done_;

    ProtoRpcController rpc_;
    NullMessage response_;
};


typedef vector<std::pair<string, NullMessage*> > RequestVector;

class RpcLoadThread :
        public benchmark::BenchmarkWorker,
        public RawRpcRequestCallback {
public:
    RpcLoadThread(const NetworkAddress& target_address, int simultaneous_requests,
            const RequestVector* requests) :

            requests_(requests),
            pending_(0) {
        // Connect to the target
        int socket = connectTCP(target_address.sockaddr());
        CHECK(socket >= 0);
        TCPConnection* connection = new TCPConnection(socket);
        channel_ = new protorpc::ProtoRpcChannel(&event_loop_, connection);

        assert(requests_ != NULL);
        assert(simultaneous_requests > 0);

        // Seed the generator with a per-client value
        generator_.seed((unsigned int) (time(NULL) + pthread_self()));

        request_objects_.reserve(simultaneous_requests);
        for (int i = 0; i < simultaneous_requests; ++i) {
            request_objects_.push_back(new RawRpcRequest());
        }
    }

    ~RpcLoadThread() {
        delete channel_;
        STLDeleteElements(&request_objects_);
    }

    virtual void initialize() {}

    virtual void work(bool measure) {
        while (!request_objects_.empty()) {
            //~ printf("%p dispatching request\n", this);
            int index = generator_.random() % (int) requests_->size();

            RawRpcRequest* request_wrapper = request_objects_.back();
            request_objects_.pop_back();

            request_wrapper->call(
                    (*requests_)[index].first, *(*requests_)[index].second, channel_, this);
            pending_ += 1;
        }

        // Loop until something changes (we receive a response)
        while (request_objects_.empty()) {
            event_base_loop(event_loop_.base(), EVLOOP_ONCE);
        }
    }

    virtual void finish() {
        // If we should exit, loop until all transactions are done
        while (pending_ > 0) {
            event_base_loop(event_loop_.base(), EVLOOP_ONCE);
        }
    }

    virtual void requestDone(RawRpcRequest* request_wrapper) {
        request_objects_.push_back(request_wrapper);
        pending_ -= 1;
    }

private:
    const RequestVector* requests_;

    RandomGenerator generator_;
    io::LibEventLoop event_loop_;
    protorpc::ProtoRpcChannel* channel_;

    vector<RawRpcRequest*> request_objects_;
    int pending_;
};

static const int WARM_UP_SECONDS = 15;
static const int TEST_SECONDS = 60;

int main(int argc, const char* argv[]) {
    if (argc != 4) {
        fprintf(stderr, "rpcbench (target address) (log file) (dispatch count)\n");
        return 1;
    }
    const char* const address = argv[1];
    const char* const log_path = argv[2];
    int dispatch = atoi(argv[3]);
    CHECK(dispatch > 0);

    // Parse target address
    NetworkAddress target_address;
    bool success = target_address.parse(address);
    CHECK(success);

    // Read the requests
    RequestVector requests;
    RpcRequest temp_request;
    for (SequenceReader reader(log_path); reader.hasValue(); reader.advance()) {
        bool success = temp_request.ParseFromString(reader.current());
        ASSERT(success);

        requests.push_back(std::make_pair(temp_request.method_name(), new NullMessage()));
        success = requests.back().second->ParseFromString(temp_request.request());
        ASSERT(success);
    }
    printf("%zd requests read from log\n", requests.size());

    // Create load threads
    vector<RpcLoadThread*> threads;
    threads.push_back(new RpcLoadThread(target_address, dispatch, &requests));

    // Run the test
    int total_requests = 0;
    int64_t us = benchmark::runThreadBench(WARM_UP_SECONDS, TEST_SECONDS, threads, &total_requests);

    // Report 
    double seconds = (double) us/1000000.0;
    printf("%f\n", total_requests/seconds);

    STLDeleteElements(&threads);
    STLDeleteValues(&requests);
    // Avoid valgrind warnings
#ifndef DEBUG
    google::protobuf::ShutdownProtobufLibrary();
#endif
    return 0;
}
