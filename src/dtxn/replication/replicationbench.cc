// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

//~ #include <cstdio>
//~ #include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <netinet/in.h>

#include "base/cachedcircularbuffer.h"
#include "benchmark/benchmark.h"
#include "io/buffer.h"
#include "io/libeventloop.h"
#include "io/message.h"
#include "libevent/include/event2/event.h"
#include "net/messageserver.h"
#include "networkaddress.h"
#include "replication/primarybackuplistener.h"

using std::string;
using std::vector;

class NumberMessage : public io::Message {
public:
    NumberMessage() : value_(0) {}

    virtual void serialize(io::FIFOBuffer* buffer) const {
        buffer->copyIn(&value_, sizeof(value_));
    }

    int32_t value() const { return value_; }
    void value(int32_t v) { value_ = v; }

    static int32_t typeCode() { return 100; }

    const char* parseFromString(const char* start, const char* end) {
        const char* next = start + sizeof(value_);
        assert(next <= end);
        memcpy(&value_, start, sizeof(value_));
        return next;
    }

private:
    int32_t value_;
};

class CounterServer : public replication::FaultTolerantLogCallback {
public:
    /** Does not own server or log. */
    CounterServer(net::MessageServer* server, replication::FaultTolerantLog* log) :
            counter_(0), message_server_(server), log_(log) {
        message_server_->addCallback(&CounterServer::increment, this);
        if (log_ != NULL) {
            log_->entry_callback(this);
        }
    }

    ~CounterServer() {
        message_server_->removeCallback(&CounterServer::increment);
        if (log_ != NULL) {
            log_->entry_callback(NULL);
        }
    }

    void increment(net::ConnectionHandle* source, const NumberMessage& message) {
        counter_ += message.value();

        NumberMessage response;
        response.value(counter_);

        if (log_ == NULL) {
            message_server_->send(source, response);
        } else {
            // Log the request
            log_->submit(message, NULL);

            // Queue the response until the log returns
            ResponsePair* r = responses_.add();
            r->source = source;
            r->response.value(response.value());
        }
    }

    virtual void nextLogEntry(int sequence, const std::string& entry, void* arg) {
        if (!responses_.empty()) {
            assert(log_->isPrimary());
            const ResponsePair& r = responses_.front();
            message_server_->send(r.source, r.response);
            responses_.pop_front();
        } else {
            // Apply the message
            assert(!log_->isPrimary());
            NumberMessage message;
            message.parseFromString(entry.data(), entry.data() + entry.size());
            counter_ += message.value();
        }
    }

    int counter() const { return counter_; }

private:
    class ResponsePair {
    public:
        void clear() {}

        net::ConnectionHandle* source;
        NumberMessage response;
    };

    int counter_;
    net::MessageServer* message_server_;
    replication::FaultTolerantLog* log_;
    base::CachedCircularBuffer<ResponsePair> responses_;
};

class ClientMessageConnectionCallback : public MessageConnectionCallback {
public:
    virtual ~ClientMessageConnectionCallback() {}

    // A message was received from connection.
    virtual void messageReceived(MessageConnection* connection, int32_t type, const std::string& message) {
        assert(type == NumberMessage::typeCode());
        assert(message_.empty());
        message_ = message;
    }

    // A new connection has arrived. connection must be deleted by the callee.
    virtual void connectionArrived(MessageConnection* connection) {
        abort();
    }

    // The connection has been closed due to the other end or an error. connection can be deleted.
    virtual void connectionClosed(MessageConnection* connection) {
        abort();
    }

    const string& message() const {
        assert(!message_.empty());
        return message_;
    }

    void clear() {
        assert(!message_.empty());
        message_.clear();
    }

private:
    string message_;
};

class ReplicationBenchmarkWorker : public benchmark::BenchmarkWorker {
public:
    ReplicationBenchmarkWorker(const NetworkAddress& primary) : last_(0) {
        TCPConnection* connection = new TCPConnection(connectTCP(primary.sockaddr()));
        message_connection_ = new TCPMessageConnection(event_loop_.base(), connection);
        message_connection_->setTarget(&callback_);
    }

    virtual ~ReplicationBenchmarkWorker() {
        delete message_connection_;
    }

    virtual void initialize() {}

    virtual void work(bool measure) {
        NumberMessage message;
        message.value(1);

        bool success = message_connection_->send(message);
        ASSERT(success);

        // HACK
        event_base_loop(event_loop_.base(), EVLOOP_ONCE);
        const string& out = callback_.message();

        message.parseFromString(out.data(), out.data() + out.size());
        assert(message.value() > last_);
        last_ = message.value();
        callback_.clear();
    };

    virtual void finish() {};

private:
    io::LibEventLoop event_loop_;
    TCPMessageConnection* message_connection_;
    ClientMessageConnectionCallback callback_;
    int32_t last_;
};

static const int WARMUP_SECONDS = 5;
static const int MEASURE_SECONDS = 30;


void runClients(const NetworkAddress& primary, int num_clients) {
    vector<ReplicationBenchmarkWorker*> workers;
    for (int i = 0; i < num_clients; ++i) {
        workers.push_back(new ReplicationBenchmarkWorker(primary));
    }

    int requests = 0;
    int64_t microseconds = benchmark::runThreadBench(
            WARMUP_SECONDS, MEASURE_SECONDS, workers, &requests);
    double requests_per_second = (double) requests / (double) microseconds * 1000000.0; 

    printf("%f requests/second\n", requests_per_second);
    STLDeleteElements(&workers);
}

static const int MAX_SIMULTANEOUS_CLIENTS = 20;

void runLatency(const NetworkAddress& primary, int desired_requests_per_second) {
    vector<ReplicationBenchmarkWorker*> workers;
    for (int i = 0; i < MAX_SIMULTANEOUS_CLIENTS; ++i) {
        workers.push_back(new ReplicationBenchmarkWorker(primary));
    }

    vector<int> latencies;
    double requests_per_second = benchmark::runThreadLatencyBench(
            WARMUP_SECONDS, MEASURE_SECONDS, workers, desired_requests_per_second, &latencies);

    printf("%f requests/second\n", requests_per_second);

    int min = std::numeric_limits<int>::max();
    int max = -1;
    int sum = 0;
    for (int i = 0; i < latencies.size(); ++i) {
        assert(latencies[i] > 0);
        if (latencies[i] < min) min = latencies[i];
        if (latencies[i] > max) max = latencies[i];
        sum += latencies[i];
    }
    printf("latency: min %d / average %f / max %d microseconds\n",
            min, (double) sum / (double) latencies.size(), max);
    STLDeleteElements(&workers);
}

void printError() {
    fprintf(stderr, "replicationbench primary [listen port] [replica host_ports (primary)]\n");
    fprintf(stderr, "replicationbench backup [listen port] (replica)\n");
    fprintf(stderr, "replicationbench client [primary host_port] [num clients] (client: closed loop)\n");
    fprintf(stderr, "replicationbench latency [primary host_port] [requests per second] (client: rate limited)\n");
}

int main(int argc, const char* argv[]) {
    if (argc < 3) {
        printError();
        return 1;
    }

    bool primary = false;
    const string execute_type = argv[1];
    if (execute_type == "client" || execute_type == "latency") {
        if (argc != 4) {
            printError();
            return 1;
        }

        NetworkAddress primary;
        bool success = primary.parse(argv[2]);
        if (!success) {
            fprintf(stderr, "Address %s could not be parsed\n", argv[2]);
            printError();
            return 1;
        }

        int argument = atoi(argv[3]);
        if (argument <= 0) {
            printError();
            return 1;
        }

        if (execute_type == "client") {
            runClients(primary, argument);
        } else {
            assert(execute_type == "latency");
            runLatency(primary, argument);
        }
        return 0;
    } else if (execute_type == "primary") {
        primary = true;
    } else if (execute_type == "backup") {
        primary = false;
    } else {
        printError();
        return 1;
    }

    int port = atoi(argv[2]);

    // Connect to backups
    io::LibEventLoop event_loop;
    vector<MessageConnection*> backup_connections;
    for (int i = 3; i < argc; ++i) {
        NetworkAddress address;
        bool success = address.parse(argv[i]);
        if (!success) {
            fprintf(stderr, "Address %s could not be parsed\n", argv[i]);
            return 1;
        }
        TCPConnection* connection = new TCPConnection(connectTCP(address.sockaddr()));
        backup_connections.push_back(new TCPMessageConnection(event_loop.base(), connection));
    }

    // Start listening on the specified port
    net::MessageServer msg_server;
    MessageListener msg_listener(event_loop.base(), &msg_server);
    if (!msg_listener.listen(port)) {
        fprintf(stderr, "Error binding to port %d\n", port);
        return 1;
    }

    // Create the replica and attach it 
    replication::PrimaryBackupListener* listener = NULL;
    replication::FaultTolerantLog* log = NULL;
    if (!primary || backup_connections.size() > 0) {
        listener = new replication::PrimaryBackupListener(&event_loop, &msg_server);
        log = listener->replica();
    }

    // If we are the primary, connect to the replicas
    if (primary && backup_connections.size() > 0) {
        listener->setPrimary(backup_connections);
    } else if (!primary) {
        listener->setBackup();
    }

    // Introduce scope so CounterServer is deleted before listener.
    {
        CounterServer counter(&msg_server, log);

        printf("listening on port %d\n", port);

        // Exit when SIGINT is caught
        event_loop.exitOnSigInt(true);

        // Run the event loop
        event_loop.run();

        printf("counter = %d\n", counter.counter());
    }

    delete listener;
    return 0;
}
