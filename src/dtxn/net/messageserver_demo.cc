// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <netinet/in.h>

#include "io/libeventloop.h"
#include "libevent/include/event2/event.h"
#include "networkaddress.h"
#include "net/demomessages.h"
#include "net/messageserver.h"
#include "strings/utils.h"

/**
TODO: An example server and client. This shows there is too much code needed to use this stuff.
*/

static void print(const net::Demo& msg) {
    printf("integer: %d\n", msg.integer);
    printf("boolean: %s\n", msg.boolean ? "true" : "false");
    printf("str: \"%s\"\n", strings::cEscape(msg.str).c_str());
}

class DemoHandler {
public:
    // Does not own server: it must live longer.
    DemoHandler(net::MessageServer* server) : server_(server) {
        server_->addCallback(&DemoHandler::demo, this);
    }

    ~DemoHandler() {
        server_->removeCallback(&DemoHandler::demo);
    }

    void demo(net::ConnectionHandle* handle, const net::Demo& msg) {
        print(msg);
        printf("\n");

        // Send a response
        net::Demo response = msg;
        response.integer += 1;
        response.boolean = !response.boolean;
        response.str += " server echo";
        server_->send(handle, response);
    }

private:
    net::MessageServer* server_;
};

// Converts the message callback interface into a blocking interface
// Stolen from dtxn/dtnxclient
// TODO: This is a big gross hack. Needs more thinking.
template <typename T>
class MessageWaiter : public MessageConnectionCallback {
public:
    MessageWaiter() : done_(false), output_(NULL) {}

    void wait(io::LibEventLoop* event_loop, T* output) {
        output_ = output;
        assert(output_ != NULL);
        done_ = false;
        while (!done_) {
            // TODO: make this less of a hack
            event_base_loop(event_loop->base(), EVLOOP_ONCE);
        }
    }

    virtual void messageReceived(
            MessageConnection* connection, int32_t type, const std::string& message) {
        CHECK(type == output_->typeCode());
        output_->parseFromString(message);
        done_ = true;
    }

    // A new connection has arrived. connection must be deleted by the callee.
    virtual void connectionArrived(MessageConnection* connection) {
        CHECK(false);
    }

    // The connection has been closed due to the other end or an error. connection can be deleted.
    virtual void connectionClosed(MessageConnection* connection) {
        CHECK(false);
    }

private:
    bool done_;
    T* output_;
};

int main(int argc, const char* argv[]) {
    if (argc != 2 && argc != 3) {
        fprintf(stderr, "server mode: messageserver_demo [listen port]\n");
        fprintf(stderr, "client mode: messageserver_demo [host] [port]\n");
        exit(1);
    }

    io::LibEventLoop event_loop;
    if (argc == 2) {
        event_loop.exitOnSigInt(true);
        int port = atoi(argv[1]);

        net::MessageServer server;
        MessageListener listener(event_loop.base(), &server);
        if (!listener.listen(port)) {
            fprintf(stderr, "Error binding to port\n");
            return 1;
        }

        DemoHandler handler(&server);
        event_loop.run();
    } else {
        // Make a request, wait for the response
        net::Demo request;
        request.integer = 1234;
        request.boolean = true;
        request.str = "foobar";

        NetworkAddress addr;
        if (!addr.parse(std::string(argv[1]) + " " + argv[2])) {
            fprintf(stderr, "Error parsing host port\n");
            return 1;
        }

        int sock = connectTCP(addr.sockaddr());
        assert(sock >= 0);
        TCPMessageConnection connection(event_loop.base(), new TCPConnection(sock));
        connection.send(request);

        MessageWaiter<net::Demo> waiter;
        connection.setTarget(&waiter);
        waiter.wait(&event_loop, &request);
        print(request);
    }
}
