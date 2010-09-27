// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstdlib>

#include "io/libeventloop.h"
#include "messageconnection.h"

// A simple echo server for really simple performance tests.
class EchoServer : public MessageConnectionCallback {
public:
    virtual ~EchoServer() {}

    // Echo messages back to the sender
    virtual void messageReceived(MessageConnection* connection, int32_t type, const std::string& message) {
        connection->rawSend(type, message.data(), (int) message.size());
    }

    // A new connection has arrived. connection must be deleted by the callee.
    virtual void connectionArrived(MessageConnection* connection) {}

    // The connection has been closed due to the other end or an error. connection can be deleted.
    virtual void connectionClosed(MessageConnection* connection) {
        delete connection;
    }
};

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "simpleserver [listen port]\n");
        return 1;
    }
    int port = atoi(argv[1]);
    assert(port != 0);

    EchoServer server;
    io::LibEventLoop event_loop;
    event_loop.exitOnSigInt(true);

    // Dynamically allocated so we can clean it up before event_loop
    MessageListener listener(event_loop.base(), &server);
    if (!listener.listen(port)) {
        // TODO: How to deal with errors?
        fprintf(stderr, "Error binding to port\n");
        return 1;
    }
    printf("listening on port %d\n", listener.port());

    // Run the event loop
    event_loop.run();

    return 0;
}
