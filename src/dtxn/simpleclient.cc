// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <netinet/in.h>

#include "base/time.h"
#include "libevent/include/event2/event.h"
#include "messageconnection.h"
#include "networkaddress.h"

using std::string;

class EchoClient : public MessageConnectionCallback {
public:
    EchoClient(MessageConnection* connection) : connection_(connection) {
        connection_->setTarget(this);
    }

    virtual ~EchoClient() {
        delete connection_;
    }

    void rawSend(int32_t type, const void* data, int length) {
        connection_->rawSend(type, data, length);
    }

    void waitForResponse(event_base* event_loop, int32_t* type, std::string* message) {
        received_ = false;
        type_ = type;
        assert(message != NULL);
        message_ = message;
        while (!received_) {
            event_base_loop(event_loop, EVLOOP_ONCE);
        }
    }

    // Echo messages back to the sender
    virtual void messageReceived(MessageConnection* connection, int32_t type, const std::string& message) {
        assert(connection == connection_);
        assert(received_ == false);
        received_ = true;
        *type_ = type;
        message_->assign(message);
    }

    virtual void connectionArrived(MessageConnection* connection) {
        // Should not be possible.
        assert(false);
    }

    // The connection has been closed due to the other end or an error. connection can be deleted.
    virtual void connectionClosed(MessageConnection* connection) {
        assert(false);
    }

private:
    MessageConnection* connection_;
    bool received_;
    int32_t* type_;
    std::string* message_;
};

static const int TXNS = 500000;

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "simpleclient [address]\n");
        return 1;
    }

    // Connect to the distributor
    NetworkAddress server;
    bool success = server.parse(argv[1]);
    ASSERT(success);

    event_base* event_loop = event_base_new();
    // Dynamically allocate so we can delete it before event_loop
    EchoClient* client = new EchoClient(new TCPMessageConnection(event_loop,
            new TCPConnection(connectTCP(server.sockaddr()))));

    struct timeval start;
    int error = gettimeofday(&start, NULL);
    assert(error == 0);

    for (int i = 0; i < TXNS; ++i) {
        client->rawSend(42, &i, sizeof(i));

        int32_t type;
        std::string result;
        client->waitForResponse(event_loop, &type, &result);
        assert(type == 42);
        assert(result.size() == sizeof(i));
        assert(memcmp(result.data(), &i, sizeof(i)) == 0);
    }
    struct timeval end;
    error = gettimeofday(&end, NULL);
    assert(error == 0);

    int64_t us = base::timevalDiffMicroseconds(end, start);
    printf("%d txns in %" PRIi64 " us = %f txns/s\n", TXNS, us, ((double) TXNS)/(double) us * 1000000);

    delete client;
    event_base_free(event_loop);
    return 0;
}
