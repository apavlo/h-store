// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "net/messageconnectionutil.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>

#include "base/stlutil.h"
#include "messageconnection.h"
#include "io/libeventloop.h"
#include "libevent/include/event2/event.h"
#include "libevent/include/event2/event_struct.h"
#include "networkaddress.h"
#include "tcplistener.h"

using std::vector;

namespace net {

// TODO: Make TCPMessageConnection take an EventLoop not an event_base.
vector<MessageConnection*> createConnections(
        io::EventLoop* event_loop,
        const vector<NetworkAddress>& addresses) {
    vector<MessageConnection*> connections;
    for (int i = 0; i < addresses.size(); ++i) {
        int sock = connectTCP(addresses[i].sockaddr());
        connections.push_back(new TCPMessageConnection(
                ((io::LibEventLoop*) event_loop)->base(), new TCPConnection(sock)));
    }

    return connections;
}


class ParallelConnectState {
public:
    ParallelConnectState(int unconnected_count, event_base* libevent_base, int retry_seconds,
            int give_up_seconds):
            unconnected_count_(unconnected_count),
            libevent_base_(libevent_base) {
        retry_timeout_.tv_sec = retry_seconds;
        retry_timeout_.tv_usec = 0;

        give_up_timeout_.tv_sec = give_up_seconds;
        give_up_timeout_.tv_usec = 0;

        // Start the give up timer
        int error = event_assign(&give_up_event_, libevent_base_,
                -1, EV_TIMEOUT, &ParallelConnectState::giveUpCallback, this);
        ASSERT(error == 0);
        error = event_add(&give_up_event_, &give_up_timeout_);
        ASSERT(error == 0);
    }

    ~ParallelConnectState() {
        int error = event_del(&give_up_event_);
        ASSERT(error == 0);
    }

    void recordSuccessfulConnect() {
        unconnected_count_ -= 1;
        assert(unconnected_count_  >= 0);
        if (unconnected_count_ == 0) {
            int error = event_del(&give_up_event_);
            ASSERT(error == 0);
            error = event_base_loopexit(libevent_base_, NULL);
            ASSERT(error == 0);
        }
    }

    event_base* base() { return libevent_base_; }
    const struct timeval* retry_timeout() const { return &retry_timeout_; }
    bool isAllConnected() const { return unconnected_count_ == 0; }

private:
    int unconnected_count_;
    event_base* libevent_base_;
    struct timeval retry_timeout_;
    struct timeval give_up_timeout_;
    event give_up_event_;

    static void giveUpCallback(int socket, short event, void* argument) {
        assert(socket == -1);
        assert(event == EV_TIMEOUT);
        //~ printf("give up\n");
        ParallelConnectState* state = (ParallelConnectState*) argument;
        int error = event_base_loopexit(state->libevent_base_, NULL);
        ASSERT(error == 0);
    }
};


class AsyncConnectionState {
public:
    AsyncConnectionState(const NetworkAddress* address, ParallelConnectState* state)
            : socket_(-1), address_(address), parallel_state_(state) {
        assert(address_ != NULL);
        assert(parallel_state_ != NULL);
    }

    ~AsyncConnectionState() {
        if (socket_ != -1) {
            int error = close(socket_);
            ASSERT(error == 0);
        }

        int error = event_del(&libevent_event_);
        ASSERT(error == 0);
    }

    static void connectCallback(int socket, short event, void* argument) {
        AsyncConnectionState* connection = (AsyncConnectionState*) argument;
        ParallelConnectState* parallel_state = connection->parallel_state_;
        assert(socket == connection->socket_);
        assert(event == EV_WRITE);
        //~ printf("connect callback\n");

        // Figure out if the connect succeeded
        int connect_error = 0;
        socklen_t option_length = sizeof(connect_error);
        int error = getsockopt(socket, SOL_SOCKET, SO_ERROR, &connect_error, &option_length);
        CHECK(error == 0);
        CHECK(option_length == sizeof(connect_error));

        if (connect_error) {
            //~ printf("connect error %d = %s\n", connect_error, strerror(connect_error));
            // Close the socket and reschedule the connection
            int error = close(connection->socket_);
            assert(error == 0);
            connection->socket_ = -1;

            error = event_assign(&connection->libevent_event_, parallel_state->base(),
                    -1, EV_TIMEOUT, &AsyncConnectionState::retryCallback, connection);
            ASSERT(error == 0);

            error = event_add(&connection->libevent_event_, parallel_state->retry_timeout());
            CHECK(error == 0);
        } else {
            // report that the connection was successful
            //~ printf("connect success\n");
            parallel_state->recordSuccessfulConnect();
        }
    }

    static void retryCallback(int socket, short event, void* argument) {
        //~ printf("retry timer\n");
        assert(socket == -1);
        assert(event == EV_TIMEOUT);
        AsyncConnectionState* state = (AsyncConnectionState*) argument;
        int error = state->startConnect();
        CHECK(error == 0);
    }

    int startConnect() {
        assert(socket_ == -1);
        socket_ = socket(PF_INET, SOCK_STREAM, 0);
        if (socket_ < 0) return socket_;

        // Make the socket non-blocking (flags probably = O_RDWR)
        int flags = fcntl(socket_, F_GETFL);
        flags |= O_NONBLOCK;
        int error = fcntl(socket_, F_SETFL, flags);
        if (error != 0) return error;

        const struct sockaddr_in addr = address_->sockaddr();
        error = connect(socket_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
        CHECK(error == -1);
        if (errno != EINPROGRESS) {
            return -errno;
        }

        error = event_assign(&libevent_event_, parallel_state_->base(),
                socket_, EV_WRITE, &AsyncConnectionState::connectCallback, this);
        ASSERT(error == 0);
        error = event_add(&libevent_event_, NULL);
        ASSERT(error == 0);
        return 0;
    }

    int takeSocket() {
        assert(socket_ != -1);
        int sock = socket_;
        socket_ = -1;
        return sock;
    }

private:
    int socket_;
    const NetworkAddress* address_;
    ParallelConnectState* parallel_state_;

    // Needed because we need to cancel when we abort.
    event libevent_event_;
};

static const int RETRY_TIMEOUT_S = 5;
static const int GIVEUP_TIMEOUT_S = 30;

vector<TCPConnection*> createConnectionsWithRetry(
        io::EventLoop* event_loop,
        const vector<NetworkAddress>& addresses) {

    ParallelConnectState state(
            static_cast<int>(addresses.size()),
            ((io::LibEventLoop*) event_loop)->base(),
            RETRY_TIMEOUT_S,
            GIVEUP_TIMEOUT_S);

    vector<AsyncConnectionState*> connection_states(addresses.size());
    for (int i = 0; i < connection_states.size(); ++i) {
        connection_states[i] = new AsyncConnectionState(&addresses[i], &state);
        int error = connection_states[i]->startConnect();
        CHECK(error == 0);
    }

    event_loop->run();

    vector<TCPConnection*> connections;
    if (state.isAllConnected()) {
        for (int i = 0; i < connection_states.size(); ++i) {
            int sock = connection_states[i]->takeSocket();
            ASSERT(sock >= 0);
            connections.push_back(new TCPConnection(sock));
        }
    }

    STLDeleteElements(&connection_states);
    return connections;
}


}  // namespace net
