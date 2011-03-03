// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "tcplistener.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>

#include "base/assert.h"
#include "base/cast.h"
#include "libevent/include/event2/event.h"

int connectTCP(const sockaddr_in& address) {
    int sock = socket(PF_INET, SOCK_STREAM, 0);
    if (sock < 0) return sock;
    int error = connect(sock, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
    if (error < 0) return error;
    return sock;
}

static void makeSocketNonBlocking(int socket) {
    // Make the socket non-blocking
    int flags = fcntl(socket, F_GETFL);
    assert(flags >= 0);
    // On Linux, a non-blocking listen socket makes blocking connections.
    // On Mac OS X, it makes a non-blocking connection. Only set flags if we
    // need to.
    if ((flags & O_NONBLOCK) == 0) {
        flags |= O_NONBLOCK;
        int error = fcntl(socket, F_SETFL, flags);
        ASSERT(error == 0);
    }
}

TCPConnection::TCPConnection(int socket) :
        target_(NULL),
        sock_(socket),
        read_event_(NULL),
        write_event_(NULL) {
    assert(sock_ >= 0);

    makeSocketNonBlocking(socket);

    // Disable the Nagle algorithm. This forces the data to be sent when we do a write(). This is
    // what we want for minimum latency processing.
    int flag = 1;
    int error = setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    ASSERT(error == 0);
#ifdef __APPLE__
    // We want failed writes to return EPIPE, not deliver SIGPIPE. See TCPConnection::write.
    error = setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &flag, sizeof(flag));
    ASSERT(error == 0);
#elif defined(__sun__)
    // Solaris has no equivalent: http://bugs.opensolaris.org/bugdatabase/view_bug.do?bug_id=6313487
    // Mask the signal
    struct sigaction action;
    action.sa_handler = SIG_IGN;
    int result = sigaction(SIGPIPE, &action, NULL);
    ASSERT(result == 0);
#endif
}

TCPConnection::~TCPConnection() {
    if (read_event_ != NULL) {
        unregisterLibevent();
    }

    int error = close(sock_);
    ASSERT(error == 0);
}

int TCPConnection::read(char* buffer, size_t length) {
    assert(length <= std::numeric_limits<int>::max());

    // If result is -1, many errors are possible:
    // EWOULDBLOCK: No data available
    // ECONNRESET: The other end closed the connection unexpectedly
    // TODO: Call a connection closed callback?
    ssize_t result = ::read(sock_, buffer, length);
    if (result == 0) {
        // Connection closed: return -1
        return -1;
    } else if (result < 0) {
        // TODO: Handle other errors?
        if (errno == EWOULDBLOCK) {
            // Return 0 to indicate no bytes are available
            return 0;
        }
        perror("TCPConnection::read()");
        CHECK(errno == EWOULDBLOCK);
        return -1;
    } else {
        assert(0 < result && result <= length);
        return assert_range_cast<int>(result);
    }
}

int TCPConnection::write(const char* buffer, size_t length) {
#ifdef __linux__
    // TODO: This is non-portable. We want EPIPE to be returned when a connection breaks.
    // The portable way is to SIG_IGN the SIGPIPE signal, but there is no "clean" place to do that
    // kind of initialization. Gross! See:
    // http://bugs.opensolaris.org/bugdatabase/view_bug.do?bug_id=6313487
    static const int SEND_FLAGS = MSG_NOSIGNAL;
#else
    static const int SEND_FLAGS = 0;
#endif
    ssize_t result = ::send(sock_, buffer, length, SEND_FLAGS);
    if (result < assert_range_cast<ssize_t>(length)) {
        assert(result != 0);
        if (result == -1 && errno != EWOULDBLOCK) {
            // TODO: Silently handle other errors? ECONNRESET, EPIPE
            perror("TCPConnection::write()");
        } else {
            assert((result == -1 && errno == EWOULDBLOCK) || (0 < result && result < length));

            // Trigger the writeAvailable callback, once available.
            // TODO: Should we do this automatically?
            int error = event_add(write_event_, NULL);
            ASSERT(error == 0);
            if (result == -1) {
                result = 0;
            }
        }
    } else {
        assert(result == length);
    }

    return assert_range_cast<int>(result);
}

void TCPConnection::setTarget(event_base* event_loop, TCPConnectionCallback* target) {
    target_ = target;

    if (target_ != NULL && read_event_ == NULL) {
        assert(event_loop != NULL);
        assert(write_event_ == NULL);

        // Register with libevent
        read_event_ = event_new(event_loop, sock_, EV_READ|EV_PERSIST, libeventCallback, this);
        ASSERT(read_event_ != NULL);
        int error = event_add(read_event_, NULL);
        ASSERT(error == 0);
        //~ printf("registered for reading on %d\n", sock_);

        // Initialize the write event, but don't enable it: it gets called if the socket is
        // available for writing, which is almost always. It should only be added when needed.
        write_event_ = event_new(event_loop, sock_, EV_WRITE, libeventCallback, this);
        ASSERT(write_event_ != NULL);
    } else if (target_ == NULL && read_event_ != NULL) {
        unregisterLibevent();
    }
}

void TCPConnection::libeventCallback(int socket, short type, void* arg) {
    TCPConnection* connection = reinterpret_cast<TCPConnection*>(arg);
    assert(socket == connection->sock_);

    //~ printf("connection callback %d\n", socket);
    if (type == EV_READ) {
        connection->target_->readAvailable(connection);
    } else {
        assert(type == EV_WRITE);
        connection->target_->writeAvailable(connection);
    }
}

void TCPConnection::unregisterLibevent() {
    assert(read_event_ != NULL);
    int error = event_del(read_event_);
    assert(error == 0);
    event_free(read_event_);
    read_event_ = NULL;

    // The write event may or may not be enabled: clean it up just in case.
    assert(write_event_ != NULL);
    error = event_del(write_event_);
    assert(error == 0);
    event_free(write_event_);
    write_event_ = NULL;
}

TCPListener::~TCPListener() {
    if (event_ != NULL) {
        int error = event_del(event_);
        assert(error == 0);
        event_free(event_);

        assert(sock_ >= 0);
        error = close(sock_);
        assert(error == 0);
    }
}

// disable -Wconversion to work around a bug in htons in glibc
// TODO: Remove this eventually
#if defined(__OPTIMIZE__) && __GNUC__ == 4 && __GNUC_MINOR__ >= 3
#pragma GCC diagnostic ignored "-Wconversion"
#endif
bool TCPListener::listen(int listen_port) {
    assert(0 <= listen_port && listen_port < (1<<16));
    assert(sock_ == -1);
    assert(event_ == NULL);

    // Create a TCP listen socket
    sock_ = socket(PF_INET, SOCK_STREAM, 0);
    assert(sock_ >= 0);

    // Allow reusing ports in TIME_WAIT state
    int on = 1;
    int error = setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    assert(error == 0);

    makeSocketNonBlocking(sock_);

    // Add the socket to libevent
    event_ = event_new(event_loop_, sock_, EV_READ|EV_PERSIST, libeventCallback, this);
    error = event_add(event_, NULL);
    assert(error == 0);

    // Bind to the given port
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(assert_range_cast<uint16_t>(listen_port));
    addr.sin_addr.s_addr = INADDR_ANY;
    error = bind(sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (error != 0) return false;

    if (listen_port == 0) {
        // Figure out what port the system selected
        socklen_t s = sizeof(addr);
        error = getsockname(sock_, reinterpret_cast<sockaddr*>(&addr), &s);
        if (error != 0) return false;
        port_ = ntohs(addr.sin_port);
    } else {
        port_ = listen_port;
    }

    error = ::listen(sock_, 1024);
    if (error != 0) return false;

    return true;
}

void TCPListener::libeventCallback(int socket, short type, void* arg) {
    TCPListener* listener = reinterpret_cast<TCPListener*>(arg);
    assert(type == EV_READ);
    assert(socket == listener->sock_);

    // Accept as many connections as possible.
    sockaddr_in address;
    socklen_t address_length = sizeof(address);
    int connected_socket = -1;
    while ((connected_socket = accept(socket, reinterpret_cast<sockaddr*>(&address), &address_length)) >= 0) {
        assert(address_length == sizeof(address));
        listener->target_->connectionArrived(new TCPConnection(connected_socket));
    }
    assert(connected_socket == -1 && errno == EWOULDBLOCK);
}
