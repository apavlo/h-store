// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef TCP_LISTEN_H
#define TCP_LISTEN_H

#include <cassert>

#include "buffer.h"
#include "io/io.h"

struct event;
struct event_base;
struct sockaddr_in;

// Returns a socket connected to address. Returns -1 if either socket() or connect() fail.
int connectTCP(const sockaddr_in& address);

class TCPConnection;

// Interface for asynchronous TCP connection events.
class TCPConnectionCallback {
public:
    virtual ~TCPConnectionCallback() {}

    // Indicates that connection is available for reading.
    virtual void readAvailable(TCPConnection* connection) = 0;

    // Indicates that connection is available for writing.
    virtual void writeAvailable(TCPConnection* connection) = 0;
};

// Class for handling TCP connections in an asynchronous fashion via libevent.
class TCPConnection : public io::OutputStream, public io::InputStream {
public:
    // Creates a TCPConnection that owns socket.
    // TODO: Take the address as a parameter to avoid calls to getpeername()?
    TCPConnection(int socket);
    virtual ~TCPConnection();

    // Calls the read system call on this connection.
    virtual int read(char* buffer, size_t length);

    // Calls the write system call on this connection. Note: This is NOT guaranteed to write
    // all the bytes. Use a write buffer if that is needed. If the write returns EWOULDBLOCK, the
    // writeAvailable callback will be called once the connection can be written again.
    virtual int write(const char* buffer, size_t length);

    // Sets the callback target to target, using eventloop for callbacks. 
    void setTarget(event_base* event_loop, TCPConnectionCallback* target);

private:
    static void libeventCallback(int socket, short type, void* arg);

    // Unregisters event handlers with libevent.
    void unregisterLibevent();

    // Callback target.
    TCPConnectionCallback* target_;

    // The actual socket for the connection.
    int sock_;

    // libevent structure for read events.
    event* read_event_;
    event* write_event_;
};

// Interface for handling new TCP connections.
class TCPListenerCallback {
public:
    virtual ~TCPListenerCallback() {}

    // A new connection has arrived. connection must be deleted by the callee.
    virtual void connectionArrived(TCPConnection* connection) = 0;
};

// Creates a TCP listen socket and uses libevent for reads.
class TCPListener {
public:
    // Creates a listener that will call callbacks on target using event_loop. target and event_loop
    // are not owned by this class
    TCPListener(event_base* eventloop, TCPListenerCallback* target) :
            event_loop_(eventloop), target_(target), sock_(-1), port_(0), event_(NULL) {
        assert(event_loop_ != NULL);
        assert(target_ != NULL);
    }

    ~TCPListener();

    int port() const { return port_; }

    event_base* event_loop() const { return event_loop_; }

    // Begin listening on port. A port of 0 means "automatically pick a port." Returns true if successful.
    bool listen(int port);

private:
    static void libeventCallback(int socket, short type, void* arg);

    event_base* event_loop_;
    TCPListenerCallback* target_;
    int sock_;
    int port_;
    event* event_;
};

#endif
