// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef TESTTCPCONNECTION_H__
#define TESTTCPCONNECTION_H__

#include <arpa/inet.h>
#include <netinet/in.h>

#include <string>
#include <tr1/functional>

#include "base/assert.h"
#include "base/cast.h"
#include "base/function_thread.h"
#include "base/stringutil.h"
#include "io/libeventloop.h"
#include "libevent/include/event2/event.h"
#include "tcplistener.h"


class TestTCPConnection : public TCPConnectionCallback, public TCPListenerCallback {
public:
    TestTCPConnection() :
            listener_(new TCPListener(event_loop_.base(), this)),
            connection_(NULL),
            other_(-1),
            write_available_(false) {}

    ~TestTCPConnection() {
        if (other_ >= 0) {
            // Force close the connection
            assert(connection_ != NULL);
            delete connection_;
            connection_ = NULL;
            int error = close(other_);
            ASSERT(error == 0);
        }

        // Need to delete listener *before* we delete the event loop, since it
        // unregisters its events
        delete listener_;
    }

    virtual void connectionArrived(TCPConnection* connection) {
        assert(connection_ == NULL);
        connection_ = connection;
        connection_->setTarget(event_loop_.base(), this);
    }

    virtual void readAvailable(TCPConnection* connection) {
        assert(data_.empty());

        // Read as much as possible into data_
        data_.resize(4096, '\0');
        assert(connection == connection_);
        int bytes = connection_->read(base::stringArray(&data_), data_.size());
        if (bytes == 0 || bytes == -1) {
            // Connection closed: clean it up
            data_.clear();
            delete connection_;
            connection_ = NULL;
        } else {
            // Typically we would need a loop
            assert(0 < bytes && bytes < 4096);
            data_.resize(bytes);
        }
    }

    virtual void writeAvailable(TCPConnection* connection) {
        assert(!write_available_);
        write_available_ = true;
    }

// disable -Wconversion for this function to work around a bug in htons in glibc
// TODO: Remove this eventually
#if defined(__OPTIMIZE__) && __GNUC__ == 4 && __GNUC_MINOR__ >= 3
#pragma GCC diagnostic ignored "-Wconversion"
#endif
    void connectThread() {
        assert(other_ < 0);
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(assert_range_cast<uint16_t>(listener_->port()));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        other_ = connectTCP(addr);
        assert(other_ >= 0);
    }

    bool connect() {
        assert(other_ < 0);
        assert(connection_ == NULL);

        if (!listener_->listen(0)) {
            return false;
        }

        // Connect in another thread
        FunctionThread thread(std::tr1::bind(&TestTCPConnection::connectThread, this));
        int error = event_base_loop(event_loop_.base(), EVLOOP_ONCE);
        thread.join();
        if (error != 0) {
            return false;
        }
        return connection_ != NULL;
    }

    void detachConnection() {
        connection_ = NULL;
        other_ = -1;
    }

    bool closeOtherEnd() {
        // close the other end of the connection
        int error = close(other_);
        other_ = -1;
        if (error != 0) {
            return false;
        }
        return true;
    }

    bool closeConnection() {
        assert(other_ >= 0);
        assert(connection_ != NULL);

        // close the other end of the connection
        if (!closeOtherEnd()) {
            return false;
        }

        int error = event_base_loop(event_loop_.base(), EVLOOP_ONCE);
        return error == 0;
    }

    io::LibEventLoop event_loop_;
    TCPListener* listener_;
    TCPConnection* connection_;

    std::string data_;

    int other_;
    bool write_available_;
};

#endif
