// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef MESSAGECONNECTION_H__
#define MESSAGECONNECTION_H__

#include <cassert>
#include <cstring>
#include <string>

#include "base/assert.h"
#include "base/cast.h"
#include "buffer.h"
#include "io/buffer.h"
#include "tcplistener.h"

class MessageConnection;
class TCPConnection;

namespace io {
class MemoryOutputStream;
}

// Parses the MessageConnection protocol in an event-driven fashion.
class MessageConnectionParser {
public:
    MessageConnectionParser() {
        reset();
    }

    void reset() {
        has_type_ = false;
        has_length_ = false;
        expected_length_ = 0;
    }

    // Returns true if a message was parsed from buffer into message.
    bool parseMessage(ReadBuffer* buffer, int32_t* type, std::string* message);

private:
    bool has_type_;
    bool has_length_;
    int32_t type_;
    int32_t expected_length_;
};

// Callbacks for a connection that sends/receives entire messages.
class MessageConnectionCallback {
public:
    virtual ~MessageConnectionCallback() {}

    // A message was received from connection.
    virtual void messageReceived(MessageConnection* connection, int32_t type, const std::string& message) = 0;

    // A new connection has arrived. connection must be deleted by the callee.
    virtual void connectionArrived(MessageConnection* connection) = 0;

    // The connection has been closed due to the other end or an error. connection can be deleted.
    virtual void connectionClosed(MessageConnection* connection) = 0;
};

class MessageConnection {
public:
    MessageConnection() : target_(NULL), state_(NULL) {}

    virtual ~MessageConnection() {}

    // Returns true if the send is successful. The send can only fail because the connection died.
    bool rawSend(int32_t type, const void* data, int length) {
        // Write length and type
        out_buffer_.copyIn(&type, sizeof(type));
        int32_t length_int32 = assert_range_cast<int32_t>(length);
        out_buffer_.copyIn(&length_int32, sizeof(length_int32));
        out_buffer_.copyIn(data, length);

        return flush();
    }

    // Buffers message to be sent later.
    template <typename T>
    void bufferedSend(const T& message) {
        // Write the type
        int32_t type = message.typeCode();
        out_buffer_.copyIn(&type, sizeof(type));

        // Reserve space for the length
        void* lengthPointer = out_buffer_.writeExact(sizeof(int32_t));

        // Serialize the message
        int available_before = out_buffer_.available();
        message.T::serialize(&out_buffer_);
        int32_t length = assert_range_cast<int32_t>(out_buffer_.available() - available_before);
        assert(length >= 0);

        // update the reserved length
        memcpy(lengthPointer, &length, sizeof(length));
    }

    // Returns true if the send is successful. The send can only fail because the connection died.
    template <typename T>
    bool send(const T& message) {
        bufferedSend(message);
        // send the data immediately
        return flush();
    }

    // Set the callback target. This object does not own target.
    void setTarget(MessageConnectionCallback* target) { target_ = target; }

    // Returns this object's opaque state pointer, containing per-connection state.
    void* state() { return state_; }
    // Stores an opaque state pointer in this object, used to attach per-connection state.
    void state(void* next_state) { state_ = next_state; }

    // Implement this to write a message over the connection. It must write bytes from out_buffer_.
    // Returns true if the send is successful. The send can only fail because the connection died
    // or is closed.
    virtual bool flush() = 0;

protected:
    MessageConnectionCallback* target_;
    // TODO: Refactor to remove this or make it not protected
    io::FIFOBuffer out_buffer_;

private:
    void* state_;
};

// A MessageConnection implemented using a TCP connection.
class TCPMessageConnection : public MessageConnection, public TCPConnectionCallback {
public:
    // Owns connection.
    TCPMessageConnection(event_base* event_loop, TCPConnection* connection);

    virtual ~TCPMessageConnection();

    virtual void readAvailable(TCPConnection* connection);
    virtual void writeAvailable(TCPConnection* connection);

    virtual bool flush();

private:
    TCPConnection* connection_;
    ReadBuffer buffer_;
    MessageConnectionParser parser_;
};

// Wraps a TCP listener, creating MessageConnections instead of TCPConnections.
class MessageListener : public TCPListenerCallback {
public:
    MessageListener(event_base* event_loop, MessageConnectionCallback* target) :
            listener_(event_loop, this), target_(target) {
        assert(target_ != NULL);
    }

    bool listen(int p) {
        return listener_.listen(p);
    }

    int port() const {
        return listener_.port();
    }

    virtual void connectionArrived(TCPConnection* connection) {
        TCPMessageConnection* m = new TCPMessageConnection(listener_.event_loop(), connection);
        m->setTarget(target_);
        target_->connectionArrived(m);
    }

private:
    TCPListener listener_;
    MessageConnectionCallback* target_;
};

#endif
