// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "mockmessageconnection.h"
#include "net/messageserver.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

class NullMessage {
public:
    void parseFromString(const char* start, const char* end) {
        assert(end > start);
        message_.assign(start, end-start);
    }

    static int32_t typeCode() {
        return TYPE_CODE;
    }

    void appendToString(string* s) const {
        s->append(message_);
    }

    void serialize(io::FIFOBuffer* o) const {
        o->copyIn(message_.data(), (int) message_.size());
    }

    string message_;
    static const int32_t TYPE_CODE = 0;
};

class LameCallback {
public:
    LameCallback() : connection_(NULL) {}

    void callback(net::ConnectionHandle* connection, const NullMessage& argument) {
        connection_ = connection;
        message_ = argument.message_;
    }

    net::ConnectionHandle* connection_;
    string message_;
};

class MessageServerTest : public Test {
public:
    MessageServerTest() : connection_(new MockMessageConnection()) {
        server_.connectionArrived(connection_);
        server_.addCallback(&LameCallback::callback, &target_);
        message_.message_ = "foo";
    }

    void messageReceived(MessageConnection* connection, const string& message) {
        server_.messageReceived(connection, NullMessage::TYPE_CODE, message);
    }

    MockMessageConnection* connection_;
    net::MessageServer server_;

    LameCallback target_;
    NullMessage message_;
};

TEST_F(MessageServerTest, BadAdd) {
    EXPECT_DEATH(server_.addCallback(&LameCallback::callback, (LameCallback*) NULL));
    typedef void (LameCallback::*MemberFPtr)(net::ConnectionHandle*, const NullMessage&);
    EXPECT_DEATH(server_.addCallback((MemberFPtr) NULL, &target_));

    // This message type is already registered
    EXPECT_DEATH(server_.addCallback(&LameCallback::callback, &target_));
}

TEST_F(MessageServerTest, BadMessageReceived) {
    MockMessageConnection unconnected;
    EXPECT_DEATH(messageReceived(&unconnected, "foo"));
    EXPECT_DEATH(server_.messageReceived(connection_, NullMessage::TYPE_CODE+1, "foo"));
}

TEST_F(MessageServerTest, Registered) {
    EXPECT_TRUE(server_.registered(NullMessage::typeCode()));
    EXPECT_FALSE(server_.registered(NullMessage::typeCode()+1));
    EXPECT_TRUE(server_.registered<NullMessage>());
}

TEST_F(MessageServerTest, MessageReceived) {
    const string message = "foo";
    messageReceived(connection_, message);
    EXPECT_EQ(connection_, reinterpret_cast<MockMessageConnection*>(target_.connection_));
    EXPECT_EQ(message, target_.message_);
}

TEST_F(MessageServerTest, SendMessages) {
    server_.send(reinterpret_cast<net::ConnectionHandle*>(connection_), message_);
    EXPECT_EQ(message_.message_, connection_->message_.substr(8, string::npos));

    // Close the connection and send again
    server_.connectionClosed(connection_);

    // This silently drops the message, not cause death
    // TODO: Better error handling? Reconnect? Other?
    server_.send(reinterpret_cast<net::ConnectionHandle*>(connection_), message_);
}

TEST_F(MessageServerTest, AddConnections) {
    vector<MockMessageConnection*> connections;
    connections.push_back(new MockMessageConnection());
    connections.push_back(new MockMessageConnection());

    vector<net::ConnectionHandle*> handles = server_.addConnections(
            vector<MessageConnection*>(connections.begin(), connections.end()));
    ASSERT_EQ(handles.size(), connections.size());

    // Sending messages to the handles works
    for (size_t i = 0; i < handles.size(); ++i) {
        server_.send(handles[i], message_);
        EXPECT_EQ(message_.message_, connections[i]->message_.substr(8, string::npos));
    }

    // Receiving messages from the handle works
    for (size_t i = 0; i < handles.size(); ++i) {
        connections[i]->triggerCallback(message_);
        EXPECT_EQ(message_.message_, target_.message_);
    }

    // Closing the connection from the remote end works
    // TODO: This should eventually try to reconnect?
    for (size_t i = 0; i < handles.size(); ++i) {
        server_.connectionClosed(connections[i]);
        EXPECT_DEATH(server_.connectionClosed(connections[i]));
    }
}

TEST_F(MessageServerTest, RemoveCallback) {
    server_.removeCallback(&LameCallback::callback);

    // Now this message is unhandled
    const string message = "foo";
    EXPECT_DEATH(messageReceived(connection_, message));

    // Re-registering makes it work
    server_.addCallback(&LameCallback::callback, &target_);
    messageReceived(connection_, message);
    EXPECT_EQ(connection_, reinterpret_cast<MockMessageConnection*>(target_.connection_));
    EXPECT_EQ(message, target_.message_);
}

TEST_F(MessageServerTest, LocalCloseConnection) {
    MockMessageConnection* connect = new MockMessageConnection();
    net::ConnectionHandle* handle = server_.addConnection(connect);
    server_.closeConnection(handle);

    // Closing the connection again is not a problem since we don't get notification when
    // connections close.
    // TODO: This *should* be a problem when we attempt to reconnect
    server_.closeConnection(handle);
}

TEST_F(MessageServerTest, BufferedSendFlush) {
    server_.bufferedSend(reinterpret_cast<net::ConnectionHandle*>(connection_), message_);
    EXPECT_FALSE(connection_->hasMessage());

    server_.flush(reinterpret_cast<net::ConnectionHandle*>(connection_));
    EXPECT_EQ(message_.message_, connection_->message_.substr(8, string::npos));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
