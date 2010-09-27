// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "io/eventloop.h"
#include "mockmessageconnection.h"
#include "net/messageserver.h"
#include "replication/primarybackuplistener.h"
#include "stupidunit/stupidunit.h"

using std::vector;

class PrimaryBackupListenerTest : public Test {
public:
    PrimaryBackupListenerTest() : listener_(&event_loop_, &msg_server_) {}

    io::MockEventLoop event_loop_;
    net::MessageServer msg_server_;
    replication::PrimaryBackupListener listener_;
    vector<MessageConnection*> connections_;
};

TEST_F(PrimaryBackupListenerTest, SetPrimaryEmpty) {
    EXPECT_DEATH(listener_.setPrimary(connections_));
    connections_.push_back(NULL);
    EXPECT_DEATH(listener_.setPrimary(connections_));
}

TEST_F(PrimaryBackupListenerTest, DestructorRemoveIdleCallback) {
    io::MockEventLoop event_loop;
    net::MessageServer msg_server;
    
    EXPECT_FALSE(event_loop.idle_callback_enabled_);
    {
        replication::PrimaryBackupListener listener_(&event_loop, &msg_server);
        EXPECT_TRUE(event_loop.idle_callback_enabled_);
    }
    EXPECT_FALSE(event_loop.idle_callback_enabled_);
}

TEST_F(PrimaryBackupListenerTest, UnbufferedSend) {
    MockMessageConnection* connection = new MockMessageConnection();
    connections_.push_back(connection);
    listener_.setPrimary(connections_);

    replication::Entry entry;
    EXPECT_FALSE(connection->hasMessage());
    listener_.send(1, entry);
    EXPECT_TRUE(connection->hasMessage());
}

TEST_F(PrimaryBackupListenerTest, BadFlush) {
    MockMessageConnection* connection = new MockMessageConnection();
    connections_.push_back(connection);
    listener_.setPrimary(connections_);

    EXPECT_DEATH(listener_.flush());
    listener_.buffered(true);
    listener_.flush();
}

TEST_F(PrimaryBackupListenerTest, BufferedSend) {
    MockMessageConnection* connection = new MockMessageConnection();
    connections_.push_back(connection);
    listener_.setPrimary(connections_);

    listener_.buffered(true);
    replication::Entry entry;
    EXPECT_FALSE(connection->hasMessage());
    listener_.send(1, entry);
    EXPECT_FALSE(connection->hasMessage());

    listener_.flush();
    EXPECT_TRUE(connection->hasMessage());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
