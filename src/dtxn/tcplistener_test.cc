// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <sys/types.h>
#include <sys/wait.h>

#include <cstring>

#include "messageconnection.h"
#include "strings/utils.h"
#include "stupidunit/stupidunit.h"
#include "tcplistener.h"
#include "testtcpconnection.h"

using std::string;

static const char FOO_STRING[] = "foo";

class TCPListenerTest : public Test, public TestTCPConnection {
public:
    bool writeFoo() {
        ssize_t bytes = write(other_, FOO_STRING, sizeof(FOO_STRING)-1);
        return bytes == sizeof(FOO_STRING)-1;
    }

    bool fillConnection() {
        // Make buffer a weird size so the last write is partial
        char buffer[4090];
        memset(buffer, 0, sizeof(buffer));  // makes valgrind happy
        int bytes = sizeof(buffer);
        while (bytes == sizeof(buffer)) {
            bytes = connection_->write(buffer, sizeof(buffer));
        }

        return 0 <= bytes && bytes < sizeof(buffer);
    }

    bool readFromOther() {
        char buffer[4096];
        ssize_t bytes = read(other_, buffer, sizeof(buffer));
        return bytes > 0;
    }

    bool readAllFromOther(string* out) {
        out->clear();

        TCPConnection connect(other_);
        char buffer[4096];
        int bytes = connect.read(buffer, sizeof(buffer));
        while (bytes > 0) {
            out->append(buffer, bytes);
            bytes = connect.read(buffer, sizeof(buffer));
        }

        other_ = -1;  // connection destructor will close the connection
        return bytes == -1;
    }
};

TEST_F(TCPListenerTest, BadCreate) {
    EXPECT_DEATH(TCPListener listener(event_loop_.base(), NULL));
    EXPECT_DEATH(TCPListener listener(NULL, this));
}

TEST_F(TCPListenerTest, NoPort) {
    EXPECT_EQ(0, listener_->port());
}

TEST_F(TCPListenerTest, ConnectClose) {
    ASSERT_EQ(true, connect());
    ASSERT_EQ(true, closeConnection());
    EXPECT_EQ(NULL, connection_);
}

TEST_F(TCPListenerTest, ReadAvailable) {
    ASSERT_EQ(true, connect());

    // Reading = 0
    char buffer[128];
    EXPECT_EQ(0, connection_->read(buffer, sizeof(buffer)));
    EXPECT_EQ(0, connection_->read(buffer, sizeof(buffer)));

    // Write into the socket
    ASSERT_EQ(true, writeFoo());

    ASSERT_EQ(0, event_base_loop(event_loop_.base(), EVLOOP_ONCE));
    EXPECT_EQ(FOO_STRING, data_);
    data_.clear();
}

TEST_F(TCPListenerTest, WriteAvailable) {
    ASSERT_EQ(true, connect());

    // write data into the socket until it is full
    ASSERT_EQ(true, fillConnection());

    // Read some data out the other end: the socket should become available for writing
    ASSERT_EQ(true, readFromOther());

    ASSERT_FALSE(write_available_);
    ASSERT_EQ(0, event_base_loop(event_loop_.base(), EVLOOP_ONCE));
    EXPECT_EQ(true, write_available_);
}

TEST_F(TCPListenerTest, ClearTargetPendingEvents) {
    ASSERT_EQ(true, connect());

    // Write into the socket, fill the other end
    ASSERT_EQ(true, writeFoo());
    ASSERT_EQ(true, fillConnection());

    // Disable the callback target, run the event loop. Pending events should not cause problems.
    connection_->setTarget(NULL, NULL);
    ASSERT_EQ(true, readFromOther());
    ASSERT_EQ(0, event_base_loop(event_loop_.base(), EVLOOP_NONBLOCK));
    EXPECT_EQ(true, data_.empty());
}

TEST_F(TCPListenerTest, SetNewTarget) {
    ASSERT_EQ(true, connect());

    class OtherTarget : public TCPConnectionCallback {
    public:
        OtherTarget() : read_available_(false) {}

        virtual void connectionArrived(TCPConnection* connection) {
            assert(false);
        }

        virtual void readAvailable(TCPConnection* connection) {
            assert(!read_available_);
            read_available_ = true;
        }

        virtual void writeAvailable(TCPConnection* connection) {
            assert(false);
        }
    
        bool read_available_;
    };

    OtherTarget other;
    connection_->setTarget(event_loop_.base(), &other);

    // Write into the socket
    ASSERT_EQ(true, writeFoo());

    ASSERT_EQ(0, event_base_loop(event_loop_.base(), EVLOOP_ONCE));
    EXPECT_EQ(true, other.read_available_);
    EXPECT_EQ(true, data_.empty());
}

TEST_F(TCPListenerTest, WriteOnClosedSocket) {
    ASSERT_EQ(true, listener_->listen(0));

    // Connect in another process which immediately calls exit.
    // This causes a future write to yield ECONNRESET.
    pid_t pid = fork();
    if (pid == 0) {
        connectThread();
        exit(42);
    }
    ASSERT_GT(pid, 0);

    ASSERT_EQ(0, event_base_loop(event_loop_.base(), EVLOOP_ONCE));
    int status;
    ASSERT_EQ(pid, waitpid(pid, &status, 0));
    ASSERT_EQ(42, WEXITSTATUS(status));

    // Write into the socket. On Linux we need to write twice to get the error. On Mac OS X, twice
    // *normally* works but occasionally takes longer, particularly when other things are using
    // sockets.
    static const char DATA[] = "m";
    int count = 0;
    while (count < 100) {
        count += 1;
        int result = connection_->write(DATA, sizeof(DATA));
        if (result == -1) {
            break;
        }
        ASSERT_EQ(sizeof(DATA), result);
    }
    EXPECT_LE(2, count);
    EXPECT_LT(count, 100);

    delete connection_;
}

// TODO: Refactor the TCP test infrastructure into some common component. These tests should be in
// messageconnection_test.cc.
TEST_F(TCPListenerTest, TCPMessageConnectionFillConnection) {
    ASSERT_EQ(true, connect());

    // write data into the socket until it is full
    ASSERT_EQ(true, fillConnection());

    TCPMessageConnection m(event_loop_.base(), connection_);

    // write a message
    static const char DATA[] = "message";
    EXPECT_TRUE(m.rawSend(42, DATA, sizeof(DATA) - 1));

    // Read some data out the other end: the socket should become available for writing
    ASSERT_EQ(true, readFromOther());

    // Triggers the TCPMessageConnection callback to write data
    EXPECT_FALSE(write_available_);
    ASSERT_EQ(0, event_base_loop(event_loop_.base(), EVLOOP_ONCE));
    EXPECT_FALSE(write_available_);

    string out;
    readAllFromOther(&out);
    EXPECT_TRUE(strings::endsWith(out, DATA));

    // Local connection should be closed
    char buffer[128];
    EXPECT_EQ(-1, connection_->read(buffer, sizeof(buffer)));
}

TEST_F(TCPListenerTest, TCPMessageConnectionWriteHuge) {
    ASSERT_TRUE(connect());

    // write a huge message: it should work
    char data[io::FIFOBuffer::PAGE_DATA_SIZE + 100];
    memset(data, 0x42, sizeof(data));
    TCPMessageConnection m(event_loop_.base(), connection_);
    EXPECT_TRUE(m.rawSend(42, data, sizeof(data)));

    // Read multiple chunks of data from the other end
    string out;
    readAllFromOther(&out);
    ASSERT_EQ(sizeof(data) + 2 * sizeof(int32_t), out.size());
    EXPECT_EQ(0, memcmp(out.data() + 2 * sizeof(int32_t), data, sizeof(data)));
}

TEST_F(TCPListenerTest, TCPMessageConnectionWriteError) {
    ASSERT_EQ(true, connect());

    closeOtherEnd();

    TCPMessageConnection m(event_loop_.base(), connection_);

    // write a message
    static const char DATA[] = "message";
    // See comment in WriteOnClosedSocket.
    bool triggered = false;
    for (int i = 0; !triggered && i < 100; ++i) {
        if (!m.rawSend(42, DATA, sizeof(DATA))) {
            triggered = true;
        }
    }
    EXPECT_TRUE(triggered);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
