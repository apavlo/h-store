// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstring>

#include <signal.h>

#include "base/assert.h"
#include "io/libeventloop.h"
#include "libevent/event.h"
#include "stupidunit/stupidunit.h"

static bool sent_sigint = false;
static void sendSigint(int fd, short type, void* argument) {
    sent_sigint = true;
    assert(type == EV_TIMEOUT);
    kill(getpid(), SIGINT);
}

class LibEventLoopTest : public Test {
public:
    LibEventLoopTest() {
        memset(&event_, 0, sizeof(event_));
    }

    ~LibEventLoopTest() {
        // This can fail if the event was never used
        event_del(&event_);
    }

    void setSendSigint(const timeval& timeout) {
        // remove the timer in case it has already been set
        evtimer_del(&event_);

        evtimer_set(&event_, sendSigint, NULL);
        int error = event_base_set(event_loop_.base(), &event_);
        assert(error == 0);
        error = event_add(&event_, &timeout);
        assert(error == 0);
    }

    io::LibEventLoop event_loop_;
    event event_;
};

TEST_F(LibEventLoopTest, ExitOnSigInt) {
    event_loop_.exitOnSigInt(false);
    event_loop_.exitOnSigInt(true);
    event_loop_.exitOnSigInt(true);

    // Install a callback to be invoked from the event loop, which will send sigint
    static const timeval timeout = { 0, 0 };
    setSendSigint(timeout);
    event_loop_.run();
    EXPECT_TRUE(sent_sigint);
    sent_sigint = false;
    // The handler should persist
    setSendSigint(timeout);
    event_loop_.run();
    EXPECT_TRUE(sent_sigint);
    sent_sigint = false;

    // Set exit on sig int to false: this will kill our process instead
    event_loop_.exitOnSigInt(false);
    setSendSigint(timeout);
    // kqueue does not survive a fork: reinit the handlers in the child
    EXPECT_DEATH(event_reinit(event_loop_.base()); event_loop_.run());
}

static void* idle_one_arg = NULL;
static bool idleOne(void* argument) {
    idle_one_arg = argument;
    return true;
}

static void* idle_two_arg = NULL;
static bool idleTwo(void* argument) {
    // This should never get called
    idle_two_arg = argument;
    return false;
}

TEST_F(LibEventLoopTest, BadAddRemoveCallback) {
    // NULL handler
    EXPECT_DEATH(event_loop_.addIdleCallback(NULL, NULL));
    // Not registered
    EXPECT_DEATH(event_loop_.removeIdleCallback(idleOne));
}

TEST_F(LibEventLoopTest, IdleCallback) {
    // Add a timeout in the future
    const timeval timeout = { 100, 0 };
    setSendSigint(timeout);

    // No idle callback registered: no callback
    int error = event_base_loop(event_loop_.base(), EVLOOP_NONBLOCK);
    assert(error == 0);
    EXPECT_EQ(NULL, idle_one_arg);

    event_loop_.addIdleCallback(idleTwo, NULL);
    event_loop_.removeIdleCallback(idleTwo);
    event_loop_.addIdleCallback(idleOne, &event_loop_);

    error = event_base_loop(event_loop_.base(), EVLOOP_NONBLOCK);
    assert(error == 0);
    EXPECT_EQ(&event_loop_, idle_one_arg);
    EXPECT_EQ(NULL, idle_two_arg);

    idle_one_arg = NULL;
    event_loop_.removeIdleCallback(idleOne);
    error = event_base_loop(event_loop_.base(), EVLOOP_NONBLOCK);
    assert(error == 0);
    EXPECT_EQ(NULL, idle_one_arg);
    EXPECT_EQ(NULL, idle_two_arg);

    // Enable both callbacks
    event_loop_.addIdleCallback(idleTwo, (void*) &timeout);
    event_loop_.addIdleCallback(idleOne, &event_loop_);
    error = event_base_loop(event_loop_.base(), EVLOOP_NONBLOCK);
    assert(error == 0);
    EXPECT_EQ(&event_loop_, idle_one_arg);
    EXPECT_EQ(&timeout, idle_two_arg);

    // TODO: Test that true/false blocking/not blocking works correctly
}

bool timeOut = false;
static void timeOutCallback(void* argument) {
    assert(!timeOut);
    ((io::LibEventLoop*) argument)->exit();
    timeOut = true;
}

// Tests both timeouts and exiting the event loop
TEST_F(LibEventLoopTest, TimeOutAndExit) {
    void* handle = event_loop_.createTimeOut(100, timeOutCallback, &event_loop_);
    EXPECT_NE(NULL, handle);

    // run the loop: we should get the callback and exit
    event_loop_.run();
    EXPECT_TRUE(timeOut);

    // time outs happen once: this will die with sigint
    static const timeval timeout = { 0, 200000 };
    setSendSigint(timeout);
    EXPECT_DEATH(event_reinit(event_loop_.base()); event_loop_.run());

    event_loop_.cancelTimeOut(handle);
}

TEST_F(LibEventLoopTest, TimeOutReset) {
    // restart the timeout: this will work
    timeOut = false;
    void* handle = event_loop_.createTimeOut(50, timeOutCallback, &event_loop_);
    event_loop_.resetTimeOut(handle, 100);
    static const timeval timeout = { 0, 300000 };
    setSendSigint(timeout);
    event_loop_.run();
    EXPECT_TRUE(timeOut);

    // restart then cancel: this should die
    event_loop_.resetTimeOut(handle, 100);
    event_loop_.cancelTimeOut(handle);
    EXPECT_DEATH(event_reinit(event_loop_.base()); event_loop_.run());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
