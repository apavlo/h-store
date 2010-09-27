// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cassert>
#include <cstdio>

#include <signal.h>

#include "base/assert.h"
#include "io/libeventloop.h"
#include "libevent/event.h"

namespace io {

LibEventLoop::LibEventLoop() :
        base_(event_base_new()),
        sigint_event_(NULL) {
    assert(base_ != NULL);
}

LibEventLoop::~LibEventLoop() {
    if (sigint_event_ != NULL) {
        exitOnSigInt(false);
    }

    event_base_free(base_);
    base_ = NULL;
}

class LibEventLoopCallback {
public:
    LibEventLoopCallback(LibEventLoop* event_loop, EventLoop::TimeOutCallback callback, void* argument) :
            callback_(callback), callback_argument_(argument) {
        assert(callback_ != NULL);
        evtimer_set(&event_, libeventCallback, this);
        int result = event_base_set(event_loop->base(), &event_);
        ASSERT(result == 0);
    }

    ~LibEventLoopCallback() {
        int result = event_del(&event_);
        ASSERT(result == 0);
    }

    void reset(int milliseconds) {
        assert(milliseconds > 0);
        // delete in case we are active
        int result = event_del(&event_);
        assert(result == 0);

        int seconds = milliseconds / 1000;
        int microseconds = (milliseconds  % 1000) * 1000;
        struct timeval timeout = { seconds, microseconds };
        result = evtimer_add(&event_, &timeout);
        assert(result == 0);
    }

    void call() {
        callback_(callback_argument_);
    }

    static void libeventCallback(int file_handle, short type, void* argument) {
        assert(type == EV_TIMEOUT);
        ((LibEventLoopCallback*) argument)->call();
    }

private:
    event event_;
    EventLoop::TimeOutCallback callback_;
    void* callback_argument_;
};

void* LibEventLoop::createTimeOut(int milliseconds, TimeOutCallback callback, void* argument) {
    LibEventLoopCallback* callback_info = new LibEventLoopCallback(this, callback, argument);
    callback_info->reset(milliseconds);
    return callback_info;
}

void LibEventLoop::resetTimeOut(void* handle, int milliseconds) {
    ((LibEventLoopCallback*) handle)->reset(milliseconds);
}

void LibEventLoop::cancelTimeOut(void* handle) {
    delete (LibEventLoopCallback*) handle;
}

static void sigintHandler(int signal, short type, void* argument) {
    assert(signal == SIGINT);
    assert(type == EV_SIGNAL);
    printf("sigint\n");
    event_base* event_loop = reinterpret_cast<event_base*>(argument);
    event_base_loopexit(event_loop, NULL);
}

void LibEventLoop::exitOnSigInt(bool value) {
    if (value) {
        if (sigint_event_ != NULL) return;

        sigint_event_ = new event();
        //~ event_set(sigint_event_, SIGINT, EV_SIGNAL, sigintHandler, base_);
        signal_set(sigint_event_, SIGINT, sigintHandler, base_);
        int error = event_base_set(base_, sigint_event_);
        ASSERT(error == 0);
        error = event_add(sigint_event_, NULL);
        ASSERT(error == 0);
    } else {
        if (sigint_event_ == NULL) return;

        int error = event_del(sigint_event_);
        ASSERT(error == 0);
        delete sigint_event_;
        sigint_event_ = NULL;
    }
}

void LibEventLoop::exit() {
    int error = event_base_loopexit(base_, NULL);
    ASSERT(error == 0);
}

void LibEventLoop::run() {
    event_base_dispatch(base_);
}

void LibEventLoop::enableIdleCallback(bool enable) {
    if (enable) {
        event_base_set_idle(base_, &libeventIdleCallback, this);
    } else {
        event_base_set_idle(base_, NULL, NULL);
    }
}


int LibEventLoop::libeventIdleCallback(struct event_base* event_loop, void* argument) {
    LibEventLoop* loop = reinterpret_cast<LibEventLoop*>(argument);
    return loop->runIdleCallbacks();
}

}
