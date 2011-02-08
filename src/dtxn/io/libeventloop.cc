// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cassert>
#include <cstdio>

#include <signal.h>

#include "base/assert.h"
#include "io/libeventloop.h"
#include "libevent/include/event2/event.h"
#include "libevent/include/event2/thread.h"
#include "libevent/include/event2/event_struct.h"
#include "google/protobuf/stubs/common.h"

using google::protobuf::Closure;

namespace io {

LibEventLoop::LibEventLoop() : sigint_event_(NULL) {
    int error = evthread_use_pthreads();
    ASSERT(error == 0);
    base_ = event_base_new();
    ASSERT(base_ != NULL);
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
        int error = event_assign(&event_, event_loop->base(), -1, EV_TIMEOUT, libeventCallback, this);
        ASSERT(error == 0);
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

        sigint_event_ = evsignal_new(base_, SIGINT, sigintHandler, base_);
        int error = event_add(sigint_event_, NULL);
        ASSERT(error == 0);
    } else {
        if (sigint_event_ == NULL) return;

        int error = event_del(sigint_event_);
        ASSERT(error == 0);
        event_free(sigint_event_);
        sigint_event_ = NULL;
    }
}

void LibEventLoop::exit() {
    int error = event_base_loopbreak(base_);
    ASSERT(error == 0);
}

void LibEventLoop::run() {
    // run the loop and block even if there are no events: other threads may add them
    event_base_loop(base_, EVLOOP_BLOCK_FOR_EXPLICIT_EXIT);
}

class LibEventClosureWrapper {
public:
    static LibEventClosureWrapper* New(event_base* base, Closure* callback) {
        return new LibEventClosureWrapper(base, callback);
    }

    void activate() {
        event_active(&event_, EV_WRITE, 1);
    }

private:
    LibEventClosureWrapper(event_base* base, Closure* callback) : callback_(callback) {
        assert(callback_ != NULL);
        int error = event_assign(&event_, base, -1, 0, libeventCallback, this);
        ASSERT(error == 0);
    }

    static void libeventCallback(int fd, short type, void* argument) {
        ASSERT(fd == -1);
        ASSERT(type == EV_WRITE);
        LibEventClosureWrapper* wrapper = reinterpret_cast<LibEventClosureWrapper*>(argument);
        Closure* callback = wrapper->callback_;
        delete wrapper;

        callback->Run();
    }

    Closure* callback_;
    event event_;
};

void LibEventLoop::runInEventLoop(Closure* callback) {
    // TODO: Use a recycle list to avoid allocation/deallocation? But it needs to be thread-safe
    LibEventClosureWrapper* wrapper = LibEventClosureWrapper::New(base_, callback);
    wrapper->activate();
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
