// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef IO_EVENTLOOP_H__
#define IO_EVENTLOOP_H__

#include <cassert>
#include <vector>

namespace google { namespace protobuf {
class Closure;
}}  // namespace google::protobuf

namespace io {

/** Interface to a generic event loop. This permits libevent code to be unit tested. */
class EventLoop {
public:
    virtual ~EventLoop() {}

    typedef bool (*IdleCallback)(void* argument);
    /** An idle callback is called each time through the event loop, after delivering all read/write
    callbacks and before calling epoll again. */
    // TODO: Maybe calling it an "idle" callback is a misnomer?
    void addIdleCallback(IdleCallback handler, void* argument) {
        assert(handler != NULL);
        idle_callbacks_.push_back(handler);
        idle_arguments_.push_back(argument);
        if (idle_callbacks_.size() == 1) {
            enableIdleCallback(true);
        }
    }

    void removeIdleCallback(IdleCallback handler) {
        for (size_t i = 0; i < idle_callbacks_.size(); ++i) {
            if (idle_callbacks_[i] == handler) {
                // Found the entry: remove both handlers and arguements
                assert(idle_callbacks_.size() == idle_arguments_.size());
                idle_callbacks_.erase(idle_callbacks_.begin() + i);
                idle_arguments_.erase(idle_arguments_.begin() + i);

                if (idle_callbacks_.empty()) {
                    enableIdleCallback(false);
                }
                return;
            }
        }
        assert(false);
    }

    typedef void (*TimeOutCallback)(void*);
    /** Sets a time out that will call the callback function after milliseconds has passed, with
    argument. Returns an opaque pointer that can be used to cancel the timeout before it is
    triggered. Must call cancelTimeOut to free the timeout data. The timeout will fire once. */
    virtual void* createTimeOut(int milliseconds, TimeOutCallback callback, void* argument) = 0;

    /** Resets the timeout represented by handle to trigger again after milliseconds. */
    virtual void resetTimeOut(void* handle, int milliseconds) = 0;

    /** Cancels and deletes the timeout represented by handle. */
    virtual void cancelTimeOut(void* handle) = 0;

    /** If enabled, SIGINT will be caught and used to exit the event loop. */
    virtual void exitOnSigInt(bool value) = 0;

    /** Causes the event loop to exit at some point in the future. Safe if called by another
    thread.*/
    virtual void exit() = 0;

    /** Runs the event loop. */
    virtual void run() = 0;

    /** Runs callback in the event loop. This can be used to "defer" a callback until later, or
    to run something in the event thread, rather than a sub thread. */
    virtual void runInEventLoop(google::protobuf::Closure* callback) = 0;

protected:
    /** If enable is true, the idle callback should be called. If false it should be turned off. */
    virtual void enableIdleCallback(bool enable) = 0;

    /** The implementation should call this from the idle callback. */
    bool runIdleCallbacks() {
        assert(!idle_callbacks_.empty());
        bool poll = false;
        for (size_t i = 0; i < idle_callbacks_.size(); ++i) {
            poll |= idle_callbacks_[i](idle_arguments_[i]);
        }
        return poll;
    }

private:
    std::vector<IdleCallback> idle_callbacks_;
    std::vector<void*> idle_arguments_;
};

/** A mock EventLoop implementation for unit testing. */
class MockEventLoop : public io::EventLoop {
public:
    MockEventLoop() : last_timeout_(NULL), idle_callback_enabled_(false), should_exit_(false) {}

    struct TimeOutData {
        TimeOutCallback callback_;
        void* argument_;
        int milliseconds_;
    };

    virtual void* createTimeOut(int milliseconds, TimeOutCallback callback, void* argument) {
        assert(milliseconds >= 0);
        assert(callback != NULL);
        
        TimeOutData* timeout = new TimeOutData();
        timeout->callback_ = callback;
        timeout->argument_ = argument;
        timeout->milliseconds_ = milliseconds;
        last_timeout_ = timeout;
        return timeout;
    }

    virtual void resetTimeOut(void* handle, int milliseconds) {
        ((TimeOutData*) handle)->milliseconds_ = milliseconds;
    }

    virtual void cancelTimeOut(void* handle) {
        delete (TimeOutData*) handle;
    }

    virtual void exitOnSigInt(bool value) {
        assert(false);
    }

    virtual void exit() {
        should_exit_ = true;
    }

    virtual void run() {
        if (should_exit_) {
            should_exit_ = false;
            return;
        }
        assert(false);
    }

    virtual void runInEventLoop(google::protobuf::Closure* callback) {
        assert(false);
    }

    bool idle() {
        assert(idle_callback_enabled_);
        return runIdleCallbacks();
    }
    
    void triggerLastTimeout() {
        last_timeout_->callback_(last_timeout_->argument_);
    }

    virtual void enableIdleCallback(bool enable) {
        assert(enable == !idle_callback_enabled_);
        idle_callback_enabled_ = enable;
    }

    TimeOutData* last_timeout_;
    bool idle_callback_enabled_;
    bool should_exit_;
};

}  // namespace io
#endif
