// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef IO_LIBEVENTLOOP_H__
#define IO_LIBEVENTLOOP_H__

#include "io/eventloop.h"

struct event_base;
struct event;

namespace io {

/** Creates a libevent event loop. */
class LibEventLoop : public EventLoop {
public:
    LibEventLoop();
    virtual ~LibEventLoop();

    virtual void* createTimeOut(int milliseconds, TimeOutCallback callback, void* argument);
    virtual void resetTimeOut(void* handle, int milliseconds);
    virtual void cancelTimeOut(void* handle);
    virtual void exitOnSigInt(bool value);
    virtual void exit();
    virtual void run();
    virtual void runInEventLoop(google::protobuf::Closure* callback);

    struct event_base* base() { return base_; }

protected:
    virtual void enableIdleCallback(bool enable);

private:
    static int libeventIdleCallback(struct event_base* event_loop, void* argument);

    event_base* base_;
    event* sigint_event_;
};

}

#endif
