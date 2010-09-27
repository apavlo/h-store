// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef REPLICATION_NULLLOG_H__
#define REPLICATION_NULLLOG_H__

#include "io/buffer.h"
#include "replication/faulttolerantlog.h"

namespace replication {

// A FaultTolerantLog implementation that "fakes" replication by immediately triggering callbacks.
class NullLog : public FaultTolerantLog {
public:
    NullLog() : sequence_(0) {}
    virtual ~NullLog();

    virtual int submit(const io::Message& entry, void* callback_arg);
    virtual void setCallbackArg(int sequence, void* callback_arg);
    virtual bool isPrimary() const;
    virtual void flush();

private:
    int sequence_;

    // Cached members to avoid allocation / dealloction in ::submit
    io::FIFOBuffer buffer_;
};

}

#endif
