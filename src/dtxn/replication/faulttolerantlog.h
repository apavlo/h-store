// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef REPLICATION_FAULTTOLERANTLOG_H__
#define REPLICATION_FAULTTOLERANTLOG_H__

#include <cassert>
#include <string>

namespace io {
class Message;
}

namespace replication {

// Callback for the fault tolerant log.
// TODO: Use std::function? something else?
class FaultTolerantLogCallback {
public:
    virtual ~FaultTolerantLogCallback() {}

    // Called when entry is decided to be the next log entry.
    virtual void nextLogEntry(int sequence, const std::string& entry, void* arg) = 0;
};


// Interface to the replication system. Any number of operations can be "in-flight." Once they are
// decided, according to the policy of the underlying implementation, callback is invoked. Log
// sequence numbers begin at zero. Log entries cannot be empty.
class FaultTolerantLog {
public:
    FaultTolerantLog() : entry_callback_(NULL) {}
    virtual ~FaultTolerantLog() {}

    void entry_callback(FaultTolerantLogCallback* callback) {
        entry_callback_ = callback;
    }

    // Submit an entry to the log. callback_arg will be passed back to the callback. Returns the
    // tentative log sequence number.
    // TODO: Supply a per-operation callback instead of an argument?
    // TODO: Return an "operation" handle?
    virtual int submit(const io::Message& entry, void* callback_arg) = 0;

    // Change the callback argument for a pending entry with tentative sequence number.
    // TODO: Move some of the implementation of this stuff into this base class?
    virtual void setCallbackArg(int sequence, void* callback_arg) = 0;

    // Returns true if this log is the primary. This is technically an implementation detail, but
    // since all the implementations will have a primary, it might as well be here.
    virtual bool isPrimary() const = 0;

    // Flushes any buffered messages.
    // TODO: Figure out the best policy and make this unnecessary? Its gross.
    virtual void flush() = 0;

protected:
    void nextLogEntryCallback(int sequence, const std::string& entry, void* argument) {
        assert(!entry.empty());
        entry_callback_->nextLogEntry(sequence, entry, argument);
    }

private:
    FaultTolerantLogCallback* entry_callback_;
};

}

#endif
