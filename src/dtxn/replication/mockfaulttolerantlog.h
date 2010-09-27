// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef REPLICATION_MOCKFAULTTOLERANTLOG_H__
#define REPLICATION_MOCKFAULTTOLERANTLOG_H__

#include <vector>

#include "io/buffer.h"
#include "io/message.h"
#include "replication/faulttolerantlog.h"

namespace replication {

// A mock fault-tolerant log for unit tests.
class MockFaultTolerantLog : public FaultTolerantLog {
public:
    MockFaultTolerantLog() : is_primary_(false), flush_count_(0) {}
    virtual ~MockFaultTolerantLog() {}

    void is_primary(bool value) { is_primary_ = value; }

    // Make this public
    using FaultTolerantLog::nextLogEntryCallback;

    void callbackSubmitted(int index) {
        assert(0 <= index && index < submitted_.size());
        nextLogEntryCallback(index, submitted_[index], args_[index]);
    }

    // Implementation of FaultTolerantLog.
    virtual int submit(const io::Message& operation, void* argument) {
        io::FIFOBuffer buffer;
        operation.serialize(&buffer);
        const void* read;
        int length;
        buffer.readBuffer(&read, &length);

        submitted_.push_back(std::string());
        submitted_.back().assign((const char*) read, length);

        buffer.readBuffer(&read, &length);
        assert(read == NULL && length == 0);

        args_.push_back(argument);
        return (int) submitted_.size()-1;
    }
    virtual void setCallbackArg(int sequence, void* argument) {
        assert(sequence < args_.size());
        args_[sequence] = argument;
    }
    virtual bool isPrimary() const { return is_primary_; }

    virtual void flush() {
        flush_count_ += 1;
    }

    bool is_primary_;
    std::vector<std::string> submitted_;
    std::vector<void*> args_;
    int flush_count_;
};

}
#endif
