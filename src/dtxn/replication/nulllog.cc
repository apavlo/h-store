// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "replication/nulllog.h"

#include "io/buffer.h"
#include "io/message.h"

namespace replication {

NullLog::~NullLog() {}

int NullLog::submit(const io::Message& entry, void* callback_arg) {
    // NOTE: serialized_entry is local so this works in case of re-entrant calls
    entry.serialize(&buffer_);
    std::string serialized_entry;
    buffer_.copyOut(&serialized_entry, buffer_.available());
    buffer_.clear();

    // Increment sequence before calling callback in case of re-entrant calls
    int sequence = sequence_;
    sequence_ += 1;

    nextLogEntryCallback(sequence, serialized_entry, callback_arg);
    return sequence;
}

void NullLog::setCallbackArg(int sequence, void* callback_arg) {
    assert(false);
}

bool NullLog::isPrimary() const {
    return true;
}

void NullLog::flush() {}

}
