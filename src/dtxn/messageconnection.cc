// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "messageconnection.h"

#include "base/stringutil.h"
#include "io/buffer.h"

using std::string;

// Used for parsing integers in parseMessage.
static bool parseInt32(ReadBuffer* buffer, bool* already_parsed, int32_t* output) {
    // If we need to parse it, and we have enough data: parse it!
    if (!*already_parsed && buffer->available() >= sizeof(*output)) {
        int bytes = buffer->read(reinterpret_cast<char*>(output), sizeof(*output));
        ASSERT(bytes == sizeof(*output));
        *already_parsed = true;
    }

    return *already_parsed;
}

bool MessageConnectionParser::parseMessage(ReadBuffer* buffer, int32_t* type, string* output) {
    if (!parseInt32(buffer, &has_type_, &type_)) return false;
    if (!parseInt32(buffer, &has_length_, &expected_length_)) return false;

    assert(has_type_ && has_length_);
    assert(expected_length_ > 0);
    if (buffer->available() < expected_length_) return false;

    output->resize(expected_length_);
    int bytes = buffer->read(base::stringArray(output), expected_length_);
    ASSERT(bytes == expected_length_);
    *type = type_;

    reset();
    return true;
}

TCPMessageConnection::TCPMessageConnection(event_base* event_loop, TCPConnection* connection) :
        connection_(connection) {
    connection_->setTarget(event_loop, this);
}

TCPMessageConnection::~TCPMessageConnection() {
    delete connection_;
}

void TCPMessageConnection::readAvailable(TCPConnection* connection) {
    assert(connection == connection_);
    // Read the data
    bool moreData = buffer_.readAll(connection_);

    // Parse messages
    int32_t type;
    string message;
    while (parser_.parseMessage(&buffer_, &type, &message)) {
        target_->messageReceived(this, type, message);
    }

    if (!moreData) {
        // The connection is closed: clean up
        target_->connectionClosed(this);
    }
}

void TCPMessageConnection::writeAvailable(TCPConnection* connection) {
    assert(connection == connection_);
    flush();
}

bool TCPMessageConnection::flush() {
    const void* read;
    int length;
    out_buffer_.readBuffer(&read, &length);

    while (length > 0) {
        int written = connection_->write((const char*) read, length);
        if (written == -1) {
            return false;
        } else if (written < length) {
            assert(written >= 0);
            // blocked: undo the read and come back later
            out_buffer_.undoRead(length - written);
            return true;
        }
        // We wrote the entire block: write more, if more is available
        assert(written == length);
        out_buffer_.readBuffer(&read, &length);
    }

    return true;
}
