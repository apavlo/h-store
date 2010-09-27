// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef MOCKMESSAGECONNECTION_H__
#define MOCKMESSAGECONNECTION_H__

#include "messageconnection.h"

// Parses a message from data, then clears data. Used for unit tests.
template <typename MessageType>
void messageFromString(std::string* data, MessageType* result) {
    assert(data->size() >= sizeof(int32_t)*2);
    const char* start = data->data();
    int32_t type = 0;
    memcpy(&type, start, sizeof(type));
    start += sizeof(type);
    assert(type == MessageType::typeCode());

    int32_t size = 0;
    memcpy(&size, start, sizeof(size));
    start += sizeof(size);
    assert(size == data->size() - sizeof(int32_t)*2);

    result->parseFromString(start, start + size);
    data->clear();
}

// Fake MessageConnection for use in unit tests.
class MockMessageConnection : public MessageConnection {
public:
    MessageConnectionCallback* target() { return target_; }

    bool hasMessage() const { return !message_.empty(); }

    template <typename MessageType>
    void getMessage(MessageType* result) {
        messageFromString(&message_, result);
    }

    std::string message_;

    template <typename MessageType>
    void triggerCallback(const MessageType& message) {
        std::string buffer;
        message.appendToString(&buffer);
        triggerCallback(message.typeCode(), buffer);
    }

    void triggerCallback(int32_t type_code, const std::string& message) {
        target_->messageReceived(this, type_code, message);
    }

    virtual bool flush() {
        message_.clear();

        const void* read;
        int length;
        out_buffer_.readBuffer(&read, &length);
        while (length > 0) {
            message_.append((const char*) read, length);
            out_buffer_.readBuffer(&read, &length);
        }

        return true;
    }
};

#endif
