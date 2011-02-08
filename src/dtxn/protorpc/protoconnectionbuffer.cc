// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "protorpc/protoconnectionbuffer.h"

#include "base/assert.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/message_lite.h"

using std::string;

namespace protorpc {

class ZeroCopyFIFOReadAdapter : public google::protobuf::io::ZeroCopyInputStream {
public:
    ZeroCopyFIFOReadAdapter(io::FIFOBuffer* buffer) : buffer_(buffer) {}

    virtual ~ZeroCopyFIFOReadAdapter() {}
    
    virtual bool Next(const void** data, int* size) {
        buffer_->readBuffer(data, size);
        if (*size == 0) {
            assert(*data == NULL);
            assert(buffer_->available() == 0);
            return false;
        }
        return true;
    }

    virtual void BackUp(int count) {
        buffer_->undoRead(count);
    }

    virtual bool Skip(int count) {
        assert(false);
        return false;
    }

    virtual int64_t ByteCount() const {
        assert(false);
        return -1;
    }

private:
    io::FIFOBuffer* buffer_;
};

class ZeroCopyFIFOWriteAdapter : public google::protobuf::io::ZeroCopyOutputStream {
public:
    ZeroCopyFIFOWriteAdapter(io::FIFOBuffer* buffer) : buffer_(buffer) {}

    virtual ~ZeroCopyFIFOWriteAdapter() {}

    virtual bool Next(void** data, int* size) {
        buffer_->writeBuffer(data, size);
        assert(*data != NULL);
        assert(size > 0);
        return true;
    }

    virtual void BackUp(int count) {
        buffer_->undoWrite(count);
    }

    virtual int64_t ByteCount() const {
        assert(false);
        return -1;
    }

private:
    io::FIFOBuffer* buffer_;
};

bool ProtoConnectionBuffer::readBufferedMessage(google::protobuf::MessageLite* message) {
    if (!has_length_) {
        if (input_buffer_.available() < sizeof(length_)) return false;

        input_buffer_.copyOut(&length_, sizeof(length_));
        CHECK(length_ >= 0);
        has_length_ = true;
    }

    if (input_buffer_.available() < length_) return false;

    ZeroCopyFIFOReadAdapter adapter(&input_buffer_);
    bool success = message->ParseFromBoundedZeroCopyStream(&adapter, length_);
    CHECK(success);

    length_ = 0;
    has_length_ = false;
    return true;
}

// Serializes message to the internal buffer.
void ProtoConnectionBuffer::bufferMessage(const google::protobuf::MessageLite& message) {
    ZeroCopyFIFOWriteAdapter adapter(&output_buffer_);
    google::protobuf::io::CodedOutputStream out(&adapter);

    int32_t length = message.ByteSize();
    out.WriteLittleEndian32(length);
    message.SerializeWithCachedSizes(&out);
    assert(!out.HadError());
}

// Attempts to write all internally buffered responses. Returns true if
// the write blocks and more data needs to be sent.
bool ProtoConnectionBuffer::tryWrite(io::OutputStream* output) {
    int result = output_buffer_.writeAvailable(output);
    // TODO: Handle connection closed
    CHECK(result != -1);
    if (result == 0) {
        return false;
    } else {
        assert(result > 0);
        return true;
    }
}

}
