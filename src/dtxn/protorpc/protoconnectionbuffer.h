// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTORPC_PROTOCONNECTIONBUFFER_H__
#define PROTORPC_PROTOCONNECTIONBUFFER_H__

#include "io/buffer.h"

namespace google { namespace protobuf {
class MessageLite;
}}

namespace protorpc {

class ProtoConnection;

// Buffers and parses the ProtoRPC protocol in an asynchronous fashion.
class ProtoConnectionBuffer {
public:
    ProtoConnectionBuffer() : has_length_(false), length_(0) {}
    ~ProtoConnectionBuffer() {}

    // See io::FIFOBuffer::readAllAvailable().
    int readAllAvailable(io::InputStream* input) {
        return input_buffer_.readAllAvailable(input);
    }

    // Returns true if message was parsed from the internal buffer. False means not enough
    // data is in the buffer.
    bool readBufferedMessage(google::protobuf::MessageLite* message);

    // Serializes message to the internal buffer.
    void bufferMessage(const google::protobuf::MessageLite& message);

    // Attempts to write all internally buffered responses. Returns true if
    // the write blocks and more data needs to be sent.
    bool tryWrite(io::OutputStream* output);

private:
    bool has_length_;
    int length_;

    io::FIFOBuffer input_buffer_;
    io::FIFOBuffer output_buffer_;
};

}
#endif
