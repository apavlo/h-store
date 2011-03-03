// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef IO_MOCKSTREAMS_H__
#define IO_MOCKSTREAMS_H__

#include <algorithm>
#include <cassert>

#include "io/io.h"

namespace io {

class MockOutputStream : public io::OutputStream {
public:
    MockOutputStream() :
            write_count_(0), last_buffer_(NULL), last_length_(0), next_result_(-1),
            closed_(false) {}

    virtual int write(const char* buffer, size_t length) {
        write_count_ += 1;
        last_buffer_ = buffer;
        last_length_ = length;

        if (closed_) {
            return -1;
        }

        if (next_result_ != -1) {
            int bytes = std::min(next_result_, static_cast<int>(length));
            next_result_ -= bytes;
            assert(next_result_ >= 0);
            return bytes;
        } else {
            return static_cast<int>(length);
        }
    }

    int write_count_;
    const char* last_buffer_;
    size_t last_length_;

    int next_result_;
    bool closed_;
};

class MockInputStream : public io::InputStream {
public:
    MockInputStream() : read_count_(0), available_(0), closed_(false) {}

    virtual int read(char* buffer, size_t length) {
        read_count_ += 1;

        int bytes_read = std::min(available_, static_cast<int>(length));
        available_ -= bytes_read;
        assert(available_ >= 0);

        // If there are no more bytes and the stream is closed: we are done here
        if (bytes_read == 0 && closed_) return -1;

        return bytes_read;
    }

    void addFakeData(int amount) {
        available_ += amount;
    }

    void close() {
        assert(!closed_);
        closed_ = true;
    }

    int read_count_;

private:
    int available_;
    bool closed_;
};

}
#endif
