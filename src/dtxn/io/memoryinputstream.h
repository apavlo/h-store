// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef IO_MEMORYINPUTSTREAM_H__
#define IO_MEMORYINPUTSTREAM_H__

#include <algorithm>
#include <cassert>
#include <cstring>

#include "io/io.h"

namespace io {

// Wraps an array, making it an io::InputStream. Does not own the array.
class MemoryInputStream : public io::InputStream {
public:
    MemoryInputStream() : buffer_(NULL), available_(0) {}
    MemoryInputStream(const void* buffer, int length) {
        setBuffer(buffer, length);
    }

    virtual int read(char* output, size_t length) {
        if (available_ == -1) {
            return -1;
        }

        int bytes = std::min(static_cast<int>(length), available_);
        memcpy(output, buffer_, bytes);
        buffer_ += bytes;
        available_ -= bytes;
        return bytes;
    }

    void setBuffer(const void* buffer, int length) {
        buffer_ = reinterpret_cast<const char*>(buffer);
        available_ = length;
        assert(buffer != NULL || available_ == 0);
        assert(available_ >= 0);
    }

    void close() {
        buffer_ = NULL;
        available_ = -1;
    }

    int available() { return available_; }

private:
    const char* buffer_;
    int available_;
};

}
#endif
