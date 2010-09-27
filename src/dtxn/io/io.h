// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef IO_ASYNC_H__
#define IO_ASYNC_H__

#include <cstddef>

namespace io {

// An interface for reading (possibly asynchronous) streams of bytes.
class InputStream {
public:
    virtual ~InputStream() {}

    // Copy length bytes of data from the stream into buffer. Returns the number of bytes read.
    // Return values:
    // -1: The stream is closed and there is no more data.
    // [0, length): All available bytes have been read.
    // length: There may be more data available for reading.
    virtual int read(char* buffer, size_t length) = 0;
};

// An interface for writing (possible asynchronous) streams of bytes.
class OutputStream {
public:
    virtual ~OutputStream() {}

    // Copy length bytes from buffer into the stream. Returns the number of bytes written.
    // Return values:
    // -1: The stream is closed and cannot be written to.
    // [0, length): The stream is non-blocking and would block. Call write again later.
    // length: All the bytes were copied.
    virtual int write(const char* buffer, size_t length) = 0;
};

}

#endif
