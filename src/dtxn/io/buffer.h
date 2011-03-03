// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef IO_BUFFER_H__
#define IO_BUFFER_H__

#include <string>
#include <vector>

#include "base/stringutil.h"
#include "io/io.h"

namespace io {

struct Page;

// Implements OutputStream to memory blocks.
class MemoryOutputStream : public OutputStream {
public:
    MemoryOutputStream();
    virtual ~MemoryOutputStream();

    // Writes length bytes into the buffer. This always copies all the data.
    virtual int write(const char* buffer, size_t length);

    // Returns a pointer to length bytes in the buffer, and advances the write position by the
    // same amount. This is a very low-level routine that should only be used for writing length
    // prefixed messages. If it is not used carefully, garbage could be written into the buffer.
    char* reserve(size_t length); 

    // Write as much data as possible to out. Returns the number of bytes written,
    // or -1 if out.write() returns -1.
    int flush(OutputStream* out);

    // Number of bytes available to be flushed to another OutputStream.
    int available() const { return available_; }

private:
    std::vector<Page*> pages_;

    // The position that flush() will copy from, in the first page
    int read_position_;
    int available_;
};


// Provides zero-copy reads and writes to a set of internal buffers.
class FIFOBuffer {
public:
    FIFOBuffer();
    ~FIFOBuffer();

    // data will point to the beginning of the next chunk of data to be written in the buffer.
    // length will be the number of bytes available to be written.
    void writeBuffer(void** data, int* length);

    // Push length bytes back into the write end of the buffer. Used when writeBuffer() returns
    // more data than you want to write. length must be <= length returned by the last call to
    // writeBuffer().
    void undoWrite(int length);

    // Returns a pointer to a buffer available for writing exactly length bytes of data. This
    // must only be used to write "small" chunks, since it may leave some wasted bytes at the end
    // of a buffer. This simplifies serialization code at the cost of one int per page to record
    // the last bytes written.
    void* writeExact(int length);

    // Copies length bytes from data into this FIFO.
    // TODO: Make length size_t?
    void copyIn(const void* data, int length);
    void copyIn(const std::string& data) {
        copyIn(data.data(), (int) data.size());
    }

    // data will point to the beginning of the next chunk of data in the buffer. length will be the
    // number of bytes available.
    void readBuffer(const void** data, int* length);

    // Push length bytes back into the read end of the buffer. Used when readBuffer() returns more
    // data than you want to consume. length must be <= length returned by the last call to
    // readBuffer().
    void undoRead(int length);

    // Copy length bytes from the FIFO into out.
    void copyOut(void* out, int length);
    void copyOut(std::string* out, int length) {
        out->resize(length);
        copyOut(base::stringArray(out), length);
    }

    // Reads all available data from input. Returns the number of bytes
    // available, or -1 if no data was read and the input stream returns -1.
    // This allows the application an opportunity to consume the last bytes in the stream, without
    // getting stuck if the connection is closed with a partial message.
    //
    // NOTE: This used to be readUntilAvailable, to enable "incremental" processing. In other
    // words, read a chunk into the buffer, process it, read next, .... This can eliminate
    // excess allocation / deallocation of buffer chunks. However, it causes an excess read
    // in the "typical" case where the buffer is filled on the first read.
    // Using readAllAvailable yielded a ~3% performance improvement by reducing system calls.
    // TODO: Resurrect readUntilAvailable in a better form? Or maybe it doesn't matter.
    int readAllAvailable(io::InputStream* input);

    // Writes as much data as is available. Returns -1 if the output stream
    // returned -1. Returns 0 if all the data was written. Returns > 0 if
    // the OutputStream blocked and data remains in this buffer. This means
    // this needs to be called again, once the OutputStream is ready (eg. in
    // response to epoll).
    int writeAvailable(io::OutputStream* output);

    // Returns the number of bytes available for reading in the buffer.
    // TODO: Don't cache the number of available bytes?
    int available() const { return available_; }

    // Removes all data from the FIFO, resetting it to empty.
    void clear();

    // Allocate pages of this size
    static const int PAGE_SIZE = 8192;
    static const int PAGE_DATA_SIZE;

private:
    struct Page;
    Page* allocatePage() const;
    void popPage();

    Page* head_;
    Page* tail_;

    int read_position_;
    int available_;
};

}

#endif
