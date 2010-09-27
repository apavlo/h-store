// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef LOGGING_LOGFILE_H__
#define LOGGING_LOGFILE_H__

#include "logging/sequencefile.h"

namespace logging {

// Wraps a single allocation, like FIFOBuffer but simpler.
// This is needed because FIFOBuffer is not aligned.
class MinimalBuffer {
public:
    // TODO: Make this smaller (16k?) But this is useful for crash testing large writes
    static const int BUFFER_SIZE = 512 << 10;
    static const int BUFFER_ALIGN = 4 << 10;

    MinimalBuffer();
    ~MinimalBuffer();

    void getWriteBuffer(void** buffer, int* length) {
        *buffer = buffer_ + written_;
        assert(*buffer <= buffer_ + BUFFER_SIZE);
        *length = BUFFER_SIZE - written_;
        assert((char*) *buffer + *length == buffer_ + BUFFER_SIZE);
    }

    void advanceWrite(int length) {
        assert(0 <= length && length <= BUFFER_SIZE - written_);
        written_ += length;
        assert(0 <= written_ && written_ <= BUFFER_SIZE);
    }

    void getReadBuffer(const void** buffer, int* length) {
        *buffer = buffer_;
        *length = written_;
    }

    // TODO: This should be advanceRead, but we don't support that right now
    void consumedRead() {
        written_ = 0;
    }

    int available() const { return written_; }

private:
    // Pointer to the raw buffer
    char* buffer_;
    int written_;
};


// Writes data to a file, supporting data integrity via synchronize().
// NOTE: The destructor does *not* invoke a file integrity operation.
class LogWriter {
public:
    LogWriter(const char* path, int alignment, bool direct, bool o_dsync);

    void close() { writer_.close(); }

    // Writes data in buffer to the end of the unwritten segment.
    void syncWriteBuffer(MinimalBuffer* buffer);

    // To efficiently support synchronize(), the file is preallocated to be this size.
    static const int LOG_SIZE = 64 << 20;

    // This may need to be 4 k on disks with 4 k segments
    static const int DIRECT_ALIGNMENT = 512;

private:
    FileWriter writer_;
    int total_bytes_;
    int alignment_;
    bool o_dsync_;
};

// Provides a simple synchronous logging interface.
class SyncLogWriter {
public:
    SyncLogWriter(const char* path, int alignment, bool direct, bool o_dsync) :
            writer_(path, alignment, direct, o_dsync) {}
    ~SyncLogWriter() {}

    // Writes the length bytes starting at data as a log entry. The data may be buffered in memory.
    void bufferedWrite(const void* data, size_t length);

    // Synchronizes all written data to the physical disk. After this returns, the written data is
    // guaranteed to survive a reboot or power failure. Typically this calls fdatasync(), fsync()
    // or similar.
    void synchronize() { writer_.syncWriteBuffer(&buffer_); }

    void close() { writer_.close(); }

private:
    MinimalBuffer buffer_;
    LogWriter writer_;
};

}  // namespace logging
#endif
