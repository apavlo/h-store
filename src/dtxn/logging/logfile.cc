// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "logging/logfile.h"

#include <cstring>

#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "base/assert.h"

#ifndef __APPLE__
#if __GLIBC_PREREQ (2,10)
// fallocate added in glibc 2.10
#define HAVE_FALLOCATE
#endif
#define HAVE_FDATASYNC
#endif

static inline int syncFileDescriptor(int fd) {
#ifdef HAVE_FDATASYNC
    return fdatasync(fd);
#else
    return fsync(fd);
#endif
}

namespace logging {
    
MinimalBuffer::MinimalBuffer() : written_(0) {
#ifndef __APPLE__
    int error = posix_memalign((void**) &buffer_, BUFFER_ALIGN, BUFFER_SIZE);
    ASSERT(error == 0);
#else
    // Mac OS X: Doesn't need to be aligned for F_NOCACHE?
    buffer_ = (char*) malloc(BUFFER_SIZE);
#endif
}

MinimalBuffer::~MinimalBuffer() {
    free(buffer_);
}


static const int LOG_BUFFER_SIZE = 4096;  // 4k

LogWriter::LogWriter(const char* path, int alignment, bool direct, bool o_dsync) :
            writer_(path, o_dsync), total_bytes_(0), alignment_(alignment), o_dsync_(o_dsync) {
    assert(0 <= alignment_ && alignment_ <= MinimalBuffer::BUFFER_SIZE);
    if (direct) {
        CHECK(alignment_ > 0 && (alignment_ % DIRECT_ALIGNMENT) == 0);
    }

#ifdef HAVE_FALLOCATE
    // Pre-allocate disk space: this should hopefully mean the space is contiguous on ext4.
    static const int mode = 0;
    static const int fallocate_offset = 0;
    int fallocate_error= fallocate(writer_.file_descriptor(), mode, fallocate_offset , LOG_SIZE);
    // The man page says this returns -1 on failure, setting errno appropriately.
    ASSERT(fallocate_error == 0 || (fallocate_error == -1 &&
            (errno == EOPNOTSUPP || errno == ENOSYS)));
#endif

    // zero fill the log file:
    // IMPORTANT: With fdatasync, the space *must* be preallocated for correctness. Theoretically,
    // calling fallocate above is sufficient. However, zero filling the file seems to improve
    // the performance of fdatasync(). Without it, it seems as though ext4 must do file metadata
    // updates, as the performance is the same as with fsync.
    char buffer[LOG_BUFFER_SIZE];
    memset(buffer, 0, sizeof(buffer));
    for (int i = 0; i < LOG_SIZE; i += (int) sizeof(buffer)) {
        int write_size = sizeof(buffer);
        if (i + write_size > LOG_SIZE) {
            write_size = LOG_SIZE - i;
        }
        ssize_t bytes = writer_.write(buffer, write_size);
        ASSERT(bytes == write_size);
    }

    // fsync() to ensure the file metadata is on disk before overwriting the zero filled file.
    int error = fsync(writer_.file_descriptor());
    ASSERT(error == 0);
    off_t offset = lseek(writer_.file_descriptor(), 0, SEEK_SET);
    ASSERT(offset == 0);

    if (direct) {
        writer_.setDirect(direct);
    }
}

void LogWriter::syncWriteBuffer(MinimalBuffer* buffer) {
    // Strictly speaking, this is not an error. However, if there is no data, calling this is
    // probably a bug.
    assert(buffer->available() > 0);

    // Align output if required
    if (alignment_ > 0) {
        int excess = buffer->available() % alignment_ ;
        if (excess > 0) {
            // Compute the amount of bytes needed to align the block
            assert(alignment_ > 0);
            int bytes_to_align = alignment_ - excess;
            assert(bytes_to_align > 0);

            // Obtain a block of buffer
            void* data;
            int length;
            buffer->getWriteBuffer(&data, &length);
            CHECK(length >= bytes_to_align);

            // Zero the remaining bytes and align the block
            memset(data, 0, bytes_to_align);
            buffer->advanceWrite(bytes_to_align);
            assert(buffer->available() % alignment_ == 0);
        }
    }

    // TODO: Support re-writing the log file in some way?
    total_bytes_ += buffer->available();
    assert(total_bytes_ <= LOG_SIZE);

    // TODO: Check we don't write past the end.
    const void* read;
    int length;
    buffer->getReadBuffer(&read, &length);
    ssize_t written = writer_.write(read, length);
    CHECK(written == length);
    buffer->consumedRead();
    if (!o_dsync_) {
        syncFileDescriptor(writer_.file_descriptor());
    }
}

void SyncLogWriter::bufferedWrite(const void* data, size_t length) {
    void* output;
    int output_length;
    buffer_.getWriteBuffer(&output, &output_length);
    int output_bytes = SequenceBuffer::arrayWrite(output, output_length, data, (int) length);
    buffer_.advanceWrite(output_bytes);
}

}  // namespace logging
