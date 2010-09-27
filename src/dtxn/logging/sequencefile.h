// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef LOGGING_SEQUENCEFILE_H__
#define LOGGING_SEQUENCEFILE_H__

#include <cassert>
#include <string>

#include <stdint.h>

#include <sys/types.h>
       #include <sys/stat.h>
       #include <fcntl.h>


#include "io/buffer.h"

namespace logging {

// Wrapper around UNIX file I/O.
// TODO: Move this into io?
class FileWriter{
public:
    FileWriter(const char* path, bool o_dsync = false);
    ~FileWriter() { close(); }

    void close();

    // Toggle O_DIRECT (or equivalent) on or off.
    // WARNING: This implies alignment constraints on Linux. Mac OS X has performance issues
    // with incorrect alignment.
    void setDirect(bool enable) {
        setDirect(file_descriptor_, enable);
    }

    static void setDirect(int file_descriptor, bool enable);

    ssize_t write(const void* data, size_t length) {
        return ::write(file_descriptor_, data, length);
    }

    void writeBuffer(io::FIFOBuffer* buffer);

    // TODO: wrap more functions (eg. fsync fdatasync lseek)?
    int file_descriptor() { return file_descriptor_; }

private:
    int file_descriptor_;
};


// Buffers up a number of byte sequences, to be written later.
class SequenceBuffer {
public:
    // Writes a sequence into the buffer indicated by (output, output_length), using the input
    // (input, input_length). Returns the number of bytes written to output.
    static int arrayWrite(void* output, int output_length,
        const void* input, int input_length);

    // Writes the length bytes starting at data as a sequence. This data is buffered in memory.
    void bufferedWrite(const void* data, size_t length);

    io::FIFOBuffer* buffer() { return &buffer_; }

    static const uint32_t ZERO_LENGTH_CRC = 0x01020304;
private:
    io::FIFOBuffer buffer_;
};


class SequenceWriter {
public:
    SequenceWriter(const char* path) : writer_(path) {}

    void close() { writer_.close(); }

    void write(const std::string& value);

private:
    FileWriter writer_;
    SequenceBuffer buffer_;
};


class SequenceReader {
public:
    SequenceReader(const char* path);
    ~SequenceReader() { close(); }

    void close();

    const char* data() const {
        assert(hasValue());
        return data_;
    }

    size_t length() const {
        assert(hasValue());
        return length_;
    }

    /** Returns a std::string containing the current value. Note: This copies the data, so it
    should not be used for performance critical code. */
    std::string stringValue() const {
        assert(hasValue());
        return std::string(data_, length_);
    }

    bool hasValue() const {
        return data_ != NULL;
    }

    void advance();

private:
    /** Ensures that the desired_length bytes can be safely read from data. */
    void alignBufferToRead(int desired_length);

    int file_descriptor_;

    char* buffer_;
    size_t valid_buffer_length_;
    size_t max_buffer_length_;

    const char* data_;
    size_t length_;
};

}  // namespace logging
#endif
