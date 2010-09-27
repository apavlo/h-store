// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "logging/sequencefile.h"

#include <cstring>
#include <limits>

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "base/assert.h"
#include "logging/crc32c.h"

namespace logging {

FileWriter::FileWriter(const char* path, bool o_dsync) {
    int options = O_WRONLY | O_CREAT | O_TRUNC;
    if (o_dsync) {
#ifdef __APPLE__
        fprintf(stderr, "O_DSYNC unsupported on Mac OS X\n");
        abort();
#else
        options |= O_DSYNC;
#endif
    }
    file_descriptor_ = open(path, options, 0600);
    CHECK(file_descriptor_ >= 0);
}

void FileWriter::setDirect(int file_descriptor, bool enable) {
    int command;
    int arg;

#ifdef __linux__
    command = F_SETFL;
    // optional bonus option: don't update atime
    arg = O_NOATIME;
    if (enable) {
        arg |= O_DIRECT;
    }
#elif defined(__APPLE__)
    command = F_NOCACHE;
    arg = enable;
#else 
#error Unsupported OS for O_DIRECT
#endif
    int error = fcntl(file_descriptor, command, arg);
    ASSERT(error == 0);
}

void FileWriter::writeBuffer(io::FIFOBuffer* buffer) {
    while (buffer->available() > 0) {
        const void* data;
        int length;
        buffer->readBuffer(&data, &length);
        ssize_t bytes = ::write(file_descriptor_, data, length);
        if (bytes < 0) {
            printf("write(%p, %d): %s\n", data, length, strerror(errno));
        }
        ASSERT(bytes == length);
    }
    assert(buffer->available() == 0);
}

void FileWriter::close() {
    if (file_descriptor_ >= 0) {
        int error = ::close(file_descriptor_);
        ASSERT(error == 0);
        file_descriptor_ = -1;
    }
}


// The CRC32C of a zero length byte string is 0x00000000. This means that a sequence of 8 zero
// bytes is a correct zero length sequence. However, for log files, we need zeroed data to be
// incorrecct. Thus, we initialize CRC32C with a value of ~0x01020304.
// TODO: Does this weaken any of the CRC properties? I can't imagine it does ...
static uint32_t sequenceCrc(const void* data, size_t length) {
    return crc32cFinish(crc32c(~SequenceBuffer::ZERO_LENGTH_CRC, data, length));
}

int SequenceBuffer::arrayWrite(void* output, int output_length,
        const void* input, int input_length) {
    char* output_buffer = (char*) output;
    CHECK(output_length >= input_length + sizeof(int32_t) * 2);

    assert(input_length <= std::numeric_limits<int32_t>::max());
    int32_t length_i32 = (int32_t) input_length;
    memcpy(output_buffer, &length_i32, sizeof(length_i32));
    output_buffer += sizeof(length_i32);

    memcpy(output_buffer, input, input_length);
    output_buffer += input_length;

    // Compute CRC32C over the data
    uint32_t crc = sequenceCrc(input, input_length);
    memcpy(output_buffer, &crc, sizeof(crc));
    output_buffer += sizeof(crc);

    int bytes_out = (int)(output_buffer - (char*) output);
    assert(bytes_out <= output_length);
    return bytes_out;
}

void SequenceBuffer::bufferedWrite(const void* data, size_t length) {
    void* output;
    int output_length;
    buffer_.writeBuffer(&output, &output_length);
    int bytes_out = arrayWrite(output, output_length, data, (int) length);
    buffer_.undoWrite(output_length - bytes_out);
}

void SequenceWriter::write(const std::string& value) {
    buffer_.bufferedWrite(value.data(), value.size());
    writer_.writeBuffer(buffer_.buffer());
}

static const int INITIAL_BUFFER_SIZE = 4096;

SequenceReader::SequenceReader(const char* path) {
    file_descriptor_ = open(path, O_RDONLY);
    if (file_descriptor_ == -1) perror("WTF");

    CHECK(file_descriptor_ >= 0);
    buffer_ = new char[INITIAL_BUFFER_SIZE];
    max_buffer_length_ = INITIAL_BUFFER_SIZE;
    valid_buffer_length_ = INITIAL_BUFFER_SIZE;

    // Initially set the pointer to be just before a "fake" CRC: this will register as "empty"
    data_ = buffer_ + max_buffer_length_ - sizeof(uint32_t);
    length_ = 0;
    advance();
}

void SequenceReader::close() {
    if (file_descriptor_ >= 0) {
        int error = ::close(file_descriptor_);
        ASSERT(error == 0);
        file_descriptor_ = -1;
    }

    delete[] buffer_;
    buffer_ = NULL;
    data_ = NULL;
    length_ = 0;
    valid_buffer_length_ = 0;
    max_buffer_length_ = 0;
}

void SequenceReader::advance() {
    uint32_t crc;
    // Advance data past the end
    data_ += length_ + sizeof(crc);

    // Interpret the next 4 bytes as a little-endian int32_t
    int32_t length;
    alignBufferToRead(sizeof(length));
    // Valid EOF
    if (data_ + sizeof(length) > buffer_ + valid_buffer_length_) {
        CHECK(valid_buffer_length_ == 0);
        close();
        return;
    }

    memcpy(&length, data_, sizeof(length));
    data_ += sizeof(length);
    assert(data_ <= buffer_ + valid_buffer_length_);
    assert(length >= 0);

    alignBufferToRead(length + (int) sizeof(crc));
    CHECK(data_ + length + sizeof(crc) <= buffer_ + valid_buffer_length_);
    crc = sequenceCrc(data_, length);
    uint32_t expected_crc;
    memcpy(&expected_crc, data_ + length, sizeof(expected_crc));
    CHECK(crc == expected_crc);

    length_ = length;
}

/** Ensures that the desired_length bytes can be safely read from data. */
void SequenceReader::alignBufferToRead(int desired_length) {
    assert(desired_length >= 0);
    assert(data_ <= buffer_ + valid_buffer_length_);
    ssize_t bytes_remaining = (ssize_t) (buffer_ + valid_buffer_length_) - (ssize_t) data_;
    if (bytes_remaining >= desired_length) {
        // Enough space remaining! We are done
        return;
    }
    // Move the trailing data; read more data
    // TODO: This is slow in the case of reading a split length, but ensures zero-copy for the log
    memmove(buffer_, data_, bytes_remaining);
    data_ = buffer_;
    char* start = buffer_ + bytes_remaining;
    size_t remaining = max_buffer_length_ - bytes_remaining;
    ssize_t bytes = read(file_descriptor_, start, remaining);
    CHECK(0 <= bytes && bytes <= remaining);
    valid_buffer_length_ = bytes + bytes_remaining;

    //~ assert(valid_buffer_length_ >= desired_length);
}
    

}  // namespace logging
