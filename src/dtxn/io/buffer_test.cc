// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <algorithm>
#include <cstring>
#include <stdint.h>

#include "base/cast.h"
#include "io/buffer.h"
#include "io/mockstreams.h"
#include "stupidunit/stupidunit.h"

using std::string;

class MemoryOutputStreamTest : public Test {
protected:
    io::MemoryOutputStream out_;
    io::MockOutputStream mockout_;
};

TEST_F(MemoryOutputStreamTest, Empty) {
    EXPECT_EQ(0, out_.available());
    mockout_.next_result_ = 10000;
    EXPECT_EQ(0, out_.flush(&mockout_));
}

TEST_F(MemoryOutputStreamTest, Simple) {
    char temp[1000];
    memset(temp, 0, sizeof(temp));
    EXPECT_EQ(sizeof(temp), out_.write(temp, sizeof(temp)));
    EXPECT_EQ(sizeof(temp), out_.available());

    // Flush when there are no bytes available
    mockout_.next_result_ = 0;
    EXPECT_EQ(0, out_.flush(&mockout_));
    EXPECT_EQ(1, mockout_.write_count_);
    EXPECT_EQ(sizeof(temp), mockout_.last_length_);

    // Flush with some bytes available
    mockout_.next_result_ = 123;
    EXPECT_EQ(123, out_.flush(&mockout_));
    EXPECT_EQ(2, mockout_.write_count_);
    EXPECT_EQ(sizeof(temp), mockout_.last_length_);
    const char* last = mockout_.last_buffer_;

    // Flush with lots of bytes available
    mockout_.next_result_ = sizeof(temp);
    EXPECT_EQ(sizeof(temp) - 123, out_.flush(&mockout_));
    EXPECT_EQ(3, mockout_.write_count_);
    EXPECT_EQ(last + 123, mockout_.last_buffer_);
}

TEST_F(MemoryOutputStreamTest, FullPageWrites) {
    // Write the amount that exactly fills one page
    char temp[4096 - sizeof(int)];
    memset(temp, 0, sizeof(temp));
    EXPECT_EQ(sizeof(temp), out_.write(temp, sizeof(temp)));
    EXPECT_EQ(2, out_.write(temp, 2));
    EXPECT_EQ(sizeof(temp) + 2, out_.available());

    // Flush
    mockout_.next_result_ = 10000;
    EXPECT_EQ(sizeof(temp) + 2, out_.flush(&mockout_));
    EXPECT_EQ(2, mockout_.write_count_);
    EXPECT_EQ(2, mockout_.last_length_);
}

TEST_F(MemoryOutputStreamTest, CrossPageWrite) {
    char temp[5000];
    memset(temp, 0, sizeof(temp));
    EXPECT_EQ(sizeof(temp), out_.write(temp, sizeof(temp)));
    EXPECT_EQ(sizeof(temp), out_.available());

    mockout_.next_result_ = 10000;
    EXPECT_EQ(5000, out_.flush(&mockout_));
    EXPECT_EQ(5000 - (4096 - sizeof(int)), mockout_.last_length_);
    EXPECT_EQ(2, mockout_.write_count_);
}

class FIFOBufferTest : public Test {
public:
    void doWrite() {
        buffer_.writeBuffer(&write_, &remaining_);

        // scribble on the whole thing to ensure it is valid
        memset(write_, 0xff, remaining_);

        assert(remaining_ >= WRITE_LENGTH);
        memset(write_, 0x42, WRITE_LENGTH);
        remaining_ -= WRITE_LENGTH;
        buffer_.undoWrite(remaining_);
    }
    
    void verifyRead(const void* buffer, int length, uint8_t expected) {
        for (int i = 0; i < length; ++i) {
            assert(reinterpret_cast<const uint8_t*>(buffer)[i] == expected);
        }
    }

    static const int WRITE_LENGTH = 100;

    void* write_;
    const void* read_;
    int remaining_;
    io::FIFOBuffer buffer_;
};

TEST_F(FIFOBufferTest, Empty) {
    EXPECT_EQ(0, buffer_.available());

    const void* read;
    int length;
    buffer_.readBuffer(&read, &length);
    EXPECT_EQ(NULL, read);
    EXPECT_EQ(0, length);
}

TEST_F(FIFOBufferTest, ReadWrite) {
    // Write 100 bytes
    doWrite();
    EXPECT_EQ(WRITE_LENGTH, buffer_.available());

    // Read 10 bytes
    static const int READ_LENGTH = 10;
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(WRITE_LENGTH, remaining_);
    verifyRead(read_, remaining_, 0x42);
    remaining_ -= READ_LENGTH;
    buffer_.undoRead(remaining_);

    EXPECT_EQ(WRITE_LENGTH - READ_LENGTH, buffer_.available());
}

TEST_F(FIFOBufferTest, BadUndoWrite) {
    buffer_.writeBuffer(&write_, &remaining_);
    ASSERT_GT(remaining_, 0);
    EXPECT_EQ(remaining_, buffer_.available());

    EXPECT_DEATH(buffer_.undoWrite(-1));
    EXPECT_DEATH(buffer_.undoWrite(remaining_ + 1));
    buffer_.undoWrite(0);
    buffer_.undoWrite(remaining_);
    EXPECT_EQ(0, buffer_.available());
}

TEST_F(FIFOBufferTest, BadUndoRead) {
    doWrite();

    buffer_.readBuffer(&read_, &remaining_);
    ASSERT_EQ(WRITE_LENGTH, remaining_);

    EXPECT_DEATH(buffer_.undoRead(-1));
    EXPECT_DEATH(buffer_.undoRead(remaining_ + 1));
    buffer_.undoRead(0);
    buffer_.undoRead(remaining_);
    EXPECT_EQ(WRITE_LENGTH, buffer_.available());
}

TEST_F(FIFOBufferTest, MultiPageReadWrite) {
    doWrite();

    // write the rest of the page
    int write_remaining;
    buffer_.writeBuffer(&write_, &write_remaining);
    memset(write_, 0x43, write_remaining);

    // write the entire next page
    buffer_.writeBuffer(&write_, &write_remaining);
    memset(write_, 0x44, write_remaining);

    // write part of the next page
    buffer_.writeBuffer(&write_, &write_remaining);
    ASSERT_GT(write_remaining, WRITE_LENGTH);
    memset(write_, 0x45, WRITE_LENGTH);
    buffer_.undoWrite(write_remaining - WRITE_LENGTH);

    // write the rest of the page
    buffer_.writeBuffer(&write_, &write_remaining);
    memset(write_, 0x46, write_remaining);
    buffer_.undoWrite(1);

    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE * 3 - 1, buffer_.available());

    // read first page
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
    verifyRead(read_, WRITE_LENGTH, 0x42);
    verifyRead((uint8_t*)read_ + WRITE_LENGTH, io::FIFOBuffer::PAGE_DATA_SIZE - WRITE_LENGTH, 0x43);

    // end of second page
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
    verifyRead(read_, remaining_, 0x44);
    // end of buffer page
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE - 1, remaining_);
    verifyRead(read_, WRITE_LENGTH, 0x45);
    verifyRead((const uint8_t*) read_ + WRITE_LENGTH, remaining_ - WRITE_LENGTH, 0x46);

    // This provides the entire page. the previous read is invalid
    buffer_.writeBuffer(&write_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
}

TEST_F(FIFOBufferTest, CopyIn) {
    // CopyIn with an length = 0 works
    buffer_.copyIn(NULL, 0);
    EXPECT_EQ(0, buffer_.available());
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(0, remaining_);
    EXPECT_EQ(NULL, read_);

    // copyIn that spans two pages
    doWrite();
    char temp[io::FIFOBuffer::PAGE_DATA_SIZE];
    memset(temp, 0x01, sizeof(temp));
    buffer_.copyIn(temp, sizeof(temp));

    EXPECT_EQ(WRITE_LENGTH + sizeof(temp), buffer_.available());
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(WRITE_LENGTH + sizeof(temp) - io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(0, remaining_);
}

TEST_F(FIFOBufferTest, ReadAllAvailable) {
    io::MockInputStream input;
    EXPECT_EQ(0, buffer_.readAllAvailable(&input));
    EXPECT_EQ(1, input.read_count_);

    input.addFakeData(io::FIFOBuffer::PAGE_DATA_SIZE * 2);
    input.close();
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE * 2, buffer_.readAllAvailable(&input));
    EXPECT_EQ(4, input.read_count_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE * 2, buffer_.available());

    // Reading again returns -1: the connection is closed, no new data was read. This simulates
    // the case where the application read a bunch of data, but needs more so it waits for more
    // input using epoll(). That is ready immediately, so it calls readAllAvailable() again.
    EXPECT_EQ(-1, buffer_.readAllAvailable(&input));
    EXPECT_EQ(5, input.read_count_);

    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(0, remaining_);

    EXPECT_EQ(-1, buffer_.readAllAvailable(&input));
    EXPECT_EQ(6, input.read_count_);
}

TEST_F(FIFOBufferTest, WriteAvailable) {
    io::MockOutputStream output;
    EXPECT_EQ(0, buffer_.writeAvailable(&output));
    EXPECT_EQ(0, output.write_count_);

    // Fake put data in the buffer.
    buffer_.writeBuffer(&write_, &remaining_);

    // Write a limited amount of data twice
    output.next_result_ = 100;
    EXPECT_LT(0, buffer_.writeAvailable(&output));
    EXPECT_EQ(1, output.write_count_);
    EXPECT_EQ(remaining_, output.last_length_);
    output.next_result_ = 100;
    EXPECT_LT(0, buffer_.writeAvailable(&output));
    EXPECT_EQ(2, output.write_count_);
    EXPECT_EQ(remaining_ - 100, output.last_length_);

    // Write the remainder of the buffer
    output.next_result_ = -1;
    EXPECT_EQ(0, buffer_.writeAvailable(&output));
    EXPECT_EQ(3, output.write_count_);
    EXPECT_EQ(remaining_ - 200, output.last_length_);

    // Write multiple blocks
    buffer_.writeBuffer(&write_, &remaining_);
    int extra;
    buffer_.writeBuffer(&write_, &extra);
    buffer_.undoWrite(extra - 1);
    output.next_result_ = remaining_;
    EXPECT_LT(0, buffer_.writeAvailable(&output));
    EXPECT_EQ(5, output.write_count_);
    EXPECT_EQ(1, output.last_length_);
    output.last_length_ = 0;

    // Close the output and attempt to write
    output.closed_ = true;
    EXPECT_EQ(-1, buffer_.writeAvailable(&output));
    EXPECT_EQ(6, output.write_count_);
    EXPECT_EQ(1, output.last_length_);
}

TEST_F(FIFOBufferTest, WriteExact) {
    buffer_.writeBuffer(&write_, &remaining_);
    buffer_.undoWrite(1);

    // this will force allocation of a new buffer
    void* data = buffer_.writeExact(4);
    memset(data, 0x01, 4);

    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE - 1 + 4, buffer_.available());
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE - 1, remaining_);
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(4, remaining_);
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(0, remaining_);
}

TEST_F(FIFOBufferTest, ReadWriteReadWrite) {
    buffer_.writeBuffer(&write_, &remaining_);
    buffer_.undoWrite(remaining_ - 4);
    EXPECT_EQ(4, buffer_.available());

    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(4, remaining_);
    EXPECT_EQ(0, buffer_.available());

    // call to write invalidates the read
    buffer_.writeBuffer(&write_, &remaining_);
    EXPECT_EQ(io::FIFOBuffer::PAGE_DATA_SIZE, remaining_);
}

TEST_F(FIFOBufferTest, Clear) {
    char data[io::FIFOBuffer::PAGE_DATA_SIZE + 100];
    memset(data, 0x01, sizeof(data));
    buffer_.copyIn(data, sizeof(data));
    EXPECT_EQ(sizeof(data), buffer_.available());
    buffer_.clear();

    EXPECT_EQ(0, buffer_.available());
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(NULL, read_);
    EXPECT_EQ(0, remaining_);

    doWrite();
    buffer_.readBuffer(&read_, &remaining_);
    EXPECT_EQ(WRITE_LENGTH, remaining_);
    verifyRead(read_, remaining_, 0x42);
}

TEST_F(FIFOBufferTest, CopyOut) {
    char data[io::FIFOBuffer::PAGE_DATA_SIZE + 100];
    memset(data, 0x01, sizeof(data));
    buffer_.copyIn(data, sizeof(data));
    EXPECT_EQ(sizeof(data), buffer_.available());

    char out[sizeof(data)];
    EXPECT_DEATH(buffer_.copyOut(out, -1));
    EXPECT_DEATH(buffer_.copyOut(out, 0));
    EXPECT_DEATH(buffer_.copyOut(out, (int) sizeof(data) + 1));

    buffer_.copyOut(out, (int) sizeof(data) - 1);
    EXPECT_EQ(1, buffer_.available());
    EXPECT_EQ(0, memcmp(out, data, sizeof(data) - 1));

    string strout = "hello world";
    strout.resize(buffer_.available());
    string copy = strout;
    buffer_.copyOut(&strout, 1);
    EXPECT_EQ(0, buffer_.available());
    ASSERT_EQ(1, strout.size());
    EXPECT_EQ(0x1, strout[0]);
    // verify copy does not change with std::string reference counting
    EXPECT_NE(copy, strout);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
