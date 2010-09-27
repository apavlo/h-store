// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstring>

#include <fcntl.h>

#include "base/assert.h"
#include "logging/logfile.h"
#include "stupidunit/stupidunit.h"

using namespace logging;

TEST(MinimalBuffer, Simple) {
    // Write data into the buffer
    MinimalBuffer buffer;
    void* write;
    int length;
    buffer.getWriteBuffer(&write, &length);
    ASSERT_GT(length, 100);
    EXPECT_EQ(0, ((intptr_t) write) % MinimalBuffer::BUFFER_ALIGN);
    EXPECT_EQ(MinimalBuffer::BUFFER_SIZE, length);
    static const char DATA[] = "hello";
    strcpy((char*) write, DATA);
    buffer.advanceWrite(sizeof(DATA) - 1);
    EXPECT_EQ(sizeof(DATA) - 1, buffer.available());

    // Write more data into the buffer
    buffer.getWriteBuffer(&write, &length);
    strcpy((char*) write, DATA);
    buffer.advanceWrite(sizeof(DATA) - 1);
    EXPECT_EQ((sizeof(DATA) - 1) * 2, buffer.available());

    // Get the data out of the buffer
    const void* read;
    buffer.getReadBuffer(&read, &length);
    static const char TWO_DATA[] = "hellohello";
    ASSERT_EQ(sizeof(TWO_DATA) - 1, length);
    EXPECT_EQ(0, memcmp(TWO_DATA, read, length));
    buffer.consumedRead();

    buffer.getReadBuffer(&read, &length);
    EXPECT_EQ(0, length);
    buffer.getWriteBuffer(&write, &length);
}

TEST(MinimalBuffer, Overflow) {
    MinimalBuffer buffer;

    // Fill the buffer
    static const char FOUR_BYTE[] = "012";
    for (int i = 0; i < MinimalBuffer::BUFFER_SIZE / sizeof(FOUR_BYTE); ++i) {
        void* write;
        int length;
        buffer.getWriteBuffer(&write, &length);
        memcpy(write, FOUR_BYTE, sizeof(FOUR_BYTE));
        buffer.advanceWrite(sizeof(FOUR_BYTE));
    }

    // The buffer is full: filling it = crash
    void* write;
    int length;
    buffer.getWriteBuffer(&write, &length);
    EXPECT_EQ(0, length);
    EXPECT_DEATH(buffer.advanceWrite(1));
}

class LogTest : public Test {
public:
    LogTest() : writer_("foo", 0, false, false) {}

protected:
    stupidunit::ChTempDir temp_dir_;
    MinimalBuffer buffer_;
    LogWriter writer_;
};

TEST_F(LogTest, Empty) {
    EXPECT_DEATH(writer_.syncWriteBuffer(&buffer_));

    writer_.close();

    // No data: the sequence reader crashes
    EXPECT_DEATH(SequenceReader("foo"));
}

TEST_F(LogTest, Overflow) {
    void* data;
    int length;
    int total_length = 0;

    // Fill the log
    while (total_length < LogWriter::LOG_SIZE) {
        buffer_.getWriteBuffer(&data, &length);
        memset(data, 0, length);  // avoid valgrind warnings: zero fill

        int bytes_to_write = length;
        if (total_length + bytes_to_write > LogWriter::LOG_SIZE) {
            bytes_to_write = LogWriter::LOG_SIZE - total_length;
        }
        buffer_.advanceWrite(bytes_to_write);
        total_length += bytes_to_write;
        writer_.syncWriteBuffer(&buffer_);
    }

    // writing 1 more byte crashes
    buffer_.getWriteBuffer(&data, &length);
    buffer_.advanceWrite(1);
    EXPECT_DEATH(writer_.syncWriteBuffer(&buffer_));
}

TEST_F(LogTest, Simple) {
    SyncLogWriter log("foo", 0, false, false);
    log.bufferedWrite(NULL, 0);
    log.bufferedWrite("foo", 3);
    log.bufferedWrite("bar", 3);
    log.bufferedWrite(NULL, 0);
    log.synchronize();
    log.close();

    SequenceReader reader("foo");
    EXPECT_EQ("", reader.stringValue());
    reader.advance();
    EXPECT_EQ("foo", reader.stringValue());
    reader.advance();
    EXPECT_EQ("bar", reader.stringValue());
    reader.advance();
    EXPECT_EQ("", reader.stringValue());
    EXPECT_DEATH(reader.advance());
}

TEST_F(LogTest, Alignment) {
    SyncLogWriter log("foo", 4096, false, false);
    log.bufferedWrite(NULL, 0);
    log.synchronize();
    log.bufferedWrite(NULL, 0);
    log.synchronize();
    log.close();

    // Read the first 8 bytes: non zero
    int fh = open("foo", O_RDONLY);
    assert(fh >= 0);

    char BUFFER[8];
    ssize_t bytes = read(fh, BUFFER, sizeof(BUFFER));
    EXPECT_EQ(sizeof(BUFFER), bytes);
    int32_t intvalue;
    memcpy(&intvalue, BUFFER, sizeof(intvalue));
    EXPECT_EQ(0, intvalue);
    memcpy(&intvalue, BUFFER+sizeof(intvalue), sizeof(intvalue));
    EXPECT_TRUE(intvalue != 0);

    // Read the second page: same result
    off_t offset = lseek(fh, 4096, SEEK_SET);
    ASSERT(offset == 4096);
    bytes = read(fh, BUFFER, sizeof(BUFFER));
    EXPECT_EQ(sizeof(BUFFER), bytes);
    memcpy(&intvalue, BUFFER, sizeof(intvalue));
    EXPECT_EQ(0, intvalue);
    memcpy(&intvalue, BUFFER+sizeof(intvalue), sizeof(intvalue));
    EXPECT_TRUE(intvalue != 0);
}

TEST_F(LogTest, Direct) {
    EXPECT_DEATH(SyncLogWriter("foo", 0, true, false));

    SyncLogWriter log("foo", LogWriter::DIRECT_ALIGNMENT, true, false);
    log.bufferedWrite(NULL, 0);
    log.synchronize();
    log.bufferedWrite(NULL, 0);
    log.synchronize();
    log.close();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
