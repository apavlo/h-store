// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstring>

#include "logging/sequencefile.h"
#include "stupidunit/stupidunit.h"

using namespace logging;

TEST(SequenceBuffer, Simple) {
    SequenceBuffer buffer;
    static const char DATA[] = "hello";
    buffer.bufferedWrite(DATA, sizeof(DATA));

    int32_t length;
    buffer.buffer()->copyOut(&length, sizeof(length));
    EXPECT_EQ(sizeof(DATA), length);
    char TEMP[sizeof(DATA)];
    buffer.buffer()->copyOut(TEMP, sizeof(TEMP));
    EXPECT_EQ(0, memcmp(TEMP, DATA, sizeof(TEMP)));
    EXPECT_EQ(4, buffer.buffer()->available());
}

TEST(SequenceBuffer, Empty) {
    // Write a correct empty record
    SequenceBuffer buffer;
    buffer.bufferedWrite(NULL, 0);

    int32_t length;
    buffer.buffer()->copyOut(&length, sizeof(length));
    EXPECT_EQ(0, length);

    uint32_t crc;
    buffer.buffer()->copyOut(&crc, sizeof(crc));
    EXPECT_EQ(SequenceBuffer::ZERO_LENGTH_CRC, crc);
}

class SequenceTest : public Test {
protected:
    stupidunit::ChTempDir temp_dir_;
};

TEST_F(SequenceTest, Simple) {
    SequenceWriter writer("foo");

    writer.write("foo");
    writer.write("bar");
    writer.write("");
    writer.write("baz");
    writer.close();

    SequenceReader reader("foo");
    EXPECT_TRUE(reader.hasValue());
    EXPECT_EQ("foo", reader.stringValue());
    reader.advance();
    EXPECT_EQ("bar", reader.stringValue());
    reader.advance();
    EXPECT_EQ("", reader.stringValue());
    EXPECT_EQ(0, reader.length());
    reader.advance();
    EXPECT_EQ("baz", reader.stringValue());
    EXPECT_TRUE(reader.hasValue());
    reader.advance();
    EXPECT_FALSE(reader.hasValue());
    EXPECT_DEATH(reader.length());
    EXPECT_DEATH(reader.data());
    EXPECT_DEATH(reader.stringValue());
    EXPECT_DEATH(reader.advance());
    reader.close();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
