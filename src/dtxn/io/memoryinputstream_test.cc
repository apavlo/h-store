// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "io/memoryinputstream.h"

#include "stupidunit/stupidunit.h"

using io::MemoryInputStream;

class MemoryInputStreamTest : public Test {
public:
    MemoryInputStreamTest();

protected:
    static const char data_[];
    char buffer_[256];

    MemoryInputStream stream_;
};
const char MemoryInputStreamTest::data_[] = "hello world";
MemoryInputStreamTest::MemoryInputStreamTest() : stream_(data_, sizeof(data_)) {}

TEST_F(MemoryInputStreamTest, SetBuffer) {
    EXPECT_DEATH(stream_.setBuffer(NULL, 42));
    EXPECT_DEATH(stream_.setBuffer(buffer_, -1));
    stream_.setBuffer(buffer_, 0);
    stream_.setBuffer(NULL, 0);
}

TEST_F(MemoryInputStreamTest, ReadBadParameters) {
    EXPECT_DEATH(stream_.read(NULL, 42));
    EXPECT_EQ(0, stream_.read(buffer_, 0));
}

TEST_F(MemoryInputStreamTest, Simple) {
    int bytes = stream_.read(buffer_, 1);
    EXPECT_EQ(1, bytes);
    EXPECT_EQ(data_[0], buffer_[0]);

    bytes = stream_.read(buffer_, sizeof(buffer_));
    EXPECT_EQ(sizeof(data_) - 1, bytes);
    EXPECT_EQ(0, memcmp(data_ + 1, buffer_, bytes));

    EXPECT_EQ(0, stream_.read(buffer_, sizeof(buffer_)));
    EXPECT_EQ(0, stream_.read(buffer_, sizeof(buffer_)));
};

int main() {
    return TestSuite::globalInstance()->runAll();
}
