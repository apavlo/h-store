// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstdlib>

#include "io/mockstreams.h"
#include "messageconnection.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

class ReadBufferTest : public Test {
public:
    ReadBuffer buffer_;
    io::MockInputStream input_;
    char bytes_[ReadBuffer::BLOCK_SIZE];
};

TEST_F(ReadBufferTest, Simple) {
    input_.addFakeData(ReadBuffer::BLOCK_SIZE - 1);

    bool result = buffer_.readAll(&input_);
    EXPECT_EQ(true, result);

    int count = buffer_.read(bytes_, sizeof(bytes_));
    EXPECT_EQ(ReadBuffer::BLOCK_SIZE - 1, count);
}

TEST_F(ReadBufferTest, CrossBlockRead) {
    input_.addFakeData(ReadBuffer::BLOCK_SIZE*2);

    bool result = buffer_.readAll(&input_);
    EXPECT_EQ(true, result);

    int count = buffer_.read(bytes_, sizeof(bytes_)-1);
    EXPECT_EQ(sizeof(bytes_)-1, count);
    count = buffer_.read(bytes_, 2);
    EXPECT_EQ(2, count);
}

TEST_F(ReadBufferTest, NoMoreDataRead) {
    input_.addFakeData(ReadBuffer::BLOCK_SIZE + 100);

    bool result = buffer_.readAll(&input_);
    EXPECT_EQ(true, result);

    // Call read twice: once per block
    EXPECT_EQ(2, input_.read_count_);
    EXPECT_EQ(ReadBuffer::BLOCK_SIZE + 100, buffer_.available());

    // Fill the 2nd block
    input_.addFakeData(ReadBuffer::BLOCK_SIZE - 100);
    result = buffer_.readAll(&input_);
    EXPECT_EQ(true, result);

    // Calls read twice: once to fill the block, again to check for more data
    EXPECT_EQ(4, input_.read_count_);
    EXPECT_EQ(ReadBuffer::BLOCK_SIZE * 2, buffer_.available());

    input_.addFakeData(100);
    result = buffer_.readAll(&input_);
    EXPECT_EQ(true, result);

    // Calls read once
    EXPECT_EQ(5, input_.read_count_);
    EXPECT_EQ(ReadBuffer::BLOCK_SIZE * 2 + 100, buffer_.available());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
