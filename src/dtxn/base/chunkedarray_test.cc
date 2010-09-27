// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/chunkedarray.h"
#include "stupidunit/stupidunit.h"

TEST(ChunkedArray, Simple) {
    base::ChunkedArray<int> buffer;
    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(0, buffer.size());

    buffer.push_back(10);
    buffer.push_back(11);
    buffer.push_back(12);
    EXPECT_FALSE(buffer.empty());
    EXPECT_EQ(3, buffer.size());
    EXPECT_EQ(10, buffer.at(0));
    EXPECT_EQ(11, buffer.at(1));
    EXPECT_EQ(12, buffer.at(2));
    EXPECT_DEATH(buffer.at(3));
}

TEST(ChunkedArray, Const) {
    base::ChunkedArray<int> buffer;
    buffer.push_back(42);
    
    const base::ChunkedArray<int>& cbuffer = buffer;
    EXPECT_FALSE(cbuffer.empty());
    EXPECT_EQ(1, cbuffer.size());
    EXPECT_EQ(42, cbuffer.at(0));
}

TEST(ChunkedArray, Object) {
    base::ChunkedArray<std::string> buffer;

    for (int i = 0; i < buffer.BLOCK_ELEMENTS; ++i) {
        buffer.push_back("hello");
    }

    for (int i = 0; i < buffer.BLOCK_ELEMENTS; ++i) {
        EXPECT_EQ("hello", buffer.at(i));
    }
    EXPECT_DEATH(buffer.at(buffer.BLOCK_ELEMENTS));

    buffer.clear();
    buffer.push_back("world");
    EXPECT_EQ("world", buffer.at(0));
    EXPECT_DEATH(buffer.at(1));
}

TEST(ChunkedArray, Big) {
    base::ChunkedArray<int> buffer;
    for (int i = 0; i < 10000; ++i) {
        buffer.push_back(i);
    }

    EXPECT_EQ(10000, buffer.size());
    for (int i = 0; i < 10000; ++i) {
        EXPECT_EQ(i, buffer.at(i));
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
