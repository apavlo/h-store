// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "replication/cachedslidingarray.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

TEST(CachedCircularBuffer, Simple) {
    replication::CachedSlidingArray<string> buffer;
    EXPECT_TRUE(buffer.empty());

    EXPECT_EQ(0, buffer.firstIndex());
    EXPECT_EQ(0, buffer.nextIndex());
    string* a = buffer.add();
    EXPECT_FALSE(buffer.empty());
    a->assign("hello world");

    EXPECT_EQ("hello world", buffer.front());
    EXPECT_EQ("hello world", buffer.at(0));

    buffer.pop_front();
    EXPECT_TRUE(buffer.empty());

    EXPECT_EQ(1, buffer.nextIndex());
    string* b = buffer.add();
    EXPECT_EQ(a, b);
    EXPECT_TRUE(b->empty());
    b->assign("foo");

    EXPECT_EQ("foo", buffer.front());
    EXPECT_EQ("foo", buffer.at(1));

    EXPECT_EQ(b, buffer.mutableAt(1));
    EXPECT_DEATH(buffer.mutableAt(2));
}

TEST(CachedCircularBuffer, LeakRecycledElements) {
    // The simple test leaks elements in the buffer, this leaks elements in the reuse list
    replication::CachedSlidingArray<string> buffer;
    buffer.add();
    buffer.pop_front();
    EXPECT_TRUE(buffer.empty());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
