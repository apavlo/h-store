// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/unordered_set.h"
#include "stupidunit/stupidunit.h"

TEST(unordered_set, Copy) {
    base::unordered_set<int> foo;
    foo.insert(42);
    base::unordered_set<int> copy = foo;
    EXPECT_TRUE(copy.find(42) != copy.end());
}

TEST(unordered_set, ConstVoidStar) {
    base::unordered_set<const void*> foo;
    foo.insert(NULL);
    EXPECT_TRUE(foo.find(NULL) != foo.end());
}

TEST(unordered_set, TStar) {
    base::unordered_set<int*> foo;
    foo.insert(NULL);
    EXPECT_TRUE(foo.find(NULL) != foo.end());
}

TEST(unordered_set, ConstTStar) {
    base::unordered_set<const int*> foo;
    foo.insert(NULL);
    EXPECT_TRUE(foo.find(NULL) != foo.end());
}

TEST(unordered_set, erase) {
    base::unordered_set<int> foo;
    foo.insert(42);
    EXPECT_EQ(1, foo.erase(42));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
