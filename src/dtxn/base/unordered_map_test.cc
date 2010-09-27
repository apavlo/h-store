// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <tr1/unordered_map>

#include "base/unordered_map.h"
#include "stupidunit/stupidunit.h"

TEST(unordered_map, Int64) {
    base::unordered_map<int64_t, int> foo;
    foo[0] = 42;
    EXPECT_EQ(42, foo[0]);
}

TEST(unordered_map, VoidStar) {
    base::unordered_map<void*, int> foo;
    foo[NULL] = 42;
    EXPECT_EQ(42, foo[NULL]);
}

TEST(unordered_map, ConstVoidStar) {
    base::unordered_map<const void*, int> foo;
    foo[NULL] = 42;
    EXPECT_EQ(42, foo[NULL]);
}

class Foo;
TEST(unordered_map, ConstFooStar) {
    base::unordered_map<const Foo*, int> foo;
    foo[NULL] = 42;
    EXPECT_EQ(42, foo[NULL]);
}

TEST(unordered_map, ConstIntStar) {
    base::unordered_map<const int*, int> foo;
    foo[NULL] = 42;
    EXPECT_EQ(42, foo[NULL]);
}

TEST(unordered_map, String) {
    base::unordered_map<std::string, int> foo;
    std::string key = "hello";
    foo[key] = 42;
    EXPECT_EQ(42, foo[key]);
}

TEST(unordered_map, Erase) {
    base::unordered_map<int, int> foo;
    foo[0] = 92;
    foo[1] = 77;
    for (base::unordered_map<int, int>::iterator i = foo.begin(); i != foo.end();) {
        i = foo.erase(i);
    }
    EXPECT_TRUE(foo.empty());

    foo[92] = 1;
    foo[100] = 2;
    for (base::unordered_map<int, int>::const_iterator i = foo.begin(); i != foo.end();) {
        i = foo.erase(i);
    }
    EXPECT_TRUE(foo.empty());
}

TEST(unordered_map, ConstFind) {
    const base::unordered_map<int, int> foo;
    base::unordered_map<int, int>::const_iterator i = foo.find(0);
    EXPECT_TRUE(i == foo.end());
}

struct CustomHash {
    size_t operator()(int) const {
        return 0;
    }
};

TEST(unordered_map, CustomHash) {
    base::unordered_map<int, int, CustomHash> foo;
}

TEST(unordered_map, EraseByKey) {
    base::unordered_map<int, int> foo;
    foo[42] = 0;

    size_t count = foo.erase(42);
    EXPECT_EQ(1, count);
    EXPECT_TRUE(foo.empty());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
