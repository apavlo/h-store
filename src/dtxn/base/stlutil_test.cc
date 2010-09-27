// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <map>
#include <set>
#include <vector>

#include "base/stlutil.h"
#include "stupidunit/stupidunit.h"

using std::vector;

TEST(STLDeleteElements, Vector) {
    std::vector<int*> foo;
    foo.push_back(new int(42));

    STLDeleteElements(&foo);
    EXPECT_EQ(0, foo.size());
}

TEST(STLDeleteElements, Set) {
    std::set<int*> foo;
    foo.insert(new int(42));

    STLDeleteElements(&foo);
    EXPECT_EQ(0, foo.size());
}

TEST(STLDeleteValues, Map) {
    std::map<int, int*> foo;
    foo.insert(std::make_pair(5, new int(42)));

    STLDeleteValues(&foo);
    EXPECT_EQ(0, foo.size());
}

TEST(contains, Simple) {
    vector<int> foo;
    EXPECT_FALSE(base::contains(foo, 42));
    foo.push_back(99);
    foo.push_back(104);
    foo.push_back(2);
    EXPECT_FALSE(base::contains(foo, 42));
    EXPECT_TRUE(base::contains(foo, 99));
    EXPECT_TRUE(base::contains(foo, 2));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
