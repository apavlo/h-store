// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <limits>

#include "base/array.h"
#include "stupidunit/stupidunit.h"

using base::Array;
using base::arraySize;

TEST(Array, Empty) {
    Array<int> a(0);
    EXPECT_EQ(0, a.size());
    EXPECT_DEATH(a[0]);
}

TEST(Array, Single) {
    Array<int> a(1);
    a[0] = 42;
    EXPECT_EQ(42, a[0]);
}

TEST(Array, Multiple) {
    int aval = 42;
    int b = 72;
    int* values[] = { &aval, &b, NULL };

    Array<int*> a(arraySize(values));
    for (int i = 0; i < arraySize(values); ++i) {
        a[i] = values[i];
    }

    EXPECT_EQ(arraySize(values), a.size());
    for (int i = 0; i < arraySize(values); ++i) {
        EXPECT_EQ(values[i], a[i]);
    }

    EXPECT_DEATH(a[std::numeric_limits<size_t>::max()]);
    EXPECT_DEATH(a[3]);

    int i = 0;
    for (Array<int*>::iterator it = a.begin(); it != a.end(); ++it) {
        EXPECT_EQ(values[i], *it);
        i += 1;
    }
}

TEST(Array, Const) {
    Array<int> a(1);
    a[0] = 42;

    const Array<int>& a_const = a;
    EXPECT_EQ(42, a_const[0]);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
