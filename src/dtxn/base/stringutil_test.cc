// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/stringutil.h"
#include "stupidunit/stupidunit.h"

using std::string;

TEST(StringArray, Simple) {
    string empty;
    string foo("foo");
    string copy = foo;

    EXPECT_EQ(NULL, base::stringArray(&empty));

    base::stringArray(&foo)[0] = 'b';
    EXPECT_EQ("boo", foo);
    EXPECT_EQ("foo", copy);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
