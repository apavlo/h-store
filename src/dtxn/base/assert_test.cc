// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/assert.h"
#include "stupidunit/stupidunit.h"

TEST(Check, EscapePercent) {
    // Verifies that CHECK properly handles % symbols
    // Previously this would produce "error: unknown conversion type character"
    CHECK(42 % 2 == 0);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
