// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <stdint.h>

#include "base/cast.h"
#include "stupidunit/stupidunit.h"

TEST(RangeCastSignedToUnsigned, Good) {
    EXPECT_EQ(static_cast<unsigned int>(std::numeric_limits<int>::max()),
            assert_range_cast<unsigned int>(std::numeric_limits<int>::max()));
    EXPECT_EQ(0, assert_range_cast<unsigned int>(0));
}

TEST(RangeCastSignedToUnsigned, Bad) {
    EXPECT_DEATH(assert_range_cast<unsigned int>(-1));
    EXPECT_DEATH(assert_range_cast<unsigned int>(std::numeric_limits<int>::min()));
}

TEST(RangeCastUnsignedToSigned, Good) {
    EXPECT_EQ(std::numeric_limits<int>::max(), assert_range_cast<int>(
            static_cast<unsigned int>(std::numeric_limits<int>::max())));
    EXPECT_EQ(0, assert_range_cast<int>(static_cast<unsigned int>(0)));
}

TEST(RangeCastUnsignedToSigned, Bad) {
    EXPECT_DEATH(assert_range_cast<int>(std::numeric_limits<unsigned int>::max()));
    EXPECT_DEATH(assert_range_cast<int>(
            static_cast<unsigned int>(std::numeric_limits<int>::max())+1));
}

TEST(RangeCastSignedToSigned, Good) {
    // lower bound of int8_t
    int8_t c = assert_range_cast<int8_t>(-128);
    EXPECT_EQ(-128, c);
    EXPECT_EQ(-128, assert_range_cast<int>(c));

    // upper bound of int8_t
    c = assert_range_cast<int8_t>(127);
    EXPECT_EQ(127, c);
    EXPECT_EQ(127, assert_range_cast<int>(c));
}

TEST(RangeCastSignedToSigned, Bad) {
    EXPECT_DEATH(assert_range_cast<int8_t>(-129));
    EXPECT_DEATH(assert_range_cast<int8_t>(128));
}

TEST(RangeCastUnsignedToUnsigned, Good) {
    // lower bound of uint8_t
    uint8_t c = assert_range_cast<uint8_t>(static_cast<unsigned int>(0));
    EXPECT_EQ(0, c);
    EXPECT_EQ(0, assert_range_cast<unsigned int>(c));

    // upper bound of uint8_t
    c = assert_range_cast<uint8_t>(static_cast<unsigned int>(255));
    EXPECT_EQ(255, c);
    EXPECT_EQ(255, assert_range_cast<unsigned int>(c));
}

TEST(RangeCastUnsignedToUnsigned, Bad) {
    EXPECT_DEATH(assert_range_cast<uint8_t>(256));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
