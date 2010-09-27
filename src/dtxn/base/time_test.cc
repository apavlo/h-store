// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#define __STDC_CONSTANT_MACROS
#include "base/time.h"
#include "stupidunit/stupidunit.h"

using namespace base;

TEST(TimevalValid, Simple) {
    struct timeval a = { 1, 0 };
    EXPECT_TRUE(timevalValid(a));

    a.tv_sec = -1;
    EXPECT_FALSE(timevalValid(a));

    a.tv_sec = 1234567;
    a.tv_usec = 999999;
    EXPECT_TRUE(timevalValid(a));

    a.tv_usec = 1000000;
    EXPECT_FALSE(timevalValid(a));

    a.tv_usec = -1;
    EXPECT_FALSE(timevalValid(a));
}

TEST(TimevalDiffUS, Simple) {
    struct timeval a = { 1, 0 };
    struct timeval b = { 1, 0 };

    EXPECT_EQ(0, timevalDiffMicroseconds(a, b));
    EXPECT_EQ(0, timevalDiffMicroseconds(b, a));

    b.tv_usec = 100;
    EXPECT_EQ(100, timevalDiffMicroseconds(b, a));
    EXPECT_EQ(-100, timevalDiffMicroseconds(a, b));

    b.tv_sec = 3;
    b.tv_usec = 999999;
    EXPECT_EQ(-2999999, timevalDiffMicroseconds(a, b));
    EXPECT_EQ(2999999, timevalDiffMicroseconds(b, a));
}

TEST(TimevalAddUS, Simple) {
    struct timeval a = { 1, 0 };

    timevalAddMicroseconds(&a, 1);
    EXPECT_EQ(1, a.tv_sec);
    EXPECT_EQ(1, a.tv_usec);

    timevalAddMicroseconds(&a, 1000001);
    EXPECT_EQ(2, a.tv_sec);
    EXPECT_EQ(2, a.tv_usec);

    timevalAddMicroseconds(&a, 999998);
    EXPECT_EQ(3, a.tv_sec);
    EXPECT_EQ(0, a.tv_usec);
}


TEST(TimespecValid, Simple) {
    struct timespec a = { 1, 0 };
    EXPECT_TRUE(timespecValid(a));

    a.tv_sec = -1;
    EXPECT_FALSE(timespecValid(a));

    a.tv_sec = 1234567;
    a.tv_nsec = 999999999;
    EXPECT_TRUE(timespecValid(a));

    a.tv_nsec = 1000000000;
    EXPECT_FALSE(timespecValid(a));

    a.tv_nsec = -1;
    EXPECT_FALSE(timespecValid(a));
}

TEST(TimespecDiffUS, Simple) {
    struct timespec a = { 1, 0 };
    struct timespec b = { 1, 0 };

    EXPECT_EQ(0, timespecDiffNanoseconds(a, b));
    EXPECT_EQ(0, timespecDiffNanoseconds(b, a));

    b.tv_nsec = 100;
    EXPECT_EQ(100, timespecDiffNanoseconds(b, a));
    EXPECT_EQ(-100, timespecDiffNanoseconds(a, b));

    b.tv_sec = 3;
    b.tv_nsec = 999999999;
    EXPECT_EQ(INT64_C(-2999999999), timespecDiffNanoseconds(a, b));
    EXPECT_EQ(INT64_C(2999999999), timespecDiffNanoseconds(b, a));
}

TEST(TimespecAddUS, Simple) {
    struct timespec a = { 1, 0 };

    timespecAddNanoseconds(&a, 1);
    EXPECT_EQ(1, a.tv_sec);
    EXPECT_EQ(1, a.tv_nsec);

    timespecAddNanoseconds(&a, 1000000001);
    EXPECT_EQ(2, a.tv_sec);
    EXPECT_EQ(2, a.tv_nsec);

    timespecAddNanoseconds(&a, 999999998);
    EXPECT_EQ(3, a.tv_sec);
    EXPECT_EQ(0, a.tv_nsec);
}

TEST(TimespecConvert, Simple) {
    struct timespec a = { 1, 999999499 };

    struct timeval b;
    timespecToTimeval(&b, a);
    EXPECT_EQ(1, b.tv_sec);
    EXPECT_EQ(999999, b.tv_usec);

    a.tv_nsec = 999999500;
    timespecToTimeval(&b, a);
    EXPECT_EQ(2, b.tv_sec);
    EXPECT_EQ(0, b.tv_usec);

    a.tv_nsec = 999999999;
    timespecToTimeval(&b, a);
    EXPECT_EQ(2, b.tv_sec);
    EXPECT_EQ(0, b.tv_usec);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}

