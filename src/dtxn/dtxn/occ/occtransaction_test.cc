// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/locks/lock.h"
#include "dtxn/occ/occtransaction.h"
#include "stupidunit/stupidunit.h"

using namespace dtxn;

class OCCTransactionTest : public Test {
public:
    OCCTransactionTest() :
            one_(NULL, "foo", true, false),
            two_(NULL, "foo", true, false) {}

    Lock a;
    Lock b;
    OCCTransaction one_;
    OCCTransaction two_;
};

TEST_F(OCCTransactionTest, SinglePartitionConflicts) {
    EXPECT_TRUE(one_.tryReadLock(&a));
    EXPECT_TRUE(two_.tryReadLock(&a));

    EXPECT_TRUE(one_.canReorder(two_));
    EXPECT_TRUE(two_.canReorder(one_));

    // add an extra lock: no change
    EXPECT_TRUE(two_.tryWriteLock(&b));
    EXPECT_TRUE(one_.canReorder(two_));
    EXPECT_TRUE(two_.canReorder(one_));

    // upgrade one_ to a write lock: no reorder
    EXPECT_TRUE(one_.tryWriteLock(&a));
    EXPECT_FALSE(one_.canReorder(two_));
    EXPECT_FALSE(two_.canReorder(one_));

    // upgrade both to write locks: no change
    EXPECT_TRUE(two_.tryWriteLock(&a));
    EXPECT_FALSE(one_.canReorder(two_));
    EXPECT_FALSE(two_.canReorder(one_));
}

TEST_F(OCCTransactionTest, Depends) {
    EXPECT_TRUE(one_.tryReadLock(&a));
    EXPECT_TRUE(two_.tryReadLock(&a));

    // read locks: no dependencies
    EXPECT_FALSE(one_.dependsOn(two_));
    EXPECT_FALSE(one_.dependsOn(two_));

    // extra lock: no change
    EXPECT_TRUE(two_.tryWriteLock(&b));
    EXPECT_FALSE(one_.dependsOn(two_));
    EXPECT_FALSE(two_.dependsOn(one_));

    EXPECT_TRUE(one_.tryReadLock(&b));
    // if two executed before one, one depends on it (write then read)
    EXPECT_TRUE(one_.dependsOn(two_));
    // if one executed before two, no dependency (read then write)
    EXPECT_FALSE(two_.dependsOn(one_));

    // if they both wrote, then there are dependencies both ways
    EXPECT_TRUE(one_.tryWriteLock(&b));
    EXPECT_TRUE(one_.dependsOn(two_));
    EXPECT_TRUE(two_.dependsOn(one_));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
