// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "dtxn/configparser.h"
#include "stupidunit/stupidunit.h"

using dtxn::Partition;
using std::string;
using std::vector;

TEST(ConfigParser, Empty) {
    vector<Partition> partitions;
    parseConfiguration("", &partitions);
    EXPECT_EQ(0, partitions.size());
}

TEST(ConfigParser, CommentsBlanks) {
    vector<Partition> partitions;
    parseConfiguration("# hello\n   \n\t\t  # comment\n\n", &partitions);
    EXPECT_EQ(0, partitions.size());
}

TEST(ConfigParser, Simple) {
    vector<Partition> partitions;
    parseConfiguration("foo foo\n1.2.3.4 12345\n1.2.3.5 12346\n\nbar\n1.2.3.5 12346\n\n\n\n", &partitions);
    ASSERT_EQ(2, partitions.size());

    NetworkAddress one;
    ASSERT_EQ(true, one.parse("1.2.3.4 12345"));

    NetworkAddress two;
    ASSERT_EQ(true, two.parse("1.2.3.5 12346"));

    EXPECT_EQ("foo foo", partitions[0].criteria());
    ASSERT_EQ(2, partitions[0].numReplicas());
    EXPECT_EQ(one, partitions[0].replica(0));
    EXPECT_EQ(two, partitions[0].replica(1));

    EXPECT_EQ("bar", partitions[1].criteria());
    ASSERT_EQ(1, partitions[1].numReplicas());
    EXPECT_EQ(two, partitions[1].replica(0));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
