// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/slidingarray.h"
#include "stupidunit/stupidunit.h"

using base::SlidingArray;

class SlidingArrayTest : public Test {
public:
    SlidingArray<int> array_;
};

TEST_F(SlidingArrayTest, PushPopSizeAtFront) {
    EXPECT_EQ(0, array_.firstIndex());
    EXPECT_EQ(0, array_.nextIndex());
    EXPECT_EQ(0, array_.size());
    EXPECT_DEATH(array_.at(0));
    EXPECT_DEATH(array_.front());
    EXPECT_DEATH(array_.pop_front());
    EXPECT_TRUE(array_.empty());

    array_.push_back(42);
    EXPECT_EQ(0, array_.firstIndex());
    EXPECT_EQ(1, array_.nextIndex());
    EXPECT_EQ(1, array_.size());
    EXPECT_EQ(42, array_.at(0));
    EXPECT_EQ(42, array_.front());
    EXPECT_FALSE(array_.empty());

    array_.pop_front();
    EXPECT_EQ(1, array_.firstIndex());
    EXPECT_EQ(1, array_.nextIndex());
    EXPECT_EQ(0, array_.size());
    EXPECT_DEATH(array_.at(0));
    EXPECT_DEATH(array_.front());
    EXPECT_TRUE(array_.empty());

    array_.push_back(43);
    EXPECT_EQ(1, array_.firstIndex());
    EXPECT_EQ(2, array_.nextIndex());
    EXPECT_EQ(1, array_.size());
    EXPECT_DEATH(array_.at(0));
    EXPECT_EQ(43, array_.at(1));
    EXPECT_EQ(43, array_.front());

    EXPECT_EQ(43, array_.dequeue());
    EXPECT_TRUE(array_.empty());
    EXPECT_DEATH(array_.dequeue());
}

TEST_F(SlidingArrayTest, Iterator) {
    array_.push_back(42);
    array_.push_back(43);

    SlidingArray<int>::iterator it = array_.begin();
    EXPECT_EQ(42, *it);

    array_.pop_front();
    ++it;
    EXPECT_EQ(43, *it);

    ++it;
    EXPECT_EQ(array_.end(), it);
    array_.push_back(44);
    ASSERT_TRUE(it != array_.end());
    EXPECT_EQ(44, *it);
    
    array_.pop_front();
    EXPECT_EQ(44, *it);
}

TEST_F(SlidingArrayTest, Insert) {
    array_.insert(0, 42);

    // insert is permitted provided it goes forward in id space
    EXPECT_DEATH(array_.insert(0, 43));
    
    array_.insert(1, 43);
    EXPECT_EQ(2, array_.size());
    array_.insert(100, 44);
    EXPECT_EQ(101, array_.size());

    // missing elements are default constructed
    for (int i = 2; i < 100; ++i) {
        EXPECT_EQ(0, array_.at(i));
    }
    EXPECT_EQ(44, array_.at(100));

    array_.pop_front();
    EXPECT_DEATH(array_.insert(0, -2));
    EXPECT_EQ(100, array_.size());

    SlidingArray<int> second;
    second.push_back(42);
    second.pop_front();

    second.insert(100, 44);
    EXPECT_EQ(1, second.size());
    EXPECT_EQ(100, second.firstIndex());
}

TEST_F(SlidingArrayTest, Back) {
    EXPECT_DEATH(array_.back());
    array_.push_back(42);
    EXPECT_EQ(42, array_.back());
}

TEST_F(SlidingArrayTest, Clear) {
    array_.clear();
    array_.push_back(42);
    array_.push_back(43);
    EXPECT_EQ(0, array_.firstIndex());
    EXPECT_EQ(2, array_.nextIndex());

    array_.clear();
    EXPECT_EQ(2, array_.firstIndex());
    EXPECT_EQ(2, array_.nextIndex());
}

TEST_F(SlidingArrayTest, ConstCorrectness) {
    array_.push_back(42);
    
    const SlidingArray<int>& c = array_;
    EXPECT_EQ(42, c.front());
    EXPECT_EQ(42, c.at(0));

    SlidingArray<int>::const_iterator it = c.begin();
    EXPECT_NE(it, c.end());
    EXPECT_EQ(42, *it);
    ++it;
    EXPECT_EQ(it, c.end());
}


int main() {
    return TestSuite::globalInstance()->runAll();
}
