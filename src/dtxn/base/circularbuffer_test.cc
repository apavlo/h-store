// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#define __STDC_CONSTANT_MACROS  // for INT64_C in stdint.h
#define __STDC_FORMAT_MACROS  // for PRIi64 in inttypes.h

#include <algorithm>

#include <stdint.h>
#include <inttypes.h>

#include "base/circularbuffer.h"


#include "stupidunit/stupidunit.h"

class CircularBufferTest : public Test {
protected:
    CircularBuffer<int> buffer_;
};

TEST_F(CircularBufferTest, EmptyDequeue) {
    EXPECT_DEATH(buffer_.dequeue());
    EXPECT_DEATH(buffer_.pop_front());
    EXPECT_DEATH(buffer_.front());
    EXPECT_DEATH(buffer_.back());
}

// Put a single element into the buffer multiple times. This will loop around
// the initial starting point.
TEST_F(CircularBufferTest, SingleElement) {
    for (int i = 0; i < buffer_.INITIAL_CAPACITY + 5; ++i) {
        EXPECT_EQ(0, buffer_.size());
        buffer_.push_back(i);
        EXPECT_EQ(1, buffer_.size());
        EXPECT_EQ(i, buffer_.front());
        EXPECT_EQ(i, buffer_.dequeue());
    }
}

TEST_F(CircularBufferTest, Clear) {
    buffer_.push_back(-42);
    EXPECT_EQ(1, buffer_.size());
    buffer_.clear();
    EXPECT_EQ(0, buffer_.size());
    buffer_.push_back(-42);
    EXPECT_EQ(1, buffer_.size());
}

TEST_F(CircularBufferTest, ReverseQueue) {
    buffer_.push_front(42);
    EXPECT_EQ(42, buffer_.front());
    EXPECT_EQ(42, buffer_.back());

    buffer_.push_front(43);
    EXPECT_EQ(43, buffer_.front());
    EXPECT_EQ(42, buffer_.back());

    buffer_.pop_back();
    EXPECT_EQ(43, buffer_.front());
    EXPECT_EQ(43, buffer_.back());

    buffer_.pop_back();
    EXPECT_EQ(0, buffer_.size());
    EXPECT_TRUE(buffer_.empty());
}

bool equal(const std::vector<int>& vec, CircularBuffer<int>& buf) {
    if (vec.size() != buf.size()) return false;

    for (int i = 0; i < vec.size(); ++i) {
        if (vec[i] != buf.at(i)) return false;
    }
    return true;
}

TEST_F(CircularBufferTest, Erase) {
    // Shift the circular buffer so it wraps
    for (int i = 0; i < buffer_.INITIAL_CAPACITY - 3; ++i) {
        buffer_.push_back(0);
    }
    buffer_.clear();

    // Fill the reference implementation and buffer with some values
    std::vector<int> reference;
    reference.push_back(-42);
    reference.push_back(0);
    reference.push_back(99);
    reference.push_back(1);
    reference.push_back(107);
    for (int i = 0; i < reference.size(); ++i) {
        buffer_.push_back(reference[i]);
    }
    ASSERT_TRUE(equal(reference, buffer_));

    // Erase from the middle
    reference.erase(reference.begin()+2);
    buffer_.erase(2);
    EXPECT_TRUE(equal(reference, buffer_));

    // Erase from the beginning
    reference.erase(reference.begin());
    buffer_.erase(0);
    EXPECT_TRUE(equal(reference, buffer_));

    // Erase from the end
    reference.erase(reference.begin()+2);
    buffer_.erase(2);
    EXPECT_TRUE(equal(reference, buffer_));
}

TEST_F(CircularBufferTest, EraseValue) {
    EXPECT_FALSE(buffer_.eraseValue(42));
    buffer_.push_back(42);
    buffer_.push_back(43);
    buffer_.push_back(42);

    EXPECT_FALSE(buffer_.eraseValue(-1));
    EXPECT_TRUE(buffer_.eraseValue(42));
    EXPECT_EQ(2, buffer_.size());
    EXPECT_EQ(43, buffer_.at(0));
    EXPECT_EQ(42, buffer_.at(1));

    EXPECT_TRUE(buffer_.eraseValue(42));
    EXPECT_EQ(1, buffer_.size());
    EXPECT_EQ(43, buffer_.at(0));

    EXPECT_TRUE(buffer_.eraseValue(43));
    EXPECT_EQ(0, buffer_.size());
}

TEST_F(CircularBufferTest, AlmostFullBuffer) {
    // The buffer holds a maximum of capacity - 1 elements: fill it
    for (int i = 0; i < buffer_.INITIAL_CAPACITY - 1; ++i) {
        buffer_.push_back(i);
        EXPECT_EQ(i+1, buffer_.size());
    }

    for (int i = 0; i < buffer_.INITIAL_CAPACITY - 1; ++i) {
        EXPECT_EQ(i, buffer_.dequeue());
        EXPECT_EQ(buffer_.INITIAL_CAPACITY - 1 - i - 1, buffer_.size());
    }
}

TEST_F(CircularBufferTest, ResizeBuffer) {
    // The buffer holds a maximum of capacity - 1 elements: force it to resize
    // somewhere in the middle
    buffer_.push_back(-42);
    buffer_.pop_front();
    for (int i = 0; i < buffer_.INITIAL_CAPACITY; ++i) {
        buffer_.push_back(i);
        EXPECT_EQ(i+1, buffer_.size());
    }

    for (int i = 0; i < buffer_.INITIAL_CAPACITY; ++i) {
        EXPECT_EQ(i, buffer_.dequeue());
        EXPECT_EQ(buffer_.INITIAL_CAPACITY - i - 1, buffer_.size());
    }
}

TEST_F(CircularBufferTest, ResizeFront) {
    // The buffer holds a maximum of capacity - 1 elements: force it to resize
    // somewhere in the middle
    buffer_.push_front(-42);
    buffer_.pop_front();
    for (int i = 0; i < buffer_.INITIAL_CAPACITY; ++i) {
        buffer_.push_front(i);
        EXPECT_EQ(i+1, buffer_.size());
    }

    for (int i = 0; i < buffer_.INITIAL_CAPACITY; ++i) {
        EXPECT_EQ(buffer_.INITIAL_CAPACITY - i - 1, buffer_.dequeue());
        EXPECT_EQ(buffer_.INITIAL_CAPACITY - i - 1, buffer_.size());
    }
}

TEST_F(CircularBufferTest, Iterate) {
    // force it to start somewhere in the middle
    for (int i = 0; i < 3; ++i) {
        buffer_.push_back(-42);
        buffer_.pop_front();
    }

    // Fill the buffer
    for (int i = 0; i < buffer_.INITIAL_CAPACITY - 1; ++i) {
        buffer_.push_back(i);
        EXPECT_EQ(i+1, buffer_.size());
    }

    // Iterate over all the elements
    for (size_t i = 0; i < buffer_.size(); ++i) {
        EXPECT_EQ(i, buffer_.at(i));
    }

    // Use the STL iterator, which is worse but still supported
    int i = 0;
    for (CircularBuffer<int>::iterator it = buffer_.begin(); it != buffer_.end(); ++it) {
        EXPECT_EQ(i, *it);
        i += 1;
    }
}

TEST_F(CircularBufferTest, IteratorDereference) {
    typedef CircularBuffer<std::pair<int, int> > BufferType;
    BufferType buffer;
    buffer.push_back(std::make_pair<int, int>(1, 2));

    BufferType::iterator i = buffer.begin();
    EXPECT_EQ(1, i->first);
    EXPECT_EQ(1, (*i).first);
    EXPECT_EQ(2, i->second);
    EXPECT_EQ(2, (*i).second);
}

TEST_F(CircularBufferTest, STLIteratorCompatibility) {
    buffer_.push_back(42);
    buffer_.push_back(43);
    buffer_.push_back(44);

    CircularBuffer<int>::iterator i = std::find(buffer_.begin(), buffer_.end(), 43);
    EXPECT_EQ(43, *i);
}

TEST_F(CircularBufferTest, ConstCompatibility) {
    buffer_.push_back(42);
    buffer_.push_back(43);
    buffer_.push_back(44);

    const CircularBuffer<int>& cbuffer = buffer_;
    EXPECT_EQ(42, cbuffer.at(0));
    EXPECT_EQ(42, cbuffer.front());

    CircularBuffer<int>::const_iterator i = std::find(cbuffer.begin(), cbuffer.end(), 43);
    EXPECT_EQ(43, *i);
}

// Only works on 64-bit machines with lots of RAM (needs ~3G). And its slow: disabled.
//~ TEST_F(CircularBufferTest, IntOverflow) {
    //~ CircularBuffer<char> buffer;
    
    //~ static const int64_t LIMIT = INT64_C(1) << 31;
    //~ static const int64_t OUTPUT = LIMIT >> 5;
    //~ for (int64_t i = 0; i < LIMIT; ++i) {
        //~ buffer.push_back('a');
        //~ if (i % OUTPUT == 0) printf("%" PRIi64 "\n", i);
    //~ }
    //~ EXPECT_EQ(LIMIT, buffer.size());
    
    //~ buffer.push_back('b');
    //~ buffer.push_back('c');
    //~ EXPECT_EQ(LIMIT + 2, buffer.size());
    //~ EXPECT_EQ('a', buffer.dequeue());
    //~ EXPECT_EQ('a', buffer.dequeue());
    //~ EXPECT_EQ('c', buffer.back());
    //~ buffer.pop_back();
    //~ EXPECT_EQ('b', buffer.back());
    //~ buffer.pop_back();
    //~ EXPECT_EQ('a', buffer.back());
    //~ EXPECT_EQ(LIMIT - 2, buffer.size());
//~ }

int main() {
    return TestSuite::globalInstance()->runAll();
}
