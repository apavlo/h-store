// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstring>
#include <limits>

#include "serialization.h"
#include "stupidunit/stupidunit.h"

using namespace serialization;
using std::string;

class SerializationTest : public Test {
public:
    template <typename T>
    void unserialize(T* output) {
        const char* real_end = out_.data() + out_.size();
        const char* end = deserialize(output, out_.data(), real_end);
        EXPECT_EQ(real_end, end);
    }

    template <typename T>
    bool roundTrip(const T& input) {
        out_.clear();
        serialization::serialize(input, &out_);
        T value;
        unserialize(&value);
        return value == input;
    }

    string out_;
};

TEST_F(SerializationTest, Bool) {
    // Set foo to a "denormalized" value: 42
    bool foo;
    *(int8_t*) &foo = 42;
    serialize(foo, &out_);
    EXPECT_EQ(1, out_.size());
    EXPECT_EQ(1, out_[0]);

    out_.clear();
    foo = false;
    serialize(foo, &out_);
    EXPECT_EQ(1, out_.size());
    EXPECT_EQ(0, out_[0]);
}

TEST_F(SerializationTest, Char) {
    char c = std::numeric_limits<char>::max();
    serialize(c, &out_);
    EXPECT_EQ(1, out_.size());

    char x;
    unserialize(&x);
    EXPECT_EQ(std::numeric_limits<char>::max(), x);
}

TEST_F(SerializationTest, RawBytes) {
    static const char data[] = "hello world";

    // MUST NOT COMPILE. TODO: Compile death tests?
    //~ serialize(data, &out_);
    serialize(data, sizeof(data), &out_);

    string result;
    unserialize(&result);
    EXPECT_EQ(sizeof(data), result.size());
    EXPECT_EQ(0, memcmp(data, result.data(), result.size()));
}

TEST_F(SerializationTest, Int64) {
    EXPECT_TRUE(roundTrip(std::numeric_limits<int64_t>::min()));
    EXPECT_TRUE(roundTrip((int64_t) -1));
    EXPECT_TRUE(roundTrip((int64_t) 0));
    EXPECT_TRUE(roundTrip((int64_t) 1));
    EXPECT_TRUE(roundTrip(std::numeric_limits<int64_t>::max()));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
