// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <limits>

#include "strings/json.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

using namespace strings;

TEST(JSONEscape, Simple) {
    static const char TEST[] = "\"\\\b\f\n\r\t\x1f\x0f";
    // include the NULL byte
    const string test(TEST, sizeof(TEST));

    EXPECT_EQ("\\\"\\\\\\b\\f\\n\\r\\t\\u001f\\u000f\\u0000", jsonEscape(test));
}

TEST(JSONObject, Simple) {
    JSONObject obj;
    EXPECT_DEATH(obj.setField("", 5));  // empty names not permitted

    obj.setField("foo", std::numeric_limits<int64_t>::min());
    string s = obj.toString();
    EXPECT_EQ("{\"foo\": -9223372036854775808}", obj.toString());

    obj.clear();
    obj.setField("bar", "hello");
    s = obj.toString();
    EXPECT_EQ("{\"bar\": \"hello\"}", obj.toString());
}

TEST(JSONList, Simple) {
    JSONList list;
    EXPECT_EQ("[]", list.toString());

    list.push_back(std::numeric_limits<int64_t>::max());
    EXPECT_EQ("[9223372036854775807]", list.toString());
    list.push_back("foo");
    EXPECT_EQ("[9223372036854775807, \"foo\"]", list.toString());
   
    list.clear();
    JSONObject obj;
    list.push_back(obj);
    EXPECT_EQ("[{}]", list.toString());

    list.push_back(list);
    EXPECT_EQ("[{}, [{}]]", list.toString());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
