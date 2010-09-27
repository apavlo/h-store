// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <cstdlib>
#include <stdint.h>

#include "strings/utils.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

using namespace strings;

TEST(SplitIncluding, Simple) {
    vector<string> out = strings::splitIncluding("hello;world;;", ';');
    ASSERT_EQ(3, out.size());
    EXPECT_EQ("hello;", out[0]);
    EXPECT_EQ("world;", out[1]);
    EXPECT_EQ(";", out[2]);
}

TEST(SplitIncluding, MissingTrailing) {
    vector<string> out = strings::splitIncluding("hello;world", ';');
    ASSERT_EQ(2, out.size());
    EXPECT_EQ("hello;", out[0]);
    EXPECT_EQ("world", out[1]);
}

TEST(SplitExcluding, Simple) {
    vector<string> out = strings::splitExcluding("hello;world;;", ';');
    ASSERT_EQ(4, out.size());
    EXPECT_EQ("hello", out[0]);
    EXPECT_EQ("world", out[1]);
    EXPECT_EQ("", out[2]);
    EXPECT_EQ("", out[2]);
}

TEST(SplitExcluding, MissingTrailing) {
    vector<string> out = strings::splitExcluding("hello;world", ';');
    ASSERT_EQ(2, out.size());
    EXPECT_EQ("hello", out[0]);
    EXPECT_EQ("world", out[1]);
}

// Used to call the hash method on a constant object.
size_t compute(const Hash& ref, const string& value) {
    return ref(value);
}

TEST(Hash, Simple) {
    Hash foo;
    size_t value = compute(foo, "hello");
    EXPECT_NE(value, foo("world"));
}

TEST(EndsWith, Simple) {
    EXPECT_TRUE(strings::endsWith("hello world", "rld"));
    EXPECT_TRUE(strings::endsWith("hello world", "rld"));
    EXPECT_FALSE(strings::endsWith("hello world", "foo"));
    EXPECT_FALSE(strings::endsWith("world", "hello world"));
    EXPECT_TRUE(strings::endsWith("world", ""));
    EXPECT_TRUE(strings::endsWith("", ""));
}

TEST(ReadFile, FileDoesNotExist) {
    EXPECT_DEATH(strings::readFile("fileDoesNotExist......"));
}

TEST(ReadFile, Simple) {
    // Create a temporary file and read it back in
    stupidunit::ChTempDir temp_dir;
    int fh = open("out", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    ASSERT_GE(fh, 0);
    string data = "hello world\n";
    ssize_t bytes = write(fh, data.c_str(), data.size());
    ASSERT_EQ(data.size(), bytes);
    int error = close(fh);
    ASSERT_EQ(0, error);

    string in = strings::readFile("out");
    EXPECT_EQ(data, in);
}

TEST(ReplaceAll, Simple) {
    // This example could lead to an infinite loop in naive implementations
    string target("hellohello");
    strings::replaceAll(&target, "llo", "llohello");
    EXPECT_EQ("hellohellohellohello", target);
}

class AssignBytesTest : public Test {
public:
    template <typename T>
    void testAssign(T value) {
        string out;
        strings::assignBytes(&out, value);
        EXPECT_EQ(sizeof(value), out.size());
        
        T copy;
        assignBytes(&copy, out);
        EXPECT_EQ(value, copy);
    }
};

TEST_F(AssignBytesTest, Simple) {
    testAssign<int8_t>(-128);
    testAssign<uint32_t>(0xbaba);
}

TEST(CEscape, Simple) {
    EXPECT_EQ("foo", cEscape("foo"));
    static const char SPECIALS[] = "\a\b\f\n\r\t\v\\";
    static const string SPECIALS_s(SPECIALS, sizeof(SPECIALS));

    EXPECT_EQ("\\a\\b\\f\\n\\r\\t\\v\\\\\\0", cEscape(SPECIALS_s));

    static const char NON_PRINTABLE[] = "\x01\x02\x03\xff";
    EXPECT_EQ("\\x01\\x02\\x03\\xff", cEscape(NON_PRINTABLE));
}

TEST(StringPrintf, Simple) {
    // Expected compile error:
    // EXPECT_EQ("", StringPrintf(""));
    EXPECT_EQ("", StringPrintf("%s", ""));
    EXPECT_EQ("", StringPrintf("%.*s", 0, "hello"));

    EXPECT_EQ("literal", StringPrintf("literal"));
    EXPECT_EQ("foo123456foo", StringPrintf("foo%dfoo", 123456));

    // Test a string that overflows the initial buffer
    string huge = "hello";
    while (huge.size() < 1024) {
        huge += huge;
    }
    EXPECT_EQ(huge + huge, StringPrintf("%s%s", huge.c_str(), huge.c_str()));

    // Test a string that is exactly the size of the initial buffer. This needs 2 passes because
    // of how StringPrintf does null termination
    huge = "";
    for (int i = 0; i < 1024; ++i) {
        huge += "a";
    }
    EXPECT_EQ(huge, StringPrintf("%s", huge.c_str()));
}

TEST(LineReader, FileDoesNotExist) {
    EXPECT_DEATH(strings::LineReader("fileDoesNotExist......"));
}

TEST(LineReader, Simple) {
    // Create a temporary file and read it back in
    stupidunit::ChTempDir temp_dir;
    int fh = open("out", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    ASSERT_GE(fh, 0);
    static const char DATA[] = "hello world\n\nline 3";
    ssize_t bytes = write(fh, DATA, sizeof(DATA));
    ASSERT_EQ(sizeof(DATA), bytes);
    int error = close(fh);
    ASSERT_EQ(0, error);

    LineReader reader("out");
    EXPECT_TRUE(reader.hasValue());
    EXPECT_EQ("hello world", reader.value());
    EXPECT_EQ("hello world", reader.value());
    reader.next();
    EXPECT_TRUE(reader.hasValue());
    EXPECT_EQ("", reader.value());
    reader.next();
    EXPECT_TRUE(reader.hasValue());
    EXPECT_EQ("line 3", reader.value());
    reader.next();
    EXPECT_FALSE(reader.hasValue());
    EXPECT_DEATH(reader.value());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
