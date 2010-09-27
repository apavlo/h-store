// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cstdlib>

#include "io/memoryinputstream.h"
#include "messageconnection.h"
#include "mockmessageconnection.h"
#include "stupidunit/stupidunit.h"

using std::string;

class MessageConnectionParserTest : public Test {
public:
    MessageConnectionParserTest() : type_(0) {}

    bool tryParse(const char* data, int size) {
        io::MemoryInputStream stream(data, size);
        buffer_.readAll(&stream);

        return parser_.parseMessage(&buffer_, &type_, &message_);
    }

protected:
    ReadBuffer buffer_;
    MessageConnectionParser parser_;
    int32_t type_;
    string message_;
};

TEST_F(MessageConnectionParserTest, NotEnoughData) {
    // No data: no parse
    bool parsed = parser_.parseMessage(&buffer_, &type_, &message_);
    EXPECT_EQ(false, parsed);

    // Not enough to read the type
    static const char data[] = { 0, 0, 0 };
    parsed = tryParse(data, sizeof(data));
    EXPECT_EQ(false, parsed);

    // Read the type and part of the length
    parsed = tryParse(data, sizeof(data));
    EXPECT_EQ(false, parsed);

    // Enough to read the length, not enough data
    static const char data2[] = { 0, 1, 0, 0, 0, 0, 0, 0, 0, 0 };
    parsed = tryParse(data2, sizeof(data2));
    EXPECT_EQ(false, parsed);
}

TEST_F(MessageConnectionParserTest, BadLength) {
    // Parse the type correctly
    type_ = 42;
    bool result = tryParse(reinterpret_cast<char*>(&type_), sizeof(type_));
    EXPECT_EQ(false, result);

    // followed the type with bad lengths
    int zero = 0;
    EXPECT_DEATH(tryParse(reinterpret_cast<char*>(&zero), sizeof(zero)));
    int negative = -1;
    EXPECT_DEATH(tryParse(reinterpret_cast<char*>(&negative), sizeof(negative)));
}

TEST_F(MessageConnectionParserTest, Simple) {
    static const char data[] = { 42, 0, 0, 0, 1, 0, 0, 0, 0x42 };
    bool parsed = tryParse(data, sizeof(data));
    EXPECT_EQ(true, parsed);
    EXPECT_EQ(42, type_);
    EXPECT_EQ(1, message_.size());
    EXPECT_EQ(0x42, message_[0]);
}

TEST_F(MessageConnectionParserTest, Multiple) {
    static const char data[] = { 42, 0, 0, 0,  1, 0, 0, 0, 0x42, 2, 0, 0, 0, 4, 0, 0, 0, 1, 2, 3, 4 };
    bool parsed = tryParse(data, sizeof(data));
    EXPECT_EQ(true, parsed);
    EXPECT_EQ(42, type_);
    EXPECT_EQ(1, message_.size());
    EXPECT_EQ(0x42, message_[0]);

    message_.clear();
    parsed = parser_.parseMessage(&buffer_, &type_, &message_);
    EXPECT_EQ(true, parsed);
    EXPECT_EQ(2, type_);
    EXPECT_EQ(4, message_.size());
    EXPECT_EQ(1, message_[0]);
    EXPECT_EQ(2, message_[1]);
    EXPECT_EQ(3, message_[2]);
    EXPECT_EQ(4, message_[3]);
}

TEST_F(MessageConnectionParserTest, Reset) {
    static const char data[] = { 1, 0, 0, 0, 10, 0, 0, 0 };
    bool parsed = tryParse(data, sizeof(data));
    EXPECT_EQ(false, parsed);

    parser_.reset();
    static const char data2[] = { 42, 0, 0, 0, 1, 0, 0, 0, 0x42 };
    parsed = tryParse(data2, sizeof(data2));
    EXPECT_EQ(true, parsed);
    EXPECT_EQ(42, type_);
    EXPECT_EQ(1, message_.size());
    EXPECT_EQ(0x42, message_[0]);
}

TEST(MessageConnectionTest, SetState) {
    MockMessageConnection connection;
    EXPECT_EQ(NULL, connection.state());
    connection.state(this);
    EXPECT_EQ(this, connection.state());
}

class ExampleMessage {
public:
    void serialize(io::FIFOBuffer* out) const {}

    static int32_t typeCode() { return 0; }
};

TEST(MessageConnectionTest, BufferedSend) {
    MockMessageConnection connection;
    ExampleMessage message;
    EXPECT_TRUE(connection.send(message));
    EXPECT_TRUE(connection.hasMessage());

    connection.message_.clear();
    connection.bufferedSend(message);
    EXPECT_FALSE(connection.hasMessage());

    connection.flush();
    EXPECT_TRUE(connection.hasMessage());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
