// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "base/assert.h"
#include "io/memoryinputstream.h"
#include "io/mockstreams.h"
#include "protorpc/Counter.pb.h"
#include "protorpc/protoconnectionbuffer.h"
#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

using namespace protorpc;

class ProtoConnectionBufferTest : public Test {
public:
    string serialize(int value) {
        message_.set_value(value);

        int32_t length_prefix;
        string out;
        out.resize(sizeof(length_prefix));

        bool success = message_.AppendToString(&out);
        CHECK(success);

        length_prefix = (int32_t)(out.size() - sizeof(length_prefix));
        memcpy(const_cast<char*>(out.data()), &length_prefix, sizeof(length_prefix));

        message_.Clear();
        return out;
    }

    ProtoConnectionBuffer connection_;
    Value message_;
    io::MemoryInputStream in_stream_;
};

/*
TEST_F(ProtoConnectionBufferTest, ReadByteAtATime) {
    string bytes = serialize(42);

    // Feed in zero bytes
    EXPECT_EQ(0, connection_.tryRead(&in_stream_, &message_));

    // Feed in a byte at a time
    for (int i = 0; i < bytes.size(); ++i) {
        in_stream_.setBuffer(bytes.data() + i, 1);
        int result = connection_.tryRead(&in_stream_, &message_);
        EXPECT_EQ(0, in_stream_.available());
        if (i == bytes.size()-1) {
            EXPECT_LT(0, result);
        } else {
            EXPECT_EQ(0, result);
        }
    }

    EXPECT_TRUE(message_.has_value());
    EXPECT_EQ(42, message_.value());
}

TEST_F(ProtoConnectionBufferTest, ReadMultiple) {
    string bytes = serialize(42);
    bytes += serialize(43);
    bytes += serialize(44);
    bytes += (char) 0x21;

    // Feed in everything at once through a limit stream
    in_stream_.setBuffer(bytes.data(), (int) bytes.size());
    EXPECT_LT(0, connection_.tryRead(&in_stream_, &message_));
    EXPECT_EQ(42, message_.value());
    EXPECT_LT(0, connection_.tryRead(&in_stream_, &message_));
    EXPECT_EQ(43, message_.value());
    EXPECT_LT(0, connection_.tryRead(&in_stream_, &message_));
    EXPECT_EQ(44, message_.value());
    EXPECT_EQ(0, connection_.tryRead(&in_stream_, &message_));
}

TEST_F(ProtoConnectionBufferTest, ConnectionBreak) {
    string bytes = serialize(42);

    // Feed in some bytes then break the connection
    in_stream_.setBuffer(bytes.data(), (int) bytes.size() - 2);
    EXPECT_EQ(0, connection_.tryRead(&in_stream_, &message_));

    in_stream_.close();
    EXPECT_EQ(-1, connection_.tryRead(&in_stream_, &message_));
    EXPECT_EQ(0, message_.value());
    EXPECT_FALSE(message_.has_value());
}
*/

class StringOutput : public io::OutputStream {
public:
    virtual int write(const char* buffer, size_t length) {
        buffer_.append(buffer, length);
        return (int) length;
    }

    string buffer_;
};

TEST_F(ProtoConnectionBufferTest, WriteOne) {
    message_.set_value(42);

    connection_.bufferMessage(message_);

    StringOutput out;
    EXPECT_FALSE(connection_.tryWrite(&out));
    int32_t length;
    ASSERT_LE(sizeof(length), out.buffer_.size());
    memcpy(&length, out.buffer_.data(), sizeof(length));

    EXPECT_EQ(length + sizeof(length), out.buffer_.size());
    bool success = message_.ParseFromArray(out.buffer_.data() + sizeof(length), length);
    EXPECT_TRUE(success);
    EXPECT_EQ(42, message_.value());
}

TEST_F(ProtoConnectionBufferTest, WriteBlockedClosed) {
    message_.set_value(42);
    connection_.bufferMessage(message_);

    io::MockOutputStream out;
    out.next_result_ = 2;
    EXPECT_TRUE(connection_.tryWrite(&out));

    out.closed_ = true;
    EXPECT_DEATH(connection_.tryWrite(&out));

    out.closed_ = false;
    out.next_result_ = -1;
    EXPECT_FALSE(connection_.tryWrite(&out));

    // Doesn't actually try to write
    out.closed_ = true;
    EXPECT_FALSE(connection_.tryWrite(&out));
    EXPECT_EQ(2, out.write_count_);
}

int main() {
    int result = TestSuite::globalInstance()->runAll();
    google::protobuf::ShutdownProtobufLibrary();
    return result;
}
