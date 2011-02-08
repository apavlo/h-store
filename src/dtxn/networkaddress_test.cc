// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "networkaddress.h"

#include <netinet/in.h>
#include <sys/socket.h>

#include "stupidunit/stupidunit.h"

using std::string;
using std::vector;

class NetworkAddressTest : public Test {
public:
    NetworkAddress address_;
};

TEST_F(NetworkAddressTest, ParseSpace) {
    bool result = address_.parse("127.0.0.1 12345");
    EXPECT_EQ(true, result);
    EXPECT_EQ(12345, address_.port());
    EXPECT_EQ("127.0.0.1:12345", address_.toString());
    EXPECT_EQ("127.0.0.1", address_.ipToString());
}

TEST_F(NetworkAddressTest, ParseColon) {
    bool result = address_.parse("127.0.0.1:12345");
    EXPECT_EQ(true, result);
    EXPECT_EQ(12345, address_.port());
    EXPECT_EQ("127.0.0.1:12345", address_.toString());
}

// disable -Wconversion to work around a bug in htons in glibc
// TODO: Remove this eventually
#if defined(__OPTIMIZE__) && __GNUC__ == 4 && __GNUC_MINOR__ >= 3
#pragma GCC diagnostic ignored "-Wconversion"
#endif
TEST_F(NetworkAddressTest, Sockaddr) {
    bool result = address_.parse("127.0.0.1:12345");
    ASSERT_EQ(true, result);

    sockaddr_in foo = address_.sockaddr();
    EXPECT_EQ(htonl(INADDR_LOOPBACK), foo.sin_addr.s_addr);
    EXPECT_EQ(htons(12345), foo.sin_port);

    EXPECT_TRUE(address_ == foo);
    foo.sin_family = AF_INET6;
    EXPECT_FALSE(address_ == foo);
    foo.sin_family = AF_INET;
    foo.sin_port = htons(42);
    EXPECT_FALSE(address_ == foo);
    foo.sin_port = htons(12345);
    foo.sin_addr.s_addr = htonl(0x01020304);
    EXPECT_FALSE(address_ == foo);
    foo.sin_addr.s_addr = htonl(0x7f000001);
    EXPECT_TRUE(foo == address_);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
