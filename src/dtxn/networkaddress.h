// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef HSTORE_NETWORKADDRESS_H
#define HSTORE_NETWORKADDRESS_H

#include <stdint.h>

#include <string>

struct sockaddr_in;

// Stores the network address of a replica.
class NetworkAddress {
public:
    NetworkAddress() : ip_address_(0), port_(0) {}

    // Returns true if the address is parsed successfully.
    bool parse(const std::string& address);

    bool operator==(const NetworkAddress& other) const {
        return ip_address_ == other.ip_address_ && port_ == other.port_;
    }

    bool operator==(const sockaddr_in& other) const;

    /** Returns IP:port as a string. */
    std::string toString() const;

    /** Returns the IP address formatted as a string. */
    std::string ipToString() const;

    // Fills the addr structure with this address.
    void fill(struct sockaddr_in* addr) const;

    // Returns a sockaddr_in for this address. ::fill() can be more efficient.
    struct sockaddr_in sockaddr() const;

    // Returns the port in host byte order.
    uint16_t port() const;

private:
    // IPv4 address in network byte order
    uint32_t ip_address_;

    // Port in network byte order.
    uint16_t port_;
};

// Make operator== for sockaddr_in bidirectional
inline bool operator==(const sockaddr_in& a, const NetworkAddress& b) {
    return b == a;
}
#endif
