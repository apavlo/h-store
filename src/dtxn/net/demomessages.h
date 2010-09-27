// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

/* AUTOMATICALLY GENERATED: DO NOT EDIT */
#ifndef NET_DEMOMESSAGES_H
#define NET_DEMOMESSAGES_H

#include <cassert>
#include <string>
#include <vector>

#include "base/assert.h"
#include "io/message.h"
#include "serialization.h"

namespace io {
class FIFOBuffer;
}

namespace net {

// A simple test message.
class Demo : public io::Message {
public:
    Demo() :
        integer(-42),
        boolean(false) {}

    // An integer.
    int32_t integer;

    // A boolean.
    bool boolean;

    // A string.
    std::string str;

    bool operator==(const Demo& other) const {
        if (integer != other.integer) return false;
        if (boolean != other.boolean) return false;
        if (str != other.str) return false;
        return true;
    }
    bool operator!=(const Demo& other) const { return !(*this == other); }

    void appendToString(std::string* _out_) const {
        serialization::serialize(integer, _out_);
        serialization::serialize(boolean, _out_);
        serialization::serialize(str, _out_);
    }

    virtual void serialize(io::FIFOBuffer* _out_) const {
        serialization::serialize(integer, _out_);
        serialization::serialize(boolean, _out_);
        serialization::serialize(str, _out_);
    }

    const char* parseFromString(const char* _start_, const char* _end_) {
        _start_ = serialization::deserialize(&integer, _start_, _end_);
        _start_ = serialization::deserialize(&boolean, _start_, _end_);
        _start_ = serialization::deserialize(&str, _start_, _end_);
        return _start_;
    }

    void parseFromString(const std::string& _str_) {
        const char* end = parseFromString(_str_.data(), _str_.data() + _str_.size());
        ASSERT(end == _str_.data() + _str_.size());
    }

    static int32_t typeCode() { return -1721001198; }
};

}  // namespace net
#endif  // NET_DEMOMESSAGES_H
