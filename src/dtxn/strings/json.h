// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef STRINGS_JSON_H__
#define STRINGS_JSON_H__

#include <string>
#include <tr1/unordered_map>
#include <stdint.h>

namespace strings {

// Returns a string with the JSON special characters escaped.
// JSON requires the following characters to be escaped in strings:
// quotation mark, revrse solidus, and U+0000 through U+001F.
// http://www.ietf.org/rfc/rfc4627.txt
std::string jsonEscape(const std::string& str);

class JSONList;
class JSONObject;

// Returns a string with the JSON encoding of value.
std::string jsonEncode(int64_t value);
std::string jsonEncode(const std::string& value);
std::string jsonEncode(const JSONList& value);
std::string jsonEncode(const JSONObject& value);

// Builds a JSON object.
class JSONObject {
public:
    template <typename T>
    void setField(const std::string& name, const T& value) {
        internalSetField(name, jsonEncode(value));
    }

    std::string toString() const;
    void clear() { fields_.clear(); }

private:
    void internalSetField(const std::string& name, const std::string& json_value);

    typedef std::tr1::unordered_map<std::string, std::string> FieldMap;
    FieldMap fields_;
};

// Builds a JSON list.
class JSONList {
public:
    JSONList() { clear(); }

    template <typename T>
    void push_back(const T& value) {
        internal_push_back(jsonEncode(value));
    }

    std::string toString() const;
    void clear() { list_ = "["; }

private:
    void internal_push_back(const std::string& value);

    std::string list_;
};

}  // namespace string
#endif
