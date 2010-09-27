// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "strings/json.h"

// for PRIli64
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "base/assert.h"

using std::string;

namespace strings {

string jsonEscape(const string& s) {
    static const char ESCAPES[] = { '"', '\\', '\b', '\f', '\n', '\r', '\t' };
    static const char REPLACEMENTS[] = { '"', '\\', 'b', 'f', 'n', 'r', 't' };
    assert(sizeof(ESCAPES) == sizeof(REPLACEMENTS));

    string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if ((0 <= s[i] && s[i] <= 0x1f) || s[i] == '"' || s[i] =='\\') {
            // The character must be replaced with something: look in escapes
            bool replaced = false;
            for (int j = 0; j < sizeof(ESCAPES); ++j) {
                if (ESCAPES[j] == s[i]) {
                    replaced = true;
                    out.push_back('\\');
                    out.push_back(REPLACEMENTS[j]);
                    break;
                }
            }
            if (!replaced) {
                // Use a unicode hex escape sequence
                char hex[7];
                int bytes = snprintf(hex, sizeof(hex), "\\u%04hhx", s[i]);
                ASSERT(bytes == sizeof(hex)-1);
                out .append(hex);
            }
        } else {
            out.push_back(s[i]);
        }
    }
    return out;
}

// Escapes a string and adds quotes around it, appropriate for json output
string jsonEncode(const std::string& value) {
    string escaped_value = jsonEscape(value);
    escaped_value.insert(0, 1, '\"');
    escaped_value.push_back('\"');
    return escaped_value;
}

string jsonEncode(int64_t value) {
    char buffer[21];
    ssize_t length = snprintf(buffer, sizeof(buffer), "%" PRIi64, value);
    assert(length <= sizeof(buffer)-1);
    return string(buffer, length);
}

string jsonEncode(const JSONObject& obj) {
    return obj.toString();
}

string jsonEncode(const JSONList& list) {
    return list.toString();
}

void JSONObject::internalSetField(const string& name, const string& json_value) {
    assert(!name.empty());
    string escaped_name = jsonEncode(name);
    fields_[escaped_name] = json_value;
}

std::string JSONObject::toString() const {
    string out("{");
    for (FieldMap::const_iterator i = fields_.begin(); i != fields_.end(); ++i) {
        if (i != fields_.begin()) {
            out.append(",\n");
        }
        out.append(i->first);
        out.append(": ");
        out.append(i->second);
    }
    out.append("}");
    return out;
}

void JSONList::internal_push_back(const std::string& json_value) {
    if (list_.size() > 1) {
        list_.append(", ");
    }
    list_.append(json_value);
}

std::string JSONList::toString() const {
    return list_ + "]";
}

}  // namespace strings
