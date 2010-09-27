// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "strings/utils.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "base/assert.h"
#include "base/stringutil.h"

using std::string;
using std::vector;

namespace strings {

vector<string> splitIncluding(const string& input, char split) {
    vector<string> splits;

    size_t last = 0;
    while (last < input.size()) {
        size_t next = input.find(split, last);
        if (next == string::npos) {
            next = input.size();
        }

        // Push the substring [last, next) on to splits
        splits.push_back(input.substr(last, next - last + 1));
        last = next+1;
    }

    return splits;
}

vector<string> splitExcluding(const string& input, char split) {
    vector<string> splits;

    size_t last = 0;
    while (last <= input.size()) {
        size_t next = input.find(split, last);
        if (next == string::npos) {
            next = input.size();
        }

        // Push the substring [last, next) on to splits
        splits.push_back(input.substr(last, next - last));
        last = next+1;
    }

    return splits;
}

bool endsWith(const string& input, const string& end) {
    ssize_t start = input.size() - end.size();
    // Can't be true if the ending is larger than the input
    if (start < 0) return false;

    return input.compare(start, end.size(), end) == 0;
}

string readFile(const char* path) {
    int handle = open(path, O_RDONLY);
    assert(handle >= 0);

    struct stat info;
    ssize_t error = fstat(handle, &info);
    assert(error == 0);

    string data(info.st_size, '\0');
    error = read(handle, base::stringArray(&data), data.size());
    
    assert(error == data.size());
    error = close(handle);
    assert(error == 0);

    return data;
}

void replaceAll(string* target, const string& find, const string& replace) {
    size_t index = target->find(find);
    while (index != string::npos) {
        target->replace(index, find.size(), replace);
        index = target->find(find, index + replace.size());
    }
}

string cEscape(const string& s) {
    static const char PRINTABLE_SPECIALS[] = { '\"', '\\', };
    static const char NON_PRINTABLE_SPECIALS[] = {
            '\a', '\b', '\f', '\n', '\r', '\t', '\v', '\0',
    };
    static const char NON_PRINTABLE_ESCAPES[sizeof(NON_PRINTABLE_SPECIALS)] = {
            'a', 'b', 'f', 'n', 'r', 't', 'v', '0',
    };

    static const int FIRST_PRINTABLE = 32;  // space
    static const int LAST_PRINTABLE = 126;  // ~

    string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (FIRST_PRINTABLE <= s[i] && s[i] <= LAST_PRINTABLE) {
            // Printable escapes just need a backslash in front
            for (size_t j = 0; j < sizeof(PRINTABLE_SPECIALS); ++j) {
                if (s[i] == PRINTABLE_SPECIALS[j]) {
                    out.push_back('\\');
                    break;
                }
            }

            out.push_back(s[i]);
        } else {
            // Non-printable escapes either must be hex or something else
            bool found = false;
            for (size_t j = 0; j < sizeof(NON_PRINTABLE_SPECIALS); ++j) {
                if (s[i] == NON_PRINTABLE_SPECIALS[j]) {
                    found = true;
                    out.push_back('\\');
                    out.push_back(NON_PRINTABLE_ESCAPES[j]);
                    break;
                }
            }

            if (!found) {
                // hex escape!
                char buffer[5];
                ssize_t length = snprintf(buffer, sizeof(buffer), "\\x%02hhx", s[i]);
                ASSERT(length == 4);
                out.append(buffer);
            }
        }
    }

    return out;
}

string StringPrintf(const char* format, ...) {
    string result;

    // guess the initial buffer size
    ssize_t required_bytes = 1023;
    do {
        result.resize(required_bytes + 1);
        va_list args;
        va_start(args, format);
        required_bytes = vsnprintf(base::stringArray(&result), result.size(),
                format, args);
        va_end(args);

        // if the initial buffer is too small, do it again
    } while (required_bytes >= result.size());

    assert(required_bytes < result.size());
    result.resize(required_bytes);
    return result;
}

LineReader::LineReader(const char* path) : available_(0), next_(0) {
    file_descriptor_ = open(path, O_RDONLY);
    assert(file_descriptor_ >= 0);
    has_value_ = readLine();
}

LineReader::~LineReader() {
    int error = close(file_descriptor_);
    ASSERT(!error);
}

bool LineReader::readLine() {
    value_.clear();
    while (true) {
        // Read the next chunk of the file
        if (next_ == available_) {
            ssize_t bytes = read(file_descriptor_, buffer_, sizeof(buffer_));
            assert(bytes >= 0);
            available_ = (int) bytes;
            next_ = 0;

            if (available_ == 0) {
                // end of the file: Did we read anything?
                return !value_.empty();
            }
        }

        int last = next_;

        // scan for \n
        while (next_ < available_) {
            if (buffer_[next_] == '\n') {
                break;
            }
            next_ += 1;
        }

        // copy from last to next into the string
        value_.append(buffer_, last, next_ - last);
        if (next_ < available_) {
            assert(buffer_[next_] == '\n');
            next_ += 1;
            return true;
        }
    }
}

}  // namespace strings
