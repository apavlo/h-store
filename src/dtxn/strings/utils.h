// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef STRINGUTILS_H
#define STRINGUTILS_H

#include <cassert>
#include <cstdarg>
#include <cstring>
#include <string>
#include <vector>

namespace strings {

// Computes the DJB hash. Good for English text, not so good for other stuff.
struct Hash {
    size_t operator()(const std::string& value) const {
        size_t hash = 5381;
        for (int i = 0; i < value.size(); ++i) {
            hash = hash*33 + value[i];
        }
        return hash;
    }
};

// Splits the string including the separators.
std::vector<std::string> splitIncluding(const std::string& input, char split);

// Splits the string excluding the separators
std::vector<std::string> splitExcluding(const std::string& input, char split);

// Returns true if input ends with end.
bool endsWith(const std::string& input, const std::string& end);

// Reads the entire file into a string
std::string readFile(const char* path);

// Replaces all instances of find in target with replace.
void replaceAll(std::string* target, const std::string& find, const std::string& replace);

// Assigns the raw bytes from source to destination.
template <typename T>
void assignBytes(std::string* destination, const T& source) {
    destination->assign(reinterpret_cast<const char*>(&source), sizeof(source));
}

template <typename T>
void assignBytes(T* destination, const std::string& source) {
    assert(source.size() == sizeof(*destination));
    memcpy(destination, source.data(), source.size());
}

// Returns a new string with special characters escaped.
std::string cEscape(const std::string& str);

/** Returns a string created with the given format string. */
std::string StringPrintf(const char* format, ...) __attribute__((format(printf, 1, 2)));

/** Provides an STL container interface to lines in a file. */
class LineReader {
public:
    LineReader(const char* path);
    ~LineReader();

    bool hasValue() const { return has_value_; }
    void next() { has_value_ = readLine(); }
    const std::string& value() const { assert(has_value_); return value_; }

private:
    bool readLine();

    std::string value_;
    bool has_value_;

    static const int BUFFER_SIZE = 2048;
    int file_descriptor_;
    char buffer_[BUFFER_SIZE];
    int available_;
    int next_;
};

}  // namespace string
#endif
