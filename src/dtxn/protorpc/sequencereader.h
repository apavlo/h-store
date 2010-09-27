// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef PROTORPC_SEQUENCEREADER_H__
#define PROTORPC_SEQUENCEREADER_H__

#include <cassert>
#include <string>

namespace google { namespace protobuf { namespace io {
class FileInputStream;
}}}

namespace protorpc {

// Reads a sequence of bytes from a file.
// TODO: This could be made fairly "high performance" by providing a "zero copy"
// interface, by returning a pointer to the internal buffer.
class SequenceReader {
public:
    // Open file_path and position this reader at the first record.
    SequenceReader(const std::string& file_path);
    ~SequenceReader() {
        if (input_ != NULL) {
            close();
        }
    }

    // Returns true if this reader is positioned at a valid record.
    bool hasValue() {
        return input_ != NULL;
    }

    // Move the reader to the next record, if possible.
    void advance();

    const std::string& current() {
        assert(hasValue());
        return buffer_;
    }

private:
    void close();

    google::protobuf::io::FileInputStream* input_;
    std::string buffer_;
};

}  // namespace protorpc
#endif
