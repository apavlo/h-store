// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BUFFER_H__
#define BUFFER_H__

#include <vector>

#include "io/io.h"

class ReadBuffer {
public:
    ReadBuffer();
    ~ReadBuffer();

    // Returns true if there is more data. Returns false if the connection is closed.
    bool readAll(io::InputStream* connection);

    int available() const { return available_; }

    int read(char* buffer, size_t length);

    // Discard all the data in the buffer.
    void clear();

    // read 16 kB blocks at a time
    static const int BLOCK_SIZE = 1 << 14;

private:
    enum ReadStatus {
        // Read ok, there is no more data
        READ_OK_NO_MORE,
        // Read ok, there might be more data
        READ_OK_MAYBE_MORE,
        // Connection is closed/file is at EOF
        READ_DONE,
    };

    ReadStatus readOnce(io::InputStream* connection);

    struct Block {
        Block() : filled(0) {}

        int filled;
        char data[BLOCK_SIZE];
    };

    // TODO: This should be changed to use a circular array buffer
    // TODO: Does it help if socket buffers are not length prefixed?
    std::vector<Block*> blocks_;
    int read_position_;
    int available_;
};

#endif
