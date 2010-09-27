// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "buffer.h"

#include "base/cast.h"

#include <cassert>
#include <cstring>
#include <limits>

using std::vector;

ReadBuffer::ReadBuffer() : read_position_(0), available_(0) {
    blocks_.push_back(new Block());
}

ReadBuffer::~ReadBuffer() {
    for (int i = 0; i < blocks_.size(); ++i) {
        delete blocks_[i];
    }
}

bool ReadBuffer::readAll(io::InputStream* connection) {
    ReadStatus status;
    do {
        status = readOnce(connection);
    } while (status == READ_OK_MAYBE_MORE);

    if (status == READ_DONE) return false;
    assert(status == READ_OK_NO_MORE);
    return true;
}

int ReadBuffer::read(char* buffer, size_t length) {
    int bytes_copied = 0;
    char* current = buffer;

    while (bytes_copied < length && available_ > 0) {
        int remaining_in_block = blocks_.front()->filled - read_position_;
        int to_copy = std::min(assert_range_cast<int>(length) - bytes_copied, remaining_in_block);

        memcpy(current, blocks_.front()->data + read_position_, to_copy);
        current += to_copy;
        read_position_ += to_copy;
        assert(read_position_ <= blocks_.front()->filled);
        bytes_copied += to_copy;
        available_ -= to_copy;
        assert(available_ >= 0);

        if (read_position_ == blocks_.front()->filled) {
            // Consumed an entire block: remove it
            if (blocks_.size() > 1) {
                delete blocks_.front();
                blocks_.erase(blocks_.begin());
            } else {
                // Last block: just reset the counter
                assert(blocks_.size() == 1);
                blocks_.front()->filled = 0;
                assert(available_ == 0);
            }
            read_position_ = 0;
        }
    }

    assert(bytes_copied <= length);
    return bytes_copied;
}

void ReadBuffer::clear() {
    for (int i = 1; i < blocks_.size(); ++i) {
        delete blocks_[i];
    }
    blocks_.resize(1);
    blocks_.front()->filled = 0;
}

ReadBuffer::ReadStatus ReadBuffer::readOnce(io::InputStream* connection) {
    Block* current = blocks_.back();
    size_t remaining = sizeof(current->data) - current->filled;
    if (remaining == 0) {
        current = new Block();
        blocks_.push_back(current);
        remaining = sizeof(current->data);
    }
    char* end = current->data + current->filled;

    int bytes = connection->read(end, remaining);
    if (bytes == 0) {
        return READ_OK_NO_MORE;
    } else if (bytes == -1) {
        return READ_DONE;
    } else {
        assert(0 < bytes && bytes <= remaining);
        current->filled += bytes;
        assert(current->filled <= sizeof(current->data));
        available_ += bytes;

        if (current->filled == sizeof(current->data)) {
            assert(bytes == remaining);
            return READ_OK_MAYBE_MORE;
        } else {
            return READ_OK_NO_MORE;
        }
    }
}
