// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "io/buffer.h"

#include <cstring>

#include <stdint.h>

#include "base/assert.h"
#include "base/cast.h"
#include "base/stlutil.h"

using std::vector;

namespace io {

static const int PAGE_SIZE = 4096;
struct Page {
    Page() : filled(0) {}

    // Number of bytes of data that have actually been filled
    int filled;
    char data[PAGE_SIZE - sizeof(int)];
};

MemoryOutputStream::MemoryOutputStream() : read_position_(0), available_(0) {
    pages_.push_back(new Page());
}

MemoryOutputStream::~MemoryOutputStream() {
    STLDeleteElements(&pages_);
}

int MemoryOutputStream::write(const char* buffer, size_t length) {
    int remaining_bytes = assert_range_cast<int>(length);

    while (remaining_bytes > 0) {
        // Figure out the space remaining on this page
        Page* current = pages_.back();
        int to_copy = static_cast<int>(sizeof(current->data)) - current->filled;
        assert(to_copy >= 0);
        if (to_copy == 0) {
            // The page is full: add a new one
            current = new Page();
            pages_.push_back(current);
            to_copy = sizeof(current->data);
        }

        // Copy the data into the page
        to_copy = std::min(to_copy, remaining_bytes);
        char* page_end = current->data + current->filled;
        memcpy(page_end, buffer, to_copy);

        // Adjust all the counters
        buffer += to_copy;
        current->filled += to_copy;
        remaining_bytes -= to_copy;
        available_ += to_copy;
        assert(current->filled <= sizeof(current->data));
        assert(remaining_bytes >= 0);
    }
    assert(remaining_bytes == 0);

    return assert_range_cast<int>(length);
}

int MemoryOutputStream::flush(OutputStream* out) {
    int bytes_copied = 0;

    while (available_ > 0) {
        // Try to write everything in the first block
        int remaining_in_block = pages_.front()->filled - read_position_;
        int bytes_written = out->write(pages_.front()->data + read_position_, remaining_in_block);
        assert(bytes_written <= remaining_in_block);
        if (bytes_written == -1) {
            // An error occurred while writing: return the error
            return -1;
        }
        assert(bytes_written >= 0);

        // Adjust the byte counters
        read_position_ += bytes_written;
        assert(read_position_ <= pages_.front()->filled);
        bytes_copied += bytes_written;
        available_ -= bytes_written;
        assert(available_ >= 0);

        if (bytes_written < remaining_in_block) {
            // The other output stream is full: stop writing
            break;
        }
        assert(bytes_written == remaining_in_block);

        if (read_position_ == pages_.front()->filled) {
            // Consumed an entire block: remove it
            if (pages_.size() > 1) {
                delete pages_.front();
                pages_.erase(pages_.begin());
            } else {
                // Last block: just reset the counter
                assert(pages_.size() == 1);
                pages_.front()->filled = 0;
                assert(available_ == 0);
            }
            read_position_ = 0;
        }
    }

    return bytes_copied;
}


struct FIFOBuffer::Page {
    int write_position_;
    Page* next_;
    uint8_t data_[0];
};

const int FIFOBuffer::PAGE_DATA_SIZE = PAGE_SIZE - sizeof(FIFOBuffer::Page);

FIFOBuffer::FIFOBuffer() : read_position_(0), available_(0) {
    Page* p = allocatePage();
    head_ = p;
    tail_ = p;
}

FIFOBuffer::~FIFOBuffer() {
    while (head_ != NULL) {
        popPage();
    }
}

void FIFOBuffer::writeBuffer(void** out_buffer, int* out_length) {
    if (head_ == tail_ && read_position_ == tail_->write_position_) {
        // last read consumed all the data: reset the read and write positions
        read_position_ = 0;
        head_->write_position_ = 0;
    }

    int remaining_bytes = PAGE_DATA_SIZE - tail_->write_position_;
    if (remaining_bytes == 0) {
        Page* p = allocatePage();
        tail_->next_ = p;
        tail_ = p;
        remaining_bytes = PAGE_DATA_SIZE;
    }

    assert(remaining_bytes > 0);
    *out_buffer = tail_->data_ + tail_->write_position_;
    *out_length = remaining_bytes;
    tail_->write_position_ = PAGE_DATA_SIZE;
    available_ += remaining_bytes;
}

void FIFOBuffer::undoWrite(int length) {
    assert(0 <= length && length <= tail_->write_position_);
    tail_->write_position_ -= length;
    assert(0 <= tail_->write_position_ && tail_->write_position_ <= PAGE_DATA_SIZE);
    available_ -= length;
    assert(available_ >= 0);
}

void FIFOBuffer::copyIn(const void* data, int length) {
    assert(length >= 0);

    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(data);
    while (length > 0) {
        void* write;
        int remaining;
        writeBuffer(&write, &remaining);

        int written = std::min(length, remaining);
        memcpy(write, bytes, written);
        length -= written;
        bytes += written;

        // If we didn't fill the write buffer, undo the last bit
        if (written < remaining) {
            assert(length == 0);
            undoWrite(remaining - written);
        }
    }
}

int FIFOBuffer::readAllAvailable(io::InputStream* input) {
    bool first = true;
    while (true) {
        void* data;
        int length;
        writeBuffer(&data, &length);
        int bytes = input->read(reinterpret_cast<char*>(data), length);
        if (bytes < 0) {
            // Error (connection closed I hope)
            undoWrite(length);

            // If this was the first read attempt, return -1 == closed
            // otherwise we return the number of bytes available.
            // This makes it easy to consume the end of the stream
            if (first) return -1;
            break;
        } else if (bytes < length) {
            // Incomplete read: put back some of the buffer space
            assert(0 <= bytes && bytes < length);
            assert(length - bytes > 0);
            undoWrite(length - bytes);
            break;
        } else {
            // Complete read
            assert(bytes == length);
            first = false;
        }
    }

    return available_;
}

int FIFOBuffer::writeAvailable(io::OutputStream* output) {
    const void* read;
    int length;
    readBuffer(&read, &length);

    while (length > 0) {
        int written = output->write(reinterpret_cast<const char*>(read), length);
        if (written == -1) {
            return -1;
        } else if (written < length) {
            assert(written >= 0);
            // blocked: undo the read and come back later
            undoRead(length - written);
            return 1;
        }
        // We wrote the entire block: write more, if more is available
        assert(written == length);
        readBuffer(&read, &length);
    }

    assert(length == 0);
    return 0;
}

void* FIFOBuffer::writeExact(int length) {
    assert(length > 0);

    int remaining_bytes = PAGE_DATA_SIZE - tail_->write_position_;
    if (remaining_bytes < length) {
        Page* p = allocatePage();
        tail_->next_ = p;
        tail_ = p;
        remaining_bytes = PAGE_DATA_SIZE;
    }

    assert(remaining_bytes >= length);
    void* out_buffer = tail_->data_ + tail_->write_position_;
    tail_->write_position_ += length;
    available_ += length;
    return out_buffer;
}

void FIFOBuffer::readBuffer(const void** out_buffer, int* out_length) {
    // If the current page is empty, reset or discard it
    if (read_position_ == head_->write_position_) {
        read_position_ = 0;
        if (head_ != tail_) {
            // more than one page: discard this page
            popPage();
        } else {
            // reset the write position: empty!
            head_->write_position_ = 0;
        }
    }

    int remaining_bytes = head_->write_position_ - read_position_;
    assert(0 <= remaining_bytes && remaining_bytes <= available_);
    *out_length = remaining_bytes;
    if (remaining_bytes == 0) {
        *out_buffer = NULL;
    } else {
        *out_buffer = head_->data_ + read_position_;
    }
    read_position_ += remaining_bytes;
    available_ -= remaining_bytes;
}

void FIFOBuffer::undoRead(int length) {
    assert(0 <= length && length <= read_position_);
    read_position_ -= length;
    assert(0 <= read_position_ && read_position_ <= PAGE_DATA_SIZE);
    available_ += length;
}

void FIFOBuffer::copyOut(void* out, int length) {
    CHECK(0 < length && length <= available_);

    int read_length;
    while (length > 0) {
        // Read a chunk
        const void* read;
        readBuffer(&read, &read_length);

        // Copy the chunk
        int copy_length = std::min(length, read_length);
        memcpy(out, read, copy_length);
        out = reinterpret_cast<uint8_t*>(out) + copy_length;
        length -= copy_length;
        assert(length >= 0);
        read_length -= copy_length;
        assert(read_length == 0 || (read_length > 0 && length == 0));
    }

    if (read_length > 0) {
        // Unread the stuff we didn't copy
        undoRead(read_length);
    }
}

void FIFOBuffer::clear() {
    // If we have more than one page, discard them
    while (head_ != tail_) {
        popPage();
    }
    assert(head_ == tail_);
    read_position_ = 0;
    head_->write_position_ = 0;
    available_ = 0;
}

FIFOBuffer::Page* FIFOBuffer::allocatePage() const {
    // Use raw ::operator new and ::operator delete to allocate uninitialized memory
    // TODO: Use std::allocator?
    Page* page = (Page*) ::operator new(PAGE_SIZE);
    page->write_position_ = 0;
    page->next_ = NULL;
    return page;
}

void FIFOBuffer::popPage() {
    Page* previous = head_;
    head_ = head_->next_;
    ::operator delete(previous);
}

}
