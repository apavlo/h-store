// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_CHUNKEDARRAY_H__
#define BASE_CHUNKEDARRAY_H__

#include <cassert>
#include <memory>
#include <vector>

namespace base {

/** A list implementation composed of chunks. Efficient for inserts of an unknown number of
elements. */
template <typename T>
class ChunkedArray {
public:
    ChunkedArray() : size_(0) {}

    ~ChunkedArray() {
        clear();
    }

    /** Removes all the elements. */
    void clear() {
        for (int i = 0; i < blocks_.size(); ++i) {
            // Destruct each element in the memory block
            size_t block_element_count = BLOCK_ELEMENTS;
            if (i == blocks_.size()-1) {
                // Special handling for the last block
                block_element_count = size_ % BLOCK_ELEMENTS;
                if (block_element_count == 0) {
                    block_element_count = BLOCK_ELEMENTS;
                }
            }
            for (int j = 0; j < block_element_count; ++j) {
                blocks_[i][j].~T();
            }

            // deallocate the block
            allocator_.deallocate(blocks_[i], BLOCK_ELEMENTS);
        }

        size_ = 0;
        blocks_.clear();
    }

    void push_back(const T& value) {
        // Calculate the next append index
        size_t next_append = size_ % BLOCK_ELEMENTS;
        if (next_append == 0) {
            blocks_.push_back(allocator_.allocate(BLOCK_ELEMENTS));
        }
        assert(next_append < BLOCK_ELEMENTS);

        // Use placement new to copy construct the element
        new (blocks_.back() + next_append) T(value);
        size_ += 1;
    }

    const T& at(size_t index) const {
        assert(index < size_);
        size_t block = index / BLOCK_ELEMENTS;
        size_t block_index = index % BLOCK_ELEMENTS;
        assert(block < blocks_.size());
        return blocks_[block][block_index];
    }

    /** Returns the number of elements stored in this buffer. */
    size_t size() const { return size_; }

    /** Returns true if this buffer is empty. */
    bool empty() const { return size_ == 0; }

    static const size_t BLOCK_BYTES = 4096;
    static const size_t BLOCK_ELEMENTS = BLOCK_BYTES / sizeof(T);

private:
    size_t size_;
    std::vector<T*> blocks_;

    // TODO: Make this a template parameter?
    std::allocator<T> allocator_;

    // No copy constructor or operator=
    ChunkedArray(const ChunkedArray& other);
    ChunkedArray& operator=(const ChunkedArray& other);
};

}  // namespace base

#endif
