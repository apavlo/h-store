// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef DTXN_SLIDINGARRAY_H__
#define DTXN_SLIDINGARRAY_H__

#include <cassert>

#include "base/circularbuffer.h"

namespace base {

/** Provides an array where items are added to the end and removed from the beginning. This is
like a circular buffer where the indicies constantly increase.
TODO: What about overflow? Do we care? */
template <typename T>
class SlidingArray {
public:
    SlidingArray() : first_index_(0) {}

    /** Returns the index of the first item in the array. */
    size_t firstIndex() const {
        return first_index_;
    }

    /** Returns the index that the next item will be placed at. */
    size_t nextIndex() const {
        return first_index_ + queue_.size();
    }

    /** Adds value to the end of the array, returning its index. */
    size_t push_back(const T& value) {
        size_t index = nextIndex();
        queue_.push_back(value);
        return index;
    }

    /** Adds value at index, assuming index >= nextIndex(). Missing elements will be default
    constructed. */
    void insert(size_t index, const T& value) {
        assert(index >= nextIndex());
        if (size() == 0) {
            // no elements: just make this the first element
            first_index_ = index;
        } else {
            // TODO: Implement CircularBuffer.reserve to make this more efficient
            size_t extras = index - nextIndex();
            // C++-ism to zero-initialize POD types: may do bad things for class types
            T default_value = T();  
            while (extras > 0) {
                push_back(default_value);
                extras -= 1;
            }
        }
        assert(nextIndex() == index);
        push_back(value);
    }

    /** Removes all elements from the array. */
    void clear() {
        first_index_ = nextIndex();
        queue_.clear();
    }

    /** Removes the first item from the array. */
    void pop_front() {
        first_index_ += 1;
        queue_.pop_front();
    }

    T& front() { return queue_.front(); }
    const T& front() const { return const_cast<SlidingArray*>(this)->front(); }

    T& back() { return queue_.back(); }

    T dequeue() {
        T result = front();
        pop_front();
        return result;
    }

    size_t size() const { return queue_.size(); }
    bool empty() const { return queue_.empty(); }

    T& at(size_t index) {
        assert(first_index_ <= index && index < nextIndex());
        return queue_.at(index - first_index_);
    }
    const T& at(size_t index) const { return const_cast<SlidingArray*>(this)->at(index); }

    typedef T value_type;
    typedef base::ArrayContainerIterator<SlidingArray, T> iterator;
    typedef base::ArrayContainerIterator<const SlidingArray, const T> const_iterator;

    /** Returns an iterator at the first element in the queue. Note: this is less efficient than
    using at(). */
    iterator begin() {
        return iterator(this, firstIndex());
    }
    const_iterator begin() const { return const_iterator(this, firstIndex()); }

    /** Returns an iterator one past the end of the queue. Note: this is less efficient than
    using at(). */
    // TODO: Since end() is used as the end of a loop condition, this needs to be optimized.
    iterator end() {
        return iterator(this, nextIndex());
    }
    const_iterator end() const { return const_iterator(this, nextIndex()); }

private:
    size_t first_index_;
    CircularBuffer<T> queue_;
};

}  // namespace base

#endif
