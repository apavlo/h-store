// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_CIRCULARBUFFER_H__
#define BASE_CIRCULARBUFFER_H__

#include <cassert>
#include <iterator>
#include <memory>

namespace base {

/** An iterator for array-like containers. */
template <typename ContainerType, typename ValueType>
class ArrayContainerIterator :
        public std::iterator<std::forward_iterator_tag, ValueType> {
public:
    ArrayContainerIterator(ContainerType* container, size_t index) :
            container_(container), index_(index) {}

    bool operator==(const ArrayContainerIterator& other) {
        assert(container_ == other.container_);
        return index_ == other.index_;
    }
    bool operator!=(const ArrayContainerIterator& other) {
        return !operator==(other);
    }

    ArrayContainerIterator& operator++() {
        index_ += 1;
        return *this;
    }

    ValueType& operator*() {
        return container_->at(index_);
    }

    ValueType* operator->() {
        return &(this->operator*());
    }

private:
    ContainerType* const container_;
    size_t index_;
};

}  // namespace base

/** An array-based circular buffer. It is inspired by java.util.ArrayDeque, with an interface
similar to std::deque. */
template <typename T>
class CircularBuffer {
public:
    CircularBuffer() :
            capacity_(INITIAL_CAPACITY), array_(allocator.allocate(capacity_)),
            next_read_(0), next_write_(0) {
        assert(array_ != NULL);
    }

    ~CircularBuffer() {
        clear();

        allocator.deallocate(array_, capacity_);
        array_ = NULL;
        capacity_ = 0;
    }

    /** Removes all the elements from the queue. */
    void clear() {
        // Destruct all the elements in the queue
        while (!empty()) {
            pop_front();
        }

        next_read_ = 0;
        next_write_ = 0;
    }

    /** Returns a reference to the element at the head of the queue. */
    T& front() {
        assert(!empty());
        return array_[next_read_];
    }
    const T& front() const { return const_cast<CircularBuffer*>(this)->front(); }

    /** Removes the first element from the queue. */
    void pop_front() {
        assert(!empty());
        // Destruct the element in the queue and increment to the next element
        array_[next_read_].~T();
        next_read_ = increment(next_read_);
    }

    /** Returns a reference to the element at the tail of the queue. */
    T& back() {
        assert(!empty());
        return array_[decrement(next_write_)];
    }

    /** Removes the last element from the queue. */
    void pop_back() {
        // Decrement the write position and destroy that element
        next_write_ = decrement(next_write_);
        array_[next_write_].~T();
    }

    /** Removes the first element from the queue and returns it. */
    T dequeue() {
        T result = front();
        pop_front();
        return result;
    }

    void push_back(const T& value) {
        // Check if the buffer is full
        size_t next = increment(next_write_);
        if (next == next_read_) {
            increaseCapacity();
            // Recalculate the next value of next_write_
            next = increment(next_write_);
            assert(size() < capacity_ - 1);
            assert(next != next_read_);
        }

        // Use placement new to copy construct the element
        new (array_ + next_write_) T(value);

        // Move the next empty position forward
        next_write_ = next;
    }

    void push_front(const T& value) {
        // Check if the buffer is full
        size_t next = decrement(next_read_);
        if (next == next_write_) {
            increaseCapacity();
            // Recalculate the next value of next_write_
            next = decrement(next_read_);
            assert(size() < capacity_ - 1);
            assert(next != next_write_);
        }

        // Use placement new to copy construct the element
        new (array_ + next) T(value);

        // Move the first filled position backward
        next_read_ = next;
    }

    /** Returns the number of elements stored in this buffer. */
    size_t size() const {
        size_t diff = next_write_ - next_read_;
        if (diff > capacity_) {
            // overflow case: add capacity back to get into the correct range
            diff += capacity_;
        }
        assert(diff < capacity_);
        return diff;
    }

    /** Returns true if this buffer is empty. */
    bool empty() const { return next_read_ == next_write_; }

    /** Returns a reference to the object at index in the queue, where 0 is the
    head and size()-1 is the tail. */
    T& at(size_t index) {
        return array_[indexToBuffer(index)];
    }
    const T& at(size_t index) const {
        return const_cast<CircularBuffer*>(this)->at(index);
    }

    /** Erases the element at index. */
    void erase(size_t index) {
        // translate the index into the buffer index
        index = indexToBuffer(index);

        // Shift all the elements using assignment
        size_t next = increment(index);
        while (next != next_write_) {
            array_[index] = array_[next];
            index = next;
            next = increment(next);
        }

        // Erase the final element
        array_[index].~T();
        next_write_ = index;
    }

    // Removes the first element that equals value from the queue. Returns true a value is removed.
    bool eraseValue(const T& value) {
        // TODO: This might be slightly faster if we used the array directly.
        for (size_t i = 0; i < size(); ++i) {
            if (at(i) == value) {
                erase(i);
                return true;
            }
        }
        return false;
    }

    typedef T value_type;
    typedef base::ArrayContainerIterator<CircularBuffer, T> iterator;
    typedef base::ArrayContainerIterator<const CircularBuffer, const T> const_iterator;

    /** Returns an iterator at the first element in the queue. Note: this is less efficient than
    using at(). */
    iterator begin() {
        return iterator(this, 0);
    }
    const_iterator begin() const { return const_iterator(this, 0); }

    /** Returns an iterator one past the end of the queue. Note: this is less efficient than
    using at(). */
    // TODO: Since end() is used as the end of a loop condition, this needs to be optimized.
    iterator end() {
        return iterator(this, size());
    }
    const_iterator end() const { return const_iterator(this, size()); }

    // 16 * 4 = 64; so this should be ~1 cache line on most CPUs
    static const int INITIAL_CAPACITY = 16;

private:
    size_t increment(size_t value) {
        value += 1;
        if (value == capacity_) {
            value = 0;
        }
        return value;
    }

    size_t decrement(size_t value) {
        if (value == 0) {
            return capacity_ - 1;
        } else {
            return value - 1;
        }
    }

    size_t indexToBuffer(size_t index) {
        assert(index < size());
        index += next_read_;
        if (index >= capacity_) {
            index -= capacity_;
        }
        return index;
    }

    void increaseCapacity() {
        // The buffer is full: we need to resize it
        assert(size() == capacity_ - 1);
        size_t next_capacity = capacity_ * 2;
        T* next_array = allocator.allocate(next_capacity);

        // Move the elements from this array into the new array
        // TODO: Use std::move when we move to C++0x (yuck)
        size_t i;
        for (i = 0; !empty(); ++i) {
            // Use placement new to copy construct in the new position
            assert(i < next_capacity-1);
            new (next_array + i) T(array_[next_read_]);
            // Destruct the previous value
            array_[next_read_].~T();
            next_read_ = increment(next_read_);
        }

        // Free the old array, swap it with the new array and positions
        allocator.deallocate(array_, capacity_);
        capacity_ = next_capacity;
        array_ = next_array;
        next_read_ = 0;
        next_write_ = i;
    }
    
    // TODO: Make this a template parameter?
    std::allocator<T> allocator;

    // TODO: We sacrifice one spot in the array to differentiate between empty and full
    // should we explicitly store the size instead? This only matters for sizeof(T) > sizeof(int)
    size_t capacity_;
    T* array_;
    size_t next_read_;
    size_t next_write_;

    // No copy constructor or operator=
    CircularBuffer(const CircularBuffer& other);
    CircularBuffer& operator=(const CircularBuffer& other);
};

#endif
