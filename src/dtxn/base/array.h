// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_ARRAY_H__
#define BASE_ARRAY_H__

#include <cassert>
#include <cstddef>

namespace base {

// This is not a constant, but should be optimized into one. See:
// http://stackoverflow.com/questions/95500/can-this-macro-be-converted-to-a-function
template<typename T, int SIZE>
inline int arraySize(const T (&array)[SIZE]) {
    return SIZE;
}

/** Fixed-size array class. Unlike std::array, this takes a runtime size. Unlike std::vector, it
does not require that T have a copy constructor. It only requires a default constructor.
NOTE: For primitive types, the array will be uninitialized, as is typical for C++. */
template <typename T>
class Array {
public:
    /** Create an uninitialized array with size elements. */
    Array(size_t elements) : array_(new T[elements]), size_(elements) {}
    ~Array() { delete[] array_; }

    T& operator[](size_t index) {
        assert(index < size_);
        return array_[index];
    }

    const T& operator[](size_t index) const {
        return (*const_cast<Array*>(this))[index];
    }

    size_t size() const {
        return size_;
    }

    typedef T* iterator;

    iterator begin() { return array_; }
    iterator end() { return array_ + size_; }

private:
    T* array_;
    size_t size_;
};

}  // namespace base

#endif
