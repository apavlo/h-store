// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_CACHEDCONTAINER_H__
#define BASE_CACHEDCONTAINER_H__

#include <vector>

#include "base/stlutil.h"

namespace base {

// Defines a container that reuses objects that have been allocated previously.
// T must support .clear() and default construction.
// We could reuse the elements in the underlying container, but then T needs to support
// copying and it makes resizing more expensive.
template <typename T, template <typename U> class Container>
class CachedContainer {
public:
    CachedContainer() {}
    ~CachedContainer() {
        STLDeleteElements(&container_);
        STLDeleteElements(&recycler_);
    }

    // Allocate or reuse a T, at the end of the container.
    T* add() {
        T* v = NULL;
        if (!recycler_.empty()) {
            v = recycler_.back();
            recycler_.pop_back();
        } else {
            v = new T();
        }

        container_.push_back(v);
        return v;
    }

    bool empty() const { return container_.empty(); }
    size_t size() const { return container_.size(); }
    const T& at(size_t index) const { return *container_.at(index); }
    const T& front() const { return *container_.front(); }
    T* mutable_front() { return container_.front(); }

    void pop_front() {
        // Fetch and remove the first element
        T* v = container_.dequeue();

        // Hold on to the element in the recycle list
        v->clear();
        recycler_.push_back(v);
    }

    T* mutableAt(size_t index) { return container_.at(index); }

protected:
    const Container<T*>& container() const { return container_; }

private:
    // The underlying container.
    Container<T*> container_;

    // Stores allocated objects to be recycled.
    std::vector<T*> recycler_;

    CachedContainer(const CachedContainer& other);
    CachedContainer& operator=(const CachedContainer& other);
};

}  // namespace base
#endif
