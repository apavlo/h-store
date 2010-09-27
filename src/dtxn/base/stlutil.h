// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef STLUTIL_H__
#define STLUTIL_H__

#include <algorithm>

// Deletes all elements in STL container.
template <typename T>
static void STLDeleteElements(T* container) {
    const typename T::iterator end = container->end();
    for (typename T::iterator i = container->begin(); i != end; ++i) {
        delete *i;
    }
    container->clear();
};

// Deletes all values (iterator->second) in STL container.
template <typename T>
static void STLDeleteValues(T* container) {
    const typename T::iterator end = container->end();
    for (typename T::iterator i = container->begin(); i != end; ++i) {
        delete i->second;
    }
    container->clear();
};

// Adds all the values in source to the end of destination using push_back
template <typename T, typename U>
void STLExtend(T* destination, const U& source) {
    typename U::const_iterator i = source.begin();
    const typename U::const_iterator end = source.end();
    for (; i != end; ++i) {
        destination->push_back(*i);
    }
}

namespace base {

/** Returns true if value is in container. */
template <typename ContainerType, typename ValueType>
bool contains(const ContainerType& container, const ValueType& value) {
    return std::find(container.begin(), container.end(), value) != container.end();
}

/*
// TODO: Is this still useful?
// TODO: Should this be in std::tr1? Technically probably not, but it is convenient
template <typename T, typename U>
class pairhash {
public:
    size_t operator()(const std::pair<T, U>& value) const {
        size_t h = t_hash_(value.first);
        // Bitwise rotation: order matters (hash(x, y) != hash(y, x)) and equality generates
        // different hashes (hash(x, x) != hash(y, y))
        static const int BITS = 19;
        h = (h << BITS) | (h >> (sizeof(h)*8 - BITS));
        return h ^ u_hash_(value.second);
    }

private:
    std::tr1::hash<T> t_hash_;
    std::tr1::hash<U> u_hash_;
};
*/

}

#endif
