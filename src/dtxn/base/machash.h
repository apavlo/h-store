// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_MACHASH_H__
#define BASE_MACHASH_H__

// This exists solely because Mac OS X's hash implementation sucks:
// * std::tr1::hash can't hash int64_t 
// * __gnu_cxx::hash can't hash int64_t, string, or void*

#ifdef __APPLE__

#include <tr1/functional>

namespace std { namespace tr1 {

template <> class hash<int64_t> {
public:
    size_t operator()(const int64_t& v) const {
#ifdef __LP64__
        assert(sizeof(size_t) == sizeof(int64_t));
        return (size_t) v;
#else
        assert(sizeof(size_t) == 4);
        size_t h = (size_t) v;
        h ^= (size_t)(v >> 32);
        return h;
#endif
    }
};

}}

#else
#error Only include this on Mac OS X?
#endif

#endif
