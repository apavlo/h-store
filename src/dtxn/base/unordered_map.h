// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_UNORDERED_MAP_H__
#define BASE_UNORDERED_MAP_H__

// This exists solely because Mac OS X's std::tr1::unordered_map implementation sucks:
// * It can't hash int64_t
// * ::erase does not return results
// * ::find does not work with const maps

#ifdef __APPLE__
#include <ext/hash_map>
#include <tr1/functional>

#include "base/machash.h"

namespace base {

template <typename KeyType, typename ValueType, typename HashFunctor=std::tr1::hash<KeyType> >
class unordered_map : public __gnu_cxx::hash_map<KeyType, ValueType, HashFunctor> {
public:
    typedef typename __gnu_cxx::hash_map<KeyType, ValueType, HashFunctor> BaseHash;

    typename BaseHash::const_iterator erase(const typename BaseHash::const_iterator& it) {
        typename BaseHash::const_iterator copy = it;
        ++copy;
        // ::erase only takes an iterator, not a const_iterator
        BaseHash::erase(it->first);
        return copy;
    }

    size_t erase(const KeyType& key) {
        return BaseHash::erase(key);
    }

    typename BaseHash::iterator erase(const typename BaseHash::iterator& it) {
        typename BaseHash::iterator copy = it;
        ++copy;
        BaseHash::erase(it);
        return copy;
    }
};

}

#else
#include <tr1/unordered_map>

namespace base {

template <typename KeyType, typename ValueType, typename HashFunctor=std::tr1::hash<KeyType> >
class unordered_map : public std::tr1::unordered_map<KeyType, ValueType, HashFunctor> {};

}
#endif

#endif
