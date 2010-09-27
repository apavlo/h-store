// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_UNORDERED_SET_H__
#define BASE_UNORDERED_SET_H__

// This exists solely because Mac OS X's std::tr1::unordered_set implementation sucks:
// * Copy constructor has compile errors
// * erase always returns zero

#ifdef __APPLE__
#include <ext/hash_set>
#include <tr1/functional>

#include "base/machash.h"

namespace base {

template <typename KeyType>
class unordered_set : public __gnu_cxx::hash_set<KeyType, std::tr1::hash<KeyType> > {};

}

#else
#include <tr1/unordered_set>

namespace base {

template <typename KeyType>
class unordered_set : public std::tr1::unordered_set<KeyType> {};

}
#endif

#endif
