// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef REPLICATION_CACHEDSLIDINGARRAY_H__
#define REPLICATION_CACHEDSLIDINGARRAY_H__

#include "base/cachedcontainer.h"
#include "base/slidingarray.h"

namespace replication {

template <typename T>
class CachedSlidingArray : public base::CachedContainer<T, base::SlidingArray> {
public:
    size_t firstIndex() const { return this->container().firstIndex(); }
    size_t nextIndex() const { return this->container().nextIndex(); }
};

}  // namespace replication
#endif
