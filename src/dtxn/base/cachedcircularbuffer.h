// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_CACHEDCIRCULARBUFFER_H__
#define BASE_CACHEDCIRCULARBUFFER_H__

#include "base/cachedcontainer.h"
#include "base/circularbuffer.h"

namespace base {

template <typename T>
class CachedCircularBuffer : public CachedContainer<T, CircularBuffer> {};

}  // namespace base
#endif
