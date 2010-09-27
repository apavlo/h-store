// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_STRINGUTIL_H__
#define BASE_STRINGUTIL_H__

#include <string>

namespace base {

// Returns a pointer to the raw array in a string.
// NOTE: const_cast<char*>(s->data()) fails due to reference counting implementations. See test.
static inline char* stringArray(std::string* s) {
    if (s->empty()) return NULL;
    return &(*s->begin());
}

}
#endif
