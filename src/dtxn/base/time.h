// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef BASE_TIME_H__
#define BASE_TIME_H__

#include <cassert>

#include <sys/time.h>
#include <stdint.h>

/** Utility functions for working with time.
TODO: Implement a C++ wrapper class? */

namespace base {

static inline bool timevalValid(const timeval& time) {
    if (time.tv_sec < 0) return false;
    return 0 <= time.tv_usec && time.tv_usec < 1000000;
}

static inline int64_t timevalDiffMicroseconds(const timeval& end, const timeval& start) {
    assert(timevalValid(end));
    assert(timevalValid(start));
    return (end.tv_usec - start.tv_usec) + (end.tv_sec - start.tv_sec) * (int64_t) 1000000;
}

static inline void timevalAddMicroseconds(timeval* time, int64_t increment) {
    assert(timevalValid(*time));
    time->tv_sec += (time_t)(increment / 1000000);
    time->tv_usec += (time_t)(increment % 1000000);
    if (time->tv_usec >= 1000000) {
        time->tv_usec -= 1000000;
        assert(0 <= time->tv_usec && time->tv_usec < 1000000);
        time->tv_sec += 1;
    }
    assert(timevalValid(*time));
}

static inline bool timespecValid(const timespec& time) {
    if (time.tv_sec < 0) return false;
    return 0 <= time.tv_nsec && time.tv_nsec < 1000000000;
}

static inline void timespecAddNanoseconds(timespec* time, int64_t increment) {
    assert(timespecValid(*time));
    time->tv_sec += (time_t)(increment / 1000000000);
    time->tv_nsec += (time_t)(increment % 1000000000);
    if (time->tv_nsec >= 1000000000) {
        time->tv_nsec -= 1000000000;
        time->tv_sec += 1;
    }
    assert(timespecValid(*time));
}

static inline int64_t timespecDiffNanoseconds(const timespec& end, const timespec& start) {
    assert(timespecValid(end));
    assert(timespecValid(start));
    return (end.tv_nsec - start.tv_nsec) + (end.tv_sec - start.tv_sec) * (int64_t) 1000000000;
}

static inline void timespecToTimeval(timeval* out, const timespec& in) {
    assert(timespecValid(in));
    out->tv_sec = in.tv_sec;
    out->tv_usec = (in.tv_nsec + 500) / 1000;
    if (out->tv_usec == 1000000) {
        out->tv_sec += 1;
        out->tv_usec = 0;
    }
    assert(timevalValid(*out));
}

}  // namespace base

#endif
