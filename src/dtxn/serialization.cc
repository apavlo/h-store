// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include "serialization.h"

#include <cassert>
#include <cstring>

#include "io/buffer.h"

using std::string;

namespace serialization {

void serialize(bool value, string* out) {
    // Normalizes value: it could be some weird bit pattern
    int8_t byte = 0;
    if (value) byte = 1;
    serialize(byte, out);
}

void serialize(int8_t value, std::string* out) {
    out->push_back(value);
}

template <typename T>
static inline void memcpySerialize(const T& v, string* out) {
    out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

void serialize(int32_t value, string* out) {
    memcpySerialize(value, out);
}

void serialize(int64_t value, string* out) {
    memcpySerialize(value, out);
}

void serialize(float value, string* out) {
    memcpySerialize(value, out);
}

void serialize(const void* bytes, size_t length, string* out) {
    int32_t size = static_cast<int32_t>(length);
    assert(0 <= size && static_cast<size_t>(size) == length);
    serialize(size, out);
    out->append(reinterpret_cast<const char*>(bytes), length);
}

template <typename T>
static inline const char* memcpyDeserialize(T* v, const char* start, const char* end) {
    const char* next = start + sizeof(*v);
    assert(next <= end);
    memcpy(v, start, sizeof(*v));
    return next;
}

const char* deserialize(bool* value, const char* start, const char* end) {
    int8_t byte;
    const char* copy_end = memcpyDeserialize(&byte, start, end);
    *value = byte;
    return copy_end;
}

const char* deserialize(int8_t* value, const char* start, const char* end) {
    assert(start <= end);
    *value = *start;
    start += 1;
    return start;
}

const char* deserialize(int32_t* value, const char* start, const char* end) {
    return memcpyDeserialize(value, start, end);
}

const char* deserialize(int64_t* value, const char* start, const char* end) {
    return memcpyDeserialize(value, start, end);
}

const char* deserialize(float* value, const char* start, const char* end) {
    return memcpyDeserialize(value, start, end);
}

const char* deserialize(string* value, const char* start, const char* end) {
    int32_t size;
    start = deserialize(&size, start, end);

    const char* copy_end = start + size;
    assert(copy_end <= end);
    value->assign(start, size);
    return copy_end;
}


void serialize(bool value, io::FIFOBuffer* out) {
    // Normalizes value: it could be some weird bit pattern
    int8_t byte = 0;
    if (value) byte = 1;
    serialize(byte, out);
}

template <typename T>
static inline void memcpySerialize(const T& v, io::FIFOBuffer* out) {
    out->copyIn(&v, sizeof(v));
}

void serialize(int8_t value, io::FIFOBuffer* out) {
    memcpySerialize(value, out);
}

void serialize(int32_t value, io::FIFOBuffer* out) {
    memcpySerialize(value, out);
}

void serialize(int64_t value, io::FIFOBuffer* out) {
    memcpySerialize(value, out);
}

void serialize(float value, io::FIFOBuffer* out) {
    memcpySerialize(value, out);
}

void serialize(const void* bytes, size_t length, io::FIFOBuffer* out) {
    int32_t size = static_cast<int32_t>(length);
    assert(0 <= size && static_cast<size_t>(size) == length);
    serialize(size, out);
    out->copyIn(bytes, size);
}

}  // namespace serialization
