// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef SERIALIZATION_H__
#define SERIALIZATION_H__

#include <stdint.h>

#include <string>

namespace io {
class FIFOBuffer;
}

namespace serialization {

// TODO: Overloading sucks due to implicit conversions. Make these explicit names?
void serialize(bool value, std::string* out);
void serialize(int8_t value, std::string* out);
void serialize(int16_t value, std::string* out);  // NOT DEFINED (yet)
void serialize(int32_t value, std::string* out);
void serialize(int64_t value, std::string* out);
void serialize(float value, std::string* out);
void serialize(double value, std::string* out);  // NOT DEFINED (yet)

/** NOT DEFINED: this prevents implicit conversions. */
void serialize(const char* value, std::string* out);

/** Serializes a blob of binary data. This deserializes as a std::string. */
void serialize(const void* bytes, size_t length, std::string* out);

/** Serializes a std::string containing arbitrary binary data. */
inline void serialize(const std::string& value, std::string* out) {
    serialize(value.data(), value.size(), out);
}

const char* deserialize(bool* value, const char* start, const char* end);
//~ const char* deserialize(char* value, const char* start, const char* end);
const char* deserialize(int8_t* value, const char* start, const char* end);
//~ const char* deserialize(int16_t* value, const char* start, const char* end);
const char* deserialize(int32_t* value, const char* start, const char* end);
const char* deserialize(int64_t* value, const char* start, const char* end);
const char* deserialize(float* value, const char* start, const char* end);
//~ const char* deserialize(double* value, const char* start, const char* end);
const char* deserialize(std::string* value, const char* start, const char* end);

#ifndef __sun__
// On Linux, char* and int8_t* are distinct types. On Solaris, they are equivalent.
inline const char* deserialize(char* value, const char* start, const char* end) {
    return deserialize(reinterpret_cast<int8_t*>(value), start, end);
}

inline void serialize(char value, std::string* out) {
    serialize(static_cast<int8_t>(value), out);
}
#endif


// TODO: Overloading sucks due to implicit conversions. Make these explicit names?
void serialize(bool value, io::FIFOBuffer* out);
void serialize(int8_t value, io::FIFOBuffer* out);
void serialize(int16_t value, io::FIFOBuffer* out);  // NOT DEFINED (yet)
void serialize(int32_t value, io::FIFOBuffer* out);
void serialize(int64_t value, io::FIFOBuffer* out);
void serialize(float value, io::FIFOBuffer* out);
void serialize(double value, io::FIFOBuffer* out);  // NOT DEFINED (yet)

/** NOT DEFINED: this prevents implicit conversions. */
void serialize(const char* value, io::FIFOBuffer* out);

/** Serializes a blob of binary data. This deserializes as a std::string. */
void serialize(const void* bytes, size_t length, io::FIFOBuffer* out);

/** Serializes a std::string containing arbitrary binary data. */
static inline void serialize(const std::string& value, io::FIFOBuffer* out) {
    serialize(value.data(), value.size(), out);
}


}

#endif
