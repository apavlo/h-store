// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef IO_MESSAGE_H__
#define IO_MESSAGE_H__

namespace io {

class FIFOBuffer;

// Interface for serializable/deserializable messages.
class Message {
public:
    virtual ~Message() {}

    // Serialize this message to buffer.
    // TODO: If we want to support serializing to other types of buffers, replace FIFOBuffer with
    // an interface?
    virtual void serialize(FIFOBuffer* buffer) const = 0;
};

}

#endif
