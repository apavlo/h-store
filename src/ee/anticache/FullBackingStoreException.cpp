/* Copyright (C) 2012 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "anticache/FullBackingStoreException.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include <iostream>

using namespace voltdb;

std::string FullBackingStoreException::ERROR_MSG = std::string("Backing store full");

FullBackingStoreException::FullBackingStoreException(uint32_t srcBlockId, uint32_t dstBlockId) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_UNKNOWN_BLOCK, FullBackingStoreException::ERROR_MSG),
        m_src_blockId(srcBlockId), 
        m_dst_blockId(dstBlockId) {
    
    // Nothing to see, nothing to do...
}

void FullBackingStoreException::p_serialize(ReferenceSerializeOutput *output) {
    output->writeInt(m_src_blockId);
    output->writeInt(m_dst_blockId);
}
