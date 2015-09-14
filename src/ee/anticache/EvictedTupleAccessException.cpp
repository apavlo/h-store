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

#include "common/debuglog.h"
#include "anticache/EvictedTupleAccessException.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include <iostream>
#include <cassert>

using namespace voltdb;


std::string EvictedTupleAccessException::ERROR_MSG = std::string("Txn tried to access evicted tuples");

EvictedTupleAccessException::EvictedTupleAccessException(int tableId, int numBlockIds, int32_t blockIds[], int32_t tupleIDs[]) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EVICTED_TUPLE, EvictedTupleAccessException::ERROR_MSG),
        m_tableId(tableId),
        m_numBlockIds(numBlockIds),
        m_blockIds(blockIds), 
        m_tupleKeys(tupleIDs),
        // IMPORTANT: We always set the partitionId to the null id here (-1)
        // The PartitionExecutor will set this correctly for us up in the Java layer
        m_partitionId(-1) {
    
    VOLT_TRACE("In EvictedTupleAccessException constructor...setting exception type to %d.", VOLT_EE_EXCEPTION_TYPE_EVICTED_TUPLE); 
    
    // Nothing to see, nothing to do...
}

void EvictedTupleAccessException::p_serialize(ReferenceSerializeOutput *output) {
    
    VOLT_TRACE("In EvictedTupleAccessException p_serialize()."); 
    //  This is hack. But the string buffer in Java layer has limit.
    //if (m_numBlockIds > 10000)
    //    m_numBlockIds = 10000;
    
    output->writeInt(m_tableId);
    output->writeInt(m_numBlockIds); // # of block ids
    //output->writeShort(static_cast<short>(m_numBlockIds)); // # of block ids
    for (int ii = 0; ii < m_numBlockIds; ii++) {
        output->writeInt(m_blockIds[ii]);
    }
    
    for(int ii = 0; ii<m_numBlockIds; ii++) {  // write out the tuple offsets 
        output->writeInt(m_tupleKeys[ii]);
    }
    output->writeInt(m_partitionId);
}
