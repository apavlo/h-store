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

#ifndef HSTORENVMEVICTEDTABLE_H
#define HSTORENVMEVICTEDTABLE_H

#include "storage/persistenttable.h"
#include "common/executorcontext.hpp"
#include "common/Pool.hpp"


namespace voltdb {
    
class NVMEvictedTable : public PersistentTable {
        
    friend class TableFactory;
    
    public: 
        
        NVMEvictedTable(); 
                        
        const void* insertNVMEvictedTuple(TableTuple &source);
    
        void deleteNVMEvictedTuple(TableTuple source);


    protected:
        inline void allocateNextBlock() {
            #ifdef MEMCHECK
            int bytes = m_schema->tupleLength() + TUPLE_HEADER_SIZE;
            #else
            int bytes = m_tableAllocationTargetSize;
            #endif

            char *memory = NULL;
            VOLT_WARN("MMAP : PId:: %d Table: %s  Bytes:: %d ",
                    m_executorContext->getPartitionId(), this->name().c_str(), bytes);

            memory = (char*)m_pool->allocate(bytes);

            VOLT_WARN("MMAP : Table: %s :: Memory Pointer : %p ",
                    this->name().c_str(), memory);

            if (memory == NULL) {
                VOLT_ERROR("MMAP : initialization error.");
                throwFatalException("Failed to map file.");
            }

            m_data.push_back(memory);


            m_allocatedTuples += m_tuplesPerBlock;
        } 

        //NVMEvictedTable(ExecutorContext *ctx);
        NVMEvictedTable(ExecutorContext *ctx, const std::string name);
};
}

#endif












