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

#include "anticache/EvictedTable.h"
#include "storage/persistenttable.h"


namespace voltdb {

EvictedTable::EvictedTable(ExecutorContext *ctx) : PersistentTable(ctx, false) {
    
}

#ifdef ANTICACHE_TIMESTAMPS_PRIME
    enum { prime_size = 25 };
    static const int prime_list[prime_size] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97};
#endif
    
/*
 Insert a tuple into the evicted table but don't create any UNDO action. Return the address
 of the newly inserted tuple. 
 */
const void* EvictedTable::insertEvictedTuple(TableTuple &source) {
    // not null checks at first
    if (!checkNulls(source)) {
        throwFatalException("Failed to insert tuple into table %s for undo:"
                            " null constraint violation\n%s\n", m_name.c_str(),
                            source.debugNoHeader().c_str());
    }
    
    // First get the next free tuple This will either give us one from
    // the free slot list, or grab a tuple at the end of our chunk of
    // memory
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;
    
    // Then copy the source into the target
    m_tmpTarget1.copyForPersistentInsert(source);
    m_tmpTarget1.setDeletedFalse();
    
    // Make sure this tuple is marked as evicted, so that we know it is an
    // evicted tuple as we iterate through the index
    m_tmpTarget1.setEvictedTrue(); 

    assert(m_tmpTarget1.isEvicted()); 
    
    return m_tmpTarget1.address(); 
}
    
void EvictedTable::deleteEvictedTuple(TableTuple source) {
    if(source.address() == NULL)
        return; 
    
    deleteTupleStorage(source);
}
    
}

