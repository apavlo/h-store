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


#ifndef ANTICACHEEVICTIONMANAGER_H
#define ANTICACHEEVICTIONMANAGER_H

#include "storage/TupleIterator.h"
#include "anticache/EvictionIterator.h"
#include "common/tabletuple.h"

namespace voltdb {

class Table;
class PersistentTable;
class EvictionIterator;    
    
class AntiCacheEvictionManager {
        
public: 
    AntiCacheEvictionManager();
    ~AntiCacheEvictionManager();
    
    bool updateTuple(PersistentTable* table, TableTuple* tuple, bool is_insert);
    bool updateUnevictedTuple(PersistentTable* table, TableTuple* tuple);
    bool removeTuple(PersistentTable* table, TableTuple* tuple); 

    Table* evictBlock(PersistentTable *table, long blockSize, int numBlocks);
    Table* readBlocks(PersistentTable *table, int numBlocks, int16_t blockIds[], int32_t tuple_offsets[]);
    
    //int numTuplesInEvictionList(); 
    
protected:
    void initEvictResultTable();
    Table *m_evictResultTable;
    
    // TODO void initReadResultTable();
    Table *m_readResultTable;
    
    bool removeTuple(PersistentTable* table, int removal_id); 

    
}; // AntiCacheEvictionManager class


    
}

#endif
