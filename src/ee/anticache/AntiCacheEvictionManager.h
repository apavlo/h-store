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

#include "catalog/table.h"
#include "storage/TupleIterator.h"
#include "anticache/EvictionIterator.h"
#include "common/tabletuple.h"
#include "execution/VoltDBEngine.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "anticache/AntiCacheDB.h"

#include <vector>
#include <map>

namespace voltdb {

class Table;
class PersistentTable;
class EvictionIterator;    
    
class AntiCacheEvictionManager {
        
public: 
    AntiCacheEvictionManager(const VoltDBEngine *engine);
    ~AntiCacheEvictionManager();
    
    bool updateTuple(PersistentTable* table, TableTuple* tuple, bool is_insert);
    bool updateUnevictedTuple(PersistentTable* table, TableTuple* tuple);
    bool removeTuple(PersistentTable* table, TableTuple* tuple); 

    Table* evictBlock(PersistentTable *table, long blockSize, int numBlocks);
    bool evictBlockToDisk(PersistentTable *table, const long block_size, int num_blocks);
    bool evictBlockToDiskInBatch(PersistentTable *table, PersistentTable *childTable, const long block_size, int num_blocks);
    Table* evictBlockInBatch(PersistentTable *table, PersistentTable *childTable, long blockSize, int numBlocks);
    // Table* readBlocks(PersistentTable *table, int numBlocks, int16_t blockIds[], int32_t tuple_offsets[]);
    bool mergeUnevictedTuples(PersistentTable *table);
    bool readEvictedBlock(PersistentTable *table, int16_t block_id, int32_t tuple_offset);
    //int numTuplesInEvictionList(); 

    int chooseDB();
    int16_t migrateBlock(int16_t blockId, AntiCacheDB* srcDB, AntiCacheDB* dstDB); 
    int16_t migrateLRUBlock(AntiCacheDB* srcDB, AntiCacheDB* dstDB); 
    // -----------------------------------------
    // Evicted Access Tracking Methods
    // -----------------------------------------
    
    inline void initEvictedAccessTracker() {
        m_evicted_tables.clear();
        m_evicted_block_ids.clear();
        m_evicted_offsets.clear();
    }
    inline bool hasEvictedAccesses() const {
        return (m_evicted_block_ids.empty() == false);
    }
    void recordEvictedAccess(catalog::Table* catalogTable, TableTuple *tuple);
    void throwEvictedAccessException();
    
protected:
    void initEvictResultTable();
    
    bool removeTupleSingleLinkedList(PersistentTable* table, uint32_t removal_id);
    bool removeTupleDoubleLinkedList(PersistentTable* table, TableTuple* tuple_to_remove, uint32_t removal_id);
    
    void printLRUChain(PersistentTable* table, int max, bool forward);
    char *itoa(uint32_t i);
    
    Table *m_evictResultTable;
    const VoltDBEngine *m_engine;
    Table *m_readResultTable;

    // Used at runtime to track what evicted tuples we touch and throw an exception
    ValuePeeker peeker; 
    TableTuple* m_evicted_tuple; 
    
    std::vector<catalog::Table*> m_evicted_tables;
    std::vector<int16_t> m_evicted_block_ids;
    std::vector<int32_t> m_evicted_offsets;

    std::map<int16_t, AntiCacheDB*> m_db_lookup_table;
    
}; // AntiCacheEvictionManager class


}

#endif
