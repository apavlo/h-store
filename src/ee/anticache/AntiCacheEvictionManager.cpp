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

#include "anticache/AntiCacheEvictionManager.h"
#include "common/types.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "common/debuglog.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "anticache/EvictionIterator.h"

#include <string>
#include <vector>

namespace voltdb
{
            
// -----------------------------------------
// AntiCacheEvictionManager Implementation 
// -----------------------------------------
    
AntiCacheEvictionManager::AntiCacheEvictionManager() {
    
    // Initialize readBlocks table
    this->initEvictResultTable();
}

AntiCacheEvictionManager::~AntiCacheEvictionManager() {
    delete m_evictResultTable;
}

void AntiCacheEvictionManager::initEvictResultTable() {
    std::string tableName = "EVICT_RESULT";
    CatalogId databaseId = 1;
    std::vector<std::string> colNames;
    std::vector<ValueType> colTypes;
    std::vector<int32_t> colLengths;
    std::vector<bool> colAllowNull;
    
    // TABLE_NAME
    colNames.push_back("TABLE_NAME");
    colTypes.push_back(VALUE_TYPE_VARCHAR);
    colLengths.push_back(4096);
    colAllowNull.push_back(false);
    
    // ANTICACHE_TUPLES_EVICTED
    colNames.push_back("ANTICACHE_TUPLES_EVICTED");
    colTypes.push_back(VALUE_TYPE_INTEGER);
    colLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    colAllowNull.push_back(false);
    
    // ANTICACHE_BLOCKS_EVICTED
    colNames.push_back("ANTICACHE_BLOCKS_EVICTED");
    colTypes.push_back(VALUE_TYPE_INTEGER);
    colLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    colAllowNull.push_back(false);
    
    // ANTICACHE_BYTES_EVICTED
    colNames.push_back("ANTICACHE_BYTES_EVICTED");
    colTypes.push_back(VALUE_TYPE_BIGINT);
    colLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    colAllowNull.push_back(false);
    
    TupleSchema *schema = TupleSchema::createTupleSchema(colTypes,
                                                         colLengths,
                                                         colAllowNull, true);
    
    m_evictResultTable = reinterpret_cast<Table*>(TableFactory::getTempTable(
                                                        databaseId,
                                                        tableName,
                                                        schema,
                                                        &colNames[0],
                                                        NULL));
}

// insert tuple at front of chain, next for eviction 
bool AntiCacheEvictionManager::updateUnevictedTuple(PersistentTable* table, TableTuple* tuple) {
    int tuples_in_chain; 
    int current_tuple_id = table->getTupleID(tuple->address()); // scan blocks for this tuple
    
    if (current_tuple_id < 0)
        return false; 
    
    if (table->getNumTuplesInEvictionChain() == 0) { // this is the first tuple in the chain
        //VOLT_INFO("Inserting the first tuple into the chain."); 
        
        table->setNewestTupleID(current_tuple_id); 
        table->setOldestTupleID(current_tuple_id); 
        
        table->setNumTuplesInEvictionChain(1); 
        
        return true; 
    }
    
    tuple->setTupleID(table->getOldestTupleID()); 
    table->setOldestTupleID(current_tuple_id);
    
    // increment the number of tuples in the eviction chain
    tuples_in_chain = table->getNumTuplesInEvictionChain(); 
    ++tuples_in_chain; 
    table->setNumTuplesInEvictionChain(tuples_in_chain); 
    
    return true; 
}
    
bool AntiCacheEvictionManager::updateTuple(PersistentTable* table, TableTuple* tuple, bool is_insert) {
    int tuples_in_chain; 
    int current_tuple_id = table->getTupleID(tuple->address()); // scan blocks for this tuple

    //VOLT_INFO("In updateTuple() for tuple %d. %d tuples in the chain.", current_tuple_id, table->getNumTuplesInEvictionChain()); 
    
    if (current_tuple_id < 0)
        return false; 
    
    // this is an update, so we have to remove the previous entry in the chain
    if (!is_insert) {        
        assert(table->getNumTuplesInEvictionChain() > 0); 
        removeTuple(table, current_tuple_id); 
    }
    
    if (table->getNumTuplesInEvictionChain() == 0) { // this is the first tuple in the chain
        table->setNewestTupleID(current_tuple_id); 
        table->setOldestTupleID(current_tuple_id); 

        table->setNumTuplesInEvictionChain(1); 
        
        return true; 
    } else if (table->getNumTuplesInEvictionChain() == 1) {
        table->setNewestTupleID(current_tuple_id); 

        TableTuple oldest_tuple(table->dataPtrForTuple(table->getOldestTupleID()), table->m_schema); 
        oldest_tuple.setTupleID(current_tuple_id); 

        table->setNumTuplesInEvictionChain(2); 
        
        return true; 
    }
    
    // get the newest tuple in the LRU chain
    TableTuple newest_tuple(table->dataPtrForTuple(table->getNewestTupleID()), table->m_schema); 

    // set the old "newest" tuple to point to the tuple we're updating
    newest_tuple.setTupleID(current_tuple_id); 
    
    // insert the tuple we're updating to be the newest (i.e. the back of the lru chain) for this table
    table->setNewestTupleID(current_tuple_id);
    
    // increment the number of tuples in the eviction chain
    tuples_in_chain = table->getNumTuplesInEvictionChain(); 
    ++tuples_in_chain; 
    table->setNumTuplesInEvictionChain(tuples_in_chain); 

    VOLT_DEBUG("tuples in eviction chain: %d", tuples_in_chain);
    VOLT_DEBUG("newest tuple in chain: %d", table->getNewestTupleID());
    VOLT_DEBUG("oldest tuple in the chain: %d", table->getOldestTupleID());
    
    TableTuple oldest_tuple(table->dataPtrForTuple(table->getOldestTupleID()), table->m_schema);
    
    VOLT_DEBUG("2nd oldest tuple in chain: %d", oldest_tuple.getTupleID()); 
    
    return true; 
}
    
bool AntiCacheEvictionManager::removeTuple(PersistentTable* table, TableTuple* tuple) {
    int current_tuple_id = table->getTupleID(tuple->address()); 
        
    return removeTuple(table, current_tuple_id); 
}
    
bool AntiCacheEvictionManager::removeTuple(PersistentTable* table, int removal_id) {
    bool tuple_found = false; 
    
    int tuples_in_chain; 
    
    // ids for iterating through the list
    uint32_t current_tuple_id, previous_tuple_id, next_tuple_id, newest_tuple_id; 
    
    // assert we have tuples in the eviction chain before we try to remove anything
    tuples_in_chain = table->getNumTuplesInEvictionChain(); 

    if (tuples_in_chain <= 0)
        return false; 

    previous_tuple_id = 0; 
    current_tuple_id = table->getOldestTupleID(); 
    newest_tuple_id = table->getNewestTupleID(); 
    
    // set the tuple to the first tuple in the chain (i.e. oldest)
    TableTuple tuple = table->tempTuple();
    tuple.move(table->dataPtrForTuple(current_tuple_id)); 
    
    // we're removing the front  of the chain, i.e. the oldest tuple
    if (table->getOldestTupleID() == removal_id) {
        //VOLT_INFO("Removing the first tuple in the eviction chain."); 
        if (table->getNumTuplesInEvictionChain() == 1) { // this is the only tuple in the chain
            table->setOldestTupleID(0); 
            table->setNewestTupleID(0); 
        } else {
            next_tuple_id = tuple.getTupleID(); 
            table->setOldestTupleID(next_tuple_id); 

            //VOLT_INFO("Oldest tuple in the chain: %d", next_tuple_id); 
        }
        tuple_found = true; 
    }

    int iterations = 0; 
    while(!tuple_found && iterations < (table->usedTupleCount()-1)) {
        assert(iterations < table->allocatedTupleCount()); 
        
        // we've found the tuple we want to remove
        if (current_tuple_id == removal_id) {
            next_tuple_id = tuple.getTupleID(); 
            
            // create a tuple from the previous tuple id in the chain
            tuple.move(table->dataPtrForTuple(previous_tuple_id)); 
            
            // set the previous tuple to point to the next tuple
            tuple.setTupleID(next_tuple_id); 
            
            tuple_found = true; 
            break; 
        }
        
        // advance pointers
        previous_tuple_id = current_tuple_id; 
        current_tuple_id = tuple.getTupleID(); 
        tuple.move(table->dataPtrForTuple(current_tuple_id));
        
        iterations++; 
    }
    
    if (current_tuple_id == newest_tuple_id && !tuple_found) { // we are at the back of the chain
        if (current_tuple_id == removal_id) { // we're removing the back of the chain
            // set the previous tuple pointer to 0 since it is now the back of the chain
            tuple.move(table->dataPtrForTuple(previous_tuple_id)); 
            tuple.setTupleID(0);
            
            table->setNewestTupleID(previous_tuple_id); 
            
            tuple_found = true; 
        }
    }
    
    if (tuple_found) {
        --tuples_in_chain; 
        table->setNumTuplesInEvictionChain(tuples_in_chain); 
        
        return true; 
    }
    
    return false; 
}

Table* AntiCacheEvictionManager::evictBlock(PersistentTable *table, long blockSize, int numBlocks) {
    int32_t lastTuplesEvicted = table->getTuplesEvicted();
    int32_t lastBlocksEvicted = table->getBlocksEvicted();
    int64_t lastBytesEvicted  = table->getBytesEvicted();
    
    if (table->evictBlockToDisk(blockSize, numBlocks) == false) {
        throwFatalException("Failed to evict tuples from table '%s'", table->name().c_str());
    }
    
    int32_t tuplesEvicted = table->getTuplesEvicted() - lastTuplesEvicted;
    int32_t blocksEvicted = table->getBlocksEvicted() - lastBlocksEvicted; 
    int64_t bytesEvicted = table->getBytesEvicted() - lastBytesEvicted;
    
    m_evictResultTable->deleteAllTuples(false);
    TableTuple tuple = m_evictResultTable->tempTuple();
    
    int idx = 0;
    tuple.setNValue(idx++, ValueFactory::getStringValue(table->name()));
    tuple.setNValue(idx++, ValueFactory::getIntegerValue(static_cast<int32_t>(tuplesEvicted)));
    tuple.setNValue(idx++, ValueFactory::getIntegerValue(static_cast<int32_t>(blocksEvicted)));
    tuple.setNValue(idx++, ValueFactory::getBigIntValue(static_cast<int32_t>(bytesEvicted)));
    m_evictResultTable->insertTuple(tuple);
    
    return (m_evictResultTable);
}

Table* AntiCacheEvictionManager::readBlocks(PersistentTable *table, int numBlocks, int16_t blockIds[], int32_t tuple_offsets[]) {
    
    VOLT_INFO("Reading %d evicted blocks.", numBlocks);

    for(int i = 0; i < numBlocks; i++)
        table->readEvictedBlock(blockIds[i], tuple_offsets[i]);

    return (m_readResultTable);
}


}

