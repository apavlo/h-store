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
#include <time.h>
#include <stdlib.h>

namespace voltdb
{
            
// -----------------------------------------
// AntiCacheEvictionManager Implementation 
// -----------------------------------------
    
    
/*
 
 LRU Eviction Chain
 
 This class manages the LRU eviction chain that organizes the tuples in eviction order. The head of the 
 chain represents the oldest tuple and the tail of the chain represents the newest tuple. Therefore, 
 eviction in LRU order is done in a front-to-back manner along the chain. 
 
 This chain can either be a single or a double linked list (specified at comiple time) and there are corresponding 
 update() methods for each. If a single linked list is used, iterating the chain in search of a tuple is done in 
 a front-to-back (i.e. from oldest to newest) manner. If a double linked list is used, iterating the chain is done 
 in a back-to-front (i.e. newest to oldest) manner. 
 
 */
    
AntiCacheEvictionManager::AntiCacheEvictionManager() {
    
    // Initialize readBlocks table
    this->initEvictResultTable();
    srand((int)time(NULL));
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
        table->setNewestTupleID(current_tuple_id); 
        table->setOldestTupleID(current_tuple_id); 
        
        table->setNumTuplesInEvictionChain(1); 
        
        return true; 
    }
    
    // update "next" pointer
    tuple->setNextTupleInChain(table->getOldestTupleID()); 
    
#ifdef ANTICACHE_REVERSIBLE_LRU
    // update "previous" pointer
    TableTuple oldest_tuple(table->dataPtrForTuple(table->getOldestTupleID()), table->m_schema);
    oldest_tuple.setPreviousTupleInChain(current_tuple_id);
#endif
    
    table->setOldestTupleID(current_tuple_id);
    
    // increment the number of tuples in the eviction chain
    tuples_in_chain = table->getNumTuplesInEvictionChain(); 
    ++tuples_in_chain; 
    table->setNumTuplesInEvictionChain(tuples_in_chain); 
    
    return true; 
}
    
bool AntiCacheEvictionManager::updateTuple(PersistentTable* table, TableTuple* tuple, bool is_insert) {
    int SAMPLE_RATE = 100; // aLRU sampling rate
    
    if(table->getEvictedTable() == NULL)  // no need to maintain chain for non-evictable tables
        return true; 
    
    int tuples_in_chain;

    uint32_t newest_tuple_id;
    uint32_t update_tuple_id = table->getTupleID(tuple->address()); // scan blocks for this tuple
        
    // this is an update, so we have to remove the previous entry in the chain
    if (!is_insert) {
                        
         if(rand() % SAMPLE_RATE != 0)  
             return true;
        
        assert(table->getNumTuplesInEvictionChain() > 0);
#ifdef ANTICACHE_REVERSIBLE_LRU
        removeTupleDoubleLinkedList(table, update_tuple_id);
#else
        removeTupleSingleLinkedList(table, update_tuple_id); 
#endif
    }
    
    if (table->getNumTuplesInEvictionChain() == 0) { // this is the first tuple in the chain
        table->setNewestTupleID(update_tuple_id); 
        table->setOldestTupleID(update_tuple_id); 

        table->setNumTuplesInEvictionChain(1); 
        
        return true; 
    }
    
    newest_tuple_id = table->getNewestTupleID();
    
    TableTuple newest_tuple(table->dataPtrForTuple(newest_tuple_id), table->m_schema);
    TableTuple update_tuple(table->dataPtrForTuple(update_tuple_id), table->m_schema);
    
    if(table->getNumTuplesInEvictionChain() == 1) {
        
        // update "next" pointer
        newest_tuple.setNextTupleInChain(update_tuple_id);
        
#ifdef ANTICACHE_REVERSIBLE_LRU
        // update "previous" pointer
        update_tuple.setPreviousTupleInChain(newest_tuple_id);
#endif

        // udpate oldest and newest pointers for the table
        table->setNewestTupleID(update_tuple_id);
        table->setOldestTupleID(newest_tuple_id);
        
        table->setNumTuplesInEvictionChain(2);
        
        return true; 
    }
        
    // update "next" pointer
    newest_tuple.setNextTupleInChain(update_tuple_id);

#ifdef ANTICACHE_REVERSIBLE_LRU
    // update "previous" pointer
    update_tuple.setPreviousTupleInChain(newest_tuple_id);
#endif
    
    // insert the tuple we're updating to be the newest 
    table->setNewestTupleID(update_tuple_id);
    
    // increment the number of tuples in the eviction chain
    tuples_in_chain = table->getNumTuplesInEvictionChain(); 
    ++tuples_in_chain; 

    table->setNumTuplesInEvictionChain(tuples_in_chain);
        
    return true; 
}
    
bool AntiCacheEvictionManager::removeTuple(PersistentTable* table, TableTuple* tuple) {
    int current_tuple_id = table->getTupleID(tuple->address());
    
    // the removeTuple() method called is dependent on whether it is a single or double linked list
#ifdef ANTICACHE_REVERSIBLE_LRU
    return removeTupleDoubleLinkedList(table, current_tuple_id);
#else
    return removeTupleSingleLinkedList(table, current_tuple_id);
#endif
}
    
// for the double linked list we start from the tail of the chain and iterate backwards
bool AntiCacheEvictionManager::removeTupleDoubleLinkedList(PersistentTable* table, uint32_t removal_id) {
    
    bool tuple_found = false;
    
    int tuples_in_chain;
        
    // ids for iterating through the list
    uint32_t current_tuple_id;
    uint32_t previous_tuple_id;
    uint32_t next_tuple_id;
    uint32_t oldest_tuple_id;
    
    // assert we have tuples in the eviction chain before we try to remove anything
    tuples_in_chain = table->getNumTuplesInEvictionChain();
    
    if (tuples_in_chain <= 0)
        return false;
    
    previous_tuple_id = 0;
    oldest_tuple_id = table->getOldestTupleID();
    current_tuple_id =  table->getNewestTupleID(); // start iteration at back of chain
    
    // set the tuple to the back of the chain (i.e. the newest)
    TableTuple tuple = table->tempTuple();
    tuple.move(table->dataPtrForTuple(current_tuple_id));
        
    // we're removing the tail  of the chain, i.e. the newest tuple
    if (table->getNewestTupleID() == removal_id) {
                
        if (table->getNumTuplesInEvictionChain() == 1) { // this is the only tuple in the chain
            table->setOldestTupleID(0);
            table->setNewestTupleID(0);
        } else if(table->getNumTuplesInEvictionChain() == 2) {
            table->setNewestTupleID(oldest_tuple_id);
            table->setOldestTupleID(oldest_tuple_id);
        }
        else {
            
            tuple.move(table->dataPtrForTuple(table->getNewestTupleID()));
            
            // we need the previous tuple in the chain, since we're iterating from back to front
            previous_tuple_id = tuple.getPreviousTupleInChain();
            table->setNewestTupleID(previous_tuple_id);
        }
        tuple_found = true;
    }
    
    // we're removing the head of the chain, i.e. the oldest tuple
    if(table->getOldestTupleID() == removal_id && !tuple_found) {
                
        if (table->getNumTuplesInEvictionChain() == 1) { // this is the only tuple in the chain
            table->setOldestTupleID(0);
            table->setNewestTupleID(0);
        }
        else if(table->getNumTuplesInEvictionChain() == 2) {
            table->setNewestTupleID(table->getNewestTupleID());
            table->setOldestTupleID(table->getNewestTupleID());
        }
        else {
            
            tuple.move(table->dataPtrForTuple(table->getOldestTupleID()));
            
            next_tuple_id = tuple.getNextTupleInChain();
            table->setOldestTupleID(next_tuple_id);
        }
        tuple_found = true;
    }
    
    int iterations = 0;
    while(!tuple_found && iterations < table->getNumTuplesInEvictionChain()) {
                
        if(current_tuple_id == oldest_tuple_id)
            break;
        
        // we've found the tuple we want to remove
        if (current_tuple_id == removal_id) {
            next_tuple_id = tuple.getPreviousTupleInChain();
            
            // point previous tuple in chain to next tuple
            tuple.move(table->dataPtrForTuple(previous_tuple_id));
            tuple.setPreviousTupleInChain(next_tuple_id);
            
            // point next tuple in chain to previous tuple
            tuple.move(table->dataPtrForTuple(next_tuple_id));
            tuple.setNextTupleInChain(previous_tuple_id);
            
            tuple_found = true;
            break;
        }
        
        // advance pointers
        previous_tuple_id = current_tuple_id;
        current_tuple_id = tuple.getPreviousTupleInChain(); // iterate back to front
        tuple.move(table->dataPtrForTuple(current_tuple_id));
        
        iterations++;
    }
    
    if (tuple_found) {
        tuples_in_chain = table->getNumTuplesInEvictionChain(); 
        --tuples_in_chain;
        table->setNumTuplesInEvictionChain(tuples_in_chain);
        
        return true;
    }
    
    return false; 
}
    
bool AntiCacheEvictionManager::removeTupleSingleLinkedList(PersistentTable* table, uint32_t removal_id) {
    bool tuple_found = false; 
    
    int tuples_in_chain; 
    
    // ids for iterating through the list
    uint32_t current_tuple_id;
    uint32_t previous_tuple_id;
    uint32_t next_tuple_id;
    uint32_t newest_tuple_id;
    
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
    
    // we're removing the head  of the chain, i.e. the oldest tuple
    if (table->getOldestTupleID() == removal_id) {
        //VOLT_INFO("Removing the first tuple in the eviction chain."); 
        if (table->getNumTuplesInEvictionChain() == 1) { // this is the only tuple in the chain
            table->setOldestTupleID(0); 
            table->setNewestTupleID(0); 
        } else {
            next_tuple_id = tuple.getNextTupleInChain();
            table->setOldestTupleID(next_tuple_id); 
        }
        tuple_found = true; 
    }

    int iterations = 0; 
    while(!tuple_found && iterations < table->getNumTuplesInEvictionChain()) {        
        // we've found the tuple we want to remove
        if (current_tuple_id == removal_id) {
            next_tuple_id = tuple.getNextTupleInChain(); 
            
            // create a tuple from the previous tuple id in the chain
            tuple.move(table->dataPtrForTuple(previous_tuple_id)); 
            
            // set the previous tuple to point to the next tuple
            tuple.setNextTupleInChain(next_tuple_id); 
            
            tuple_found = true; 
            break; 
        }
        
        // advance pointers
        previous_tuple_id = current_tuple_id; 
        current_tuple_id = tuple.getNextTupleInChain(); 
        tuple.move(table->dataPtrForTuple(current_tuple_id));
        
        iterations++; 
    }
    
    if (current_tuple_id == newest_tuple_id && !tuple_found) { // we are at the back of the chain
        if (current_tuple_id == removal_id) { // we're removing the back of the chain
            // set the previous tuple pointer to 0 since it is now the back of the chain
            tuple.move(table->dataPtrForTuple(previous_tuple_id)); 
            tuple.setNextTupleInChain(0);
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
    
// -----------------------------------------
// Debugging Unility Methods
// -----------------------------------------
    
void AntiCacheEvictionManager::printLRUChain(PersistentTable* table, int max, bool forward)
{
    VOLT_INFO("num tuples in chain: %d", table->getNumTuplesInEvictionChain());
    VOLT_INFO("oldest tuple id: %u", table->getOldestTupleID());
    VOLT_INFO("newest tuple id: %u", table->getNewestTupleID());
    
    char chain[max * 4];
    int tuple_id;
    TableTuple tuple = table->tempTuple();
    
    if(forward)
        tuple_id = table->getOldestTupleID();
    else
        tuple_id = table->getNewestTupleID();
    
    chain[0] = '\0';
    
    int iterations = 0;
    while(iterations < table->getNumTuplesInEvictionChain() && iterations < max)
    {
        strcat(chain, itoa(tuple_id));
        strcat(chain, " ");
        
        tuple.move(table->dataPtrForTuple(tuple_id));
        
        if(forward)
            tuple_id = tuple.getNextTupleInChain();
        else
            tuple_id = tuple.getPreviousTupleInChain();
        
        iterations++;
    }
    
    VOLT_INFO("LRU CHAIN: %s", chain);
}

char* AntiCacheEvictionManager::itoa(uint32_t i)
{
    static char buf[19 + 2];
    char *p = buf + 19 + 1;     /* points to terminating '\0' */
    
    do {
        *--p = (char)('0' + (i % 10));
        i /= 10;
    }
    while (i != 0);
    
    return p;
}

}

