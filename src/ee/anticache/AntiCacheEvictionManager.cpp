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
#include "common/ValuePeeker.hpp"
#include "common/debuglog.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "anticache/EvictionIterator.h"
#include "boost/timer.hpp"
#include "anticache/EvictedTable.h"
#include "anticache/UnknownBlockAccessException.h"
#include "anticache/FullBackingStoreException.h"
#include "anticache/AntiCacheDB.h"
#include "anticache/BerkeleyAntiCacheDB.h"

#include <string>
#include <vector>
#include <time.h>
#include <stdlib.h>
// FIXME: This is relatively small. 2500 might be a better guess
//#define MAX_EVICTED_TUPLE_SIZE 1060
#define MAX_EVICTED_TUPLE_SIZE 2500

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
    
AntiCacheEvictionManager::AntiCacheEvictionManager(const VoltDBEngine *engine) {
    
    // Initialize readBlocks table
    m_engine = engine;
    this->initEvictResultTable();
    srand((int)time(NULL));
    
    // Initialize EvictedTable Tuple
    m_evicted_schema = TupleSchema::createEvictedTupleSchema();
    m_evicted_tuple = new TableTuple(m_evicted_schema);
    //TupleSchema::freeTupleSchema(evictedSchema);
    
    m_blockable_accesses = true;
    m_numdbs = 0;
    m_migrate = false;


    if (pthread_mutex_init(&lock, NULL) != 0) {
        VOLT_ERROR("Mutex init failed!");
    }

}

AntiCacheEvictionManager::~AntiCacheEvictionManager() {
    delete m_evictResultTable;
    delete m_evicted_tuple;
    TupleSchema::freeTupleSchema(m_evicted_schema);
    
    pthread_mutex_destroy(&lock);
    // int i;
    //for (i = 1; i <= m_numdbs; i++) {
    //    delete m_db_lookup[i];
    //}
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
    if(table->getEvictedTable() == NULL || table->isBatchEvicted())  // no need to maintain chain for non-evictable tables or batch evicted tables
        return true;

#ifndef ANTICACHE_TIMESTAMPS
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
#else
    // set timestamp to the coldest
    tuple->setColdTimeStamp();
#endif
    
    return true; 
}
    
bool AntiCacheEvictionManager::updateTuple(PersistentTable* table, TableTuple* tuple, bool is_insert) {
    
    if (table->getEvictedTable() == NULL || table->isBatchEvicted())  // no need to maintain chain for non-evictable tables or batch evicted tables
        return true; 


#ifndef ANTICACHE_TIMESTAMPS
    int SAMPLE_RATE = 100; // aLRU sampling rate

    int tuples_in_chain;

    uint32_t newest_tuple_id;
    uint32_t update_tuple_id = table->getTupleID(tuple->address()); // scan blocks for this tuple
        
    // this is an update, so we have to remove the previous entry in the chain
    if (!is_insert) {
                        
        if (rand() % SAMPLE_RATE != 0)  
            return true;
        
        assert(table->getNumTuplesInEvictionChain() > 0);
        #ifdef ANTICACHE_REVERSIBLE_LRU
        removeTupleDoubleLinkedList(table, tuple, update_tuple_id);
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
#else
    // set timestamp to the hotest
    TableTuple update_tuple(tuple->address(), table->m_schema);
    update_tuple.setTimeStamp();
#endif
        
    return true; 
}

#ifndef ANTICACHE_TIMESTAMPS
bool AntiCacheEvictionManager::removeTuple(PersistentTable* table, TableTuple* tuple) {
    int current_tuple_id = table->getTupleID(tuple->address());
    
    // the removeTuple() method called is dependent on whether it is a single or double linked list
    #ifdef ANTICACHE_REVERSIBLE_LRU
    return removeTupleDoubleLinkedList(table, tuple, current_tuple_id);
    #else
    return removeTupleSingleLinkedList(table, current_tuple_id);
    #endif
}
    
// for the double linked list we start from the tail of the chain and iterate backwards
bool AntiCacheEvictionManager::removeTupleDoubleLinkedList(PersistentTable* table, TableTuple* tuple_to_remove, uint32_t removal_id) {
    
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

    
    if(!tuple_found)
    {
        previous_tuple_id = tuple_to_remove->getPreviousTupleInChain(); 
        next_tuple_id = tuple_to_remove->getNextTupleInChain(); 

        //VOLT_INFO("previous: %d, removal: %d, next: %d", previous_tuple_id, removal_id, next_tuple_id); 
        //next_tuple_id = tuple.getPreviousTupleInChain();
            
        // point previous tuple in chain to next tuple
        tuple.move(table->dataPtrForTuple(previous_tuple_id));
        tuple.setNextTupleInChain(next_tuple_id);
            
        // point next tuple in chain to previous tuple
        tuple.move(table->dataPtrForTuple(next_tuple_id));
        tuple.setPreviousTupleInChain(previous_tuple_id);
            
        tuple_found = true;
    }
    
    
    /*
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
    */

    //printLRUChain(table, 100, true); 
    //VOLT_INFO("Found tuple in %d iterations.", iterations); 
    
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
#endif

Table* AntiCacheEvictionManager::evictBlock(PersistentTable *table, long blockSize, int numBlocks) {
    int32_t lastTuplesEvicted = table->getTuplesEvicted();
    int32_t lastBlocksEvicted = table->getBlocksEvicted();
    int64_t lastBytesEvicted  = table->getBytesEvicted();
    
    if (evictBlockToDisk(table, blockSize, numBlocks) == false) {
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

bool AntiCacheEvictionManager::evictBlockToDisk(PersistentTable *table, const long block_size, int num_blocks) {
    voltdb::Table* evictedTable = table->getEvictedTable();
    int m_tuplesEvicted = table->getTuplesEvicted();
    int m_blocksEvicted = table->getBlocksEvicted();
    int64_t m_bytesEvicted = table->getBytesEvicted();

    int m_tuplesWritten = table->getTuplesWritten();
    int m_blocksWritten = table->getBlocksWritten();
    int64_t m_bytesWritten = table->getBytesWritten();

    if (evictedTable == NULL) {
        throwFatalException("Trying to evict block from table '%s' before its "\
                            "EvictedTable has been initialized", table->name().c_str());
    }
    VOLT_DEBUG("Evicting a block of size %ld bytes from table '%s' with %d tuples",
               block_size, table->name().c_str(), (int)table->allocatedTupleCount());
    VOLT_DEBUG("%s Table Schema:\n%s",
              evictedTable->name().c_str(), evictedTable->schema()->debug().c_str());

    AntiCacheDB* antiCacheDB;
    int tuple_length = -1;
    bool needs_flush = false;

    #ifdef VOLT_INFO_ENABLED
    int active_tuple_count = (int)table->activeTupleCount();
    #endif

    // Iterate through the table and pluck out tuples to put in our block
    TableTuple tuple(table->m_schema);
    EvictionIterator evict_itr(table);
#ifdef ANTICACHE_TIMESTAMPS
    evict_itr.reserve((int64_t)block_size * num_blocks);
#endif

    for(int i = 0; i < num_blocks; i++)
    {

        // get the AntiCacheDB instance from the executorContext
        // For now use the single AntiCacheDB from PersistentTable but in the future, this 
        // method to get the AntiCacheDB will have to choose which AntiCacheDB from to
        // evict to
        antiCacheDB = table->getAntiCacheDB(chooseDB(block_size, m_migrate));
               
        // get the LS28B and send that to the antiCacheDB
        uint32_t _block_id = antiCacheDB->nextBlockId();

        // find out whether this tier blocks and set a flag (bit 19)
        // then shift 3b for the ACID (8 levels)
        // then shift 28b for the tier-unique block id (TUID)
        int32_t block_id = antiCacheDB->isBlocking();
        block_id = (block_id | ((int32_t)antiCacheDB->getACID() << 1));
        block_id = ((block_id << 28) | (int32_t)_block_id); 


        // create a new evicted table tuple based on the schema for the source tuple
        TableTuple evicted_tuple = evictedTable->tempTuple();
        VOLT_DEBUG("Setting %s tuple blockId %8x at offset %d", evictedTable->name().c_str(), block_id,0);
        evicted_tuple.setNValue(0, ValueFactory::getIntegerValue(block_id));   // Set the ID for this block
        evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(0));          // set the tuple offset of this block

        #ifdef VOLT_INFO_ENABLED
        boost::timer timer;
//        int64_t origEvictedTableSize = evictedTable->activeTupleCount();
        #endif

        //size_t current_tuple_start_position;

        int32_t num_tuples_evicted = 0;
        BerkeleyDBBlock block;
        std::vector<std::string> tableNames;
        tableNames.push_back(table->name());
        block.initialize(block_size, tableNames,
                _block_id,
                num_tuples_evicted);
        int initSize = block.getSerializedSize();

        VOLT_DEBUG("Starting evictable tuple iterator for %s", table->name().c_str());
        while (evict_itr.hasNext() && (block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE < block_size)) {
            if(!evict_itr.next(tuple))
                break;

            // If this is the first tuple, then we need to allocate all of the memory and
            // what not that we're going to need
            if (tuple_length == -1) {
                tuple_length = tuple.tupleLength();
            }

            //current_tuple_start_position = out.position();

            #ifndef ANTICACHE_TIMESTAMPS
            // remove the tuple from the eviction chain
            removeTuple(table, &tuple);
            #endif

            if (tuple.isEvicted()) {
                VOLT_WARN("Tuple %d from %s is already evicted. Skipping",
                          table->getTupleID(tuple.address()), table->name().c_str());
                continue;
            }
            VOLT_TRACE("Evicting Tuple: %s", tuple.debug(table->name()).c_str());
            //tuple.setEvictedTrue();

            // Populate the evicted_tuple with the block id and tuple offset
            // Make sure this tuple is marked as evicted, so that we know it is an evicted
            // tuple as we iterate through the index
            VOLT_TRACE("block id is %d offset is %d for table %s", block_id, block.getSerializedSize() - initSize, table->name().c_str());
            evicted_tuple.setNValue(0, ValueFactory::getIntegerValue(block_id)); // BLOCK ID
            evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(block.getSerializedSize() - initSize)); // OFFSET
            evicted_tuple.setEvictedTrue();
            VOLT_TRACE("EvictedTuple: %s", evicted_tuple.debug(evictedTable->name()).c_str());

            // Then add it to this table's EvictedTable
            const void* evicted_tuple_address = static_cast<EvictedTable*>(evictedTable)->insertEvictedTuple(evicted_tuple);
            VOLT_TRACE("block address is %p", evicted_tuple_address);
            // Change all of the indexes to point to our new evicted tuple
            table->setEntryToNewAddressForAllIndexes(&tuple, evicted_tuple_address, tuple.address());

            block.addTuple(tuple);
            //if (block.getSerializedSize() - initSize - (int32_t)ValuePeeker::peekInteger(evicted_tuple.getNValue(1)) > 1053)
            //    printf("BIG SIZE: %d\n", block.getSerializedSize() - initSize - (int32_t)ValuePeeker::peekInteger(evicted_tuple.getNValue(1)));

            // At this point it's safe for us to delete this mofo
            table->updateStringMemory(- ((int)tuple.getNonInlinedMemorySize()));
            tuple.freeObjectColumns(); // will return memory for uninlined strings to the heap
            table->deleteTupleStorage(tuple);

            num_tuples_evicted++;
            VOLT_TRACE("Added new evicted %s tuple to block #%8x [tuplesEvicted=%d]",
                       table->name().c_str(), block_id, num_tuples_evicted);

        } // WHILE
        VOLT_DEBUG("Finished evictable tuple iterator for %s [tuplesEvicted=%d]",
                   table->name().c_str(), num_tuples_evicted);
        
        // Only write out a bock if there are tuples in it
        if (num_tuples_evicted > 0) {
            std::vector<int> numTuples;
            numTuples.push_back(num_tuples_evicted);
            block.writeHeader(numTuples);
            int64_t bytesWritten = block.getSerializedSize() - initSize;
            
            #ifdef VOLT_INFO_ENABLED
            VOLT_INFO("Evicted %d tuples / %d bytes.", num_tuples_evicted, block.getSerializedSize());
            VOLT_INFO("Eviction Time: %.2f sec", timer.elapsed());
            timer.restart();
            #endif

            long blocksize = block.getSerializedSize();

            char* blockdata = new char[blocksize];
            memcpy(blockdata, block.getSerializedData(), blocksize);
            /*
            for (int i = 0; i < blocksize; i++) {
                printf( "%x", blockdata[i]);
            }
            cout << "\n";
            */
            // TODO: make this look like
            // block.flush();
            //  antiCacheDB->writeBlock(block);
            VOLT_DEBUG("about to write block %x to acid %d", _block_id, antiCacheDB->getACID());
            antiCacheDB->writeBlock(table->name(),
                                    _block_id,
                                    num_tuples_evicted,
                                    blockdata,
                                    blocksize,
                                    num_tuples_evicted
                                    );
            
            // MJG: We need to check whether we're reusing a blockID.

            bool reused = table->removeUnevictedBlockID(block_id);
            if (reused) {
                VOLT_DEBUG("Reusing block_id 0x%x, should be safe", block_id);
            } else {
                VOLT_DEBUG("First time block_id 0x%x has been used", block_id);
            }

            needs_flush = true;

            // store pointer to AntiCacheDB associated with this block
            //m_db_lookup_table.insert(std::pair<uint32_t, AntiCacheDB*>(block_id, antiCacheDB));
            

            // TODO MJG: Do we have to check that insert is valid? Is it better
            // check the return value of the insert or check the map for the
            // block_id already being there? 

            // Update Stats
            m_tuplesEvicted += num_tuples_evicted;
            m_blocksEvicted += 1;
            m_bytesEvicted += bytesWritten;

            m_tuplesWritten += num_tuples_evicted;
            m_blocksWritten += 1;
            m_bytesWritten += bytesWritten;

            table->setTuplesEvicted(m_tuplesEvicted);
            table->setBlocksEvicted(m_blocksEvicted);
            table->setBytesEvicted(m_bytesEvicted);
            table->setTuplesWritten(m_tuplesWritten);
            table->setBlocksWritten(m_blocksWritten);
            table->setBytesWritten(m_bytesWritten);

            #ifdef VOLT_INFO_ENABLED
            VOLT_INFO("AntiCacheDB Time: %.2f sec", timer.elapsed());
            VOLT_INFO("Evicted Block #%x for %s [tuples=%d / size=%ld / tupleLen=%d]",
                      block_id, table->name().c_str(),
                      num_tuples_evicted, m_bytesEvicted, tuple_length);
//            VOLT_INFO("%s EvictedTable [origCount:%ld / newCount:%ld]",
//                      name().c_str(), (long)origEvictedTableSize, (long)evictedTable->activeTupleCount());
            #endif
        } else {
            VOLT_WARN("No tuples were evicted from %s", table->name().c_str());
            break;
        }

    }  // FOR

    if (needs_flush) {
        #ifdef VOLT_INFO_ENABLED
        boost::timer timer;
        #endif

        // Tell the AntiCacheDB to flush our new blocks out to disk
        // This will block until the blocks are safely written
        antiCacheDB->flushBlocks();

        #ifdef VOLT_INFO_ENABLED
        VOLT_INFO("Flush Time: %.2f sec", timer.elapsed());
        #endif
    }

    VOLT_DEBUG("Evicted block to disk...active tuple count difference: %d", (active_tuple_count - (int)table->activeTupleCount()));
    return true;
}

bool AntiCacheEvictionManager::evictBlockToDiskInBatch(PersistentTable *table, PersistentTable *childTable, const long block_size, int num_blocks) {
    voltdb::Table* evictedTable = table->getEvictedTable();
    voltdb::Table* child_evictedTable = childTable->getEvictedTable();
    int m_tuplesEvicted = table->getTuplesEvicted();
    int m_blocksEvicted = table->getBlocksEvicted();
    int64_t m_bytesEvicted = table->getBytesEvicted();

    int m_tuplesWritten = table->getTuplesWritten();
    int m_blocksWritten = table->getBlocksWritten();
    int64_t m_bytesWritten = table->getBytesWritten();

    if (evictedTable == NULL) {
        throwFatalException("Trying to evict block from table '%s' before its "\
                            "EvictedTable has been initialized", table->name().c_str());
    }
    //VOLT_INFO("Evicting a block of size %ld bytes from table '%s' with %d tuples",
    //           block_size, table->name().c_str(), (int)table->allocatedTupleCount());
 //   VOLT_DEBUG("%s Table Schema:\n%s",
 //             evictedTable->name().c_str(), evictedTable->schema()->debug().c_str());

    // get the AntiCacheDB instance from the executorContext
    AntiCacheDB* antiCacheDB;
    int tuple_length = -1;
    bool needs_flush = false;


   // #ifdef VOLT_INFO_ENABLED
    //int active_tuple_count = (int)table->activeTupleCount();
   // #endif

    VOLT_DEBUG("evictBlockInBatch called!!!!!!!!!!!!!!!!!!!!!!!!!\n");

    TableIndex * pkeyIndex = table->primaryKeyIndex();
    int columnIndex = pkeyIndex->getColumnIndices().front();
    TableIndex * foreignKeyIndex = childTable->allIndexes().at(1); // Fix get the foreign key index
    //int foreignKeyIndexColumn = foreignKeyIndex->getColumnIndices().front();
    int child_tuplesEvicted = childTable->getTuplesEvicted();
    int child_blocksEvicted = childTable->getBlocksEvicted();
    int64_t child_bytesEvicted = childTable->getBytesEvicted();

    int child_tuplesWritten = childTable->getTuplesWritten();
    int child_blocksWritten = childTable->getBlocksWritten();
    int64_t child_bytesWritten = childTable->getBytesWritten();

    VOLT_DEBUG("here!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
    // Iterate through the table and pluck out tuples to put in our block
    TableTuple tuple(table->m_schema);
    EvictionIterator evict_itr(table);
    VOLT_DEBUG("here2!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");

#ifdef ANTICACHE_TIMESTAMPS
    // TODO: what should I do with this?
    evict_itr.reserve((int64_t)block_size * num_blocks / 2);
#endif

    for(int i = 0; i < num_blocks; i++)
    {
    //    VOLT_INFO("Printing parent's LRU chain");
   //     this->printLRUChain(table, 4, true);
    //    VOLT_INFO("Printing child's LRU chain");
   //     this->printLRUChain(childTable, 4, true);
        antiCacheDB = table->getAntiCacheDB(chooseDB(block_size, m_migrate));
        // get a unique block id from the executorContext
        antiCacheDB = table->getAntiCacheDB(chooseDB(block_size, m_migrate));
        uint32_t _block_id = antiCacheDB->nextBlockId();
        // find out whether this tier blocks and set a flag (bit 31)
        // then shift 3b for the ACID (8 levels)
        // then shift 28b for the tier-unique block id (TUID)
        int32_t block_id = antiCacheDB->isBlocking();
        block_id = block_id | ((int32_t)antiCacheDB->getACID() << 1);
        block_id = ((block_id << 28) | (int32_t)_block_id); 

        // create a new evicted table tuple based on the schema for the source tuple
        TableTuple evicted_tuple = evictedTable->tempTuple();
   //     VOLT_DEBUG("Setting %s tuple blockId at offset %d", evictedTable->name().c_str(), 0);
        evicted_tuple.setNValue(0, ValueFactory::getIntegerValue(block_id));   // Set the ID for this block
        evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(0));          // set the tuple offset of this block

       // #ifdef VOLT_INFO_ENABLED
      //  boost::timer timer;
       // #endif



        int32_t num_tuples_evicted = 0;
        std::vector<std::string> tableNames;
        tableNames.push_back(table->name());
        tableNames.push_back(childTable->name());
        BerkeleyDBBlock block;
        block.initialize(block_size, tableNames,
                _block_id,
                num_tuples_evicted);
        int initSize = block.getSerializedSize();

        VOLT_DEBUG("Starting evictable tuple iterator for %s", table->name().c_str());
        int64_t childBytes = 0;
        int32_t childTuples = 0;
        int64_t parentBytes = 0;
        int32_t parentTuples = 0;
        std::vector<TableTuple> childTuplesToBeEvicted;
        int childTuplesSize = 0;
        while (evict_itr.hasNext()) {
            if(!evict_itr.next(tuple))
                break;

            // If this is the first tuple, then we need to allocate all of the memory and
            // what not that we're going to need
            if (tuple_length == -1) {
                tuple_length = tuple.tupleLength();
            }

            // value of the foreign key column
            int64_t pkeyValue = ValuePeeker::peekBigInt(tuple.getNValue(columnIndex));
            VOLT_INFO("after peek !!!! %lld", (long long)pkeyValue);
            vector<ValueType> keyColumnTypes(1, VALUE_TYPE_BIGINT);
            vector<int32_t>
                keyColumnLengths(1, NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
            vector<bool> keyColumnAllowNull(1, true);
            TupleSchema* keySchema =
                TupleSchema::createTupleSchema(keyColumnTypes,
                        keyColumnLengths,
                        keyColumnAllowNull,
                        true);
            TableTuple searchkey(keySchema);
            VOLT_INFO("after search key init !!!!");
            searchkey.move(new char[searchkey.tupleLength()]);
            VOLT_INFO("after search key memory init !!!!");
            searchkey.setNValue(0, ValueFactory::getBigIntValue(pkeyValue));
            VOLT_INFO("after search key set value !!!! %lld",(long long)(ValuePeeker::peekBigInt(ValueFactory::getBigIntValue(pkeyValue))));
            bool found = foreignKeyIndex->moveToKey(&searchkey);
            VOLT_INFO("found child tuples!!!! %d", found);

            TableTuple childTuple(childTable->m_schema);
            std::vector<TableTuple> buffer;
            bool nomore = false;
            if(found){
                while (!(childTuple = foreignKeyIndex->nextValueAtKey()).isNullTuple())
                {
                    childTuplesSize+= MAX_EVICTED_TUPLE_SIZE;
                    if(block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize >= block_size){
                        VOLT_INFO("Size of block exceeds!!in child %d", block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize);
                        nomore = true;
                        break;
                    }
                    buffer.push_back(childTuple);
                }
            }
            if(nomore){
                break;
            }
            for (std::vector<TableTuple>::iterator it = buffer.begin() ; it != buffer.end(); ++it){
                childTuplesToBeEvicted.push_back(*it);
                VOLT_DEBUG("Chind tuple to be evicted: %p", (*it).address());
            }
            if(block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize >= block_size){
                VOLT_INFO("Size of block exceeds!! %d", block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize);
                break;
            }
            parentTuples++;

#ifndef ANTICACHE_TIMESTAMPS
            // remove the tuple from the eviction chain
            removeTuple(table, &tuple);
#endif
            if (tuple.isEvicted()) {
                VOLT_INFO("Tuple %d is already evicted. Skipping", table->getTupleID(tuple.address()));
                continue;
            }

            VOLT_INFO("Evicting Tuple: %s", tuple.debug(table->name()).c_str());
            tuple.setEvictedTrue();

            // Populate the evicted_tuple with the block id and tuple offset
            // Make sure this tuple is marked as evicted, so that we know it is an evicted
            // tuple as we iterate through the index
            evicted_tuple.setNValue(0, ValueFactory::getIntegerValue(block_id));
            evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(num_tuples_evicted));
            evicted_tuple.setEvictedTrue();
            //VOLT_INFO("EvictedTuple: %s", evicted_tuple.debug(evictedTable->name()).c_str());

            // Then add it to this table's EvictedTable
            const void* evicted_tuple_address = static_cast<EvictedTable*>(evictedTable)->insertEvictedTuple(evicted_tuple);

            // Change all of the indexes to point to our new evicted tuple
            table->setEntryToNewAddressForAllIndexes(&tuple, evicted_tuple_address, tuple.address());

            block.addTuple(tuple);

            // At this point it's safe for us to delete this mofo
            tuple.freeObjectColumns(); // will return memory for uninlined strings to the heap
            table->deleteTupleStorage(tuple);


            num_tuples_evicted++;
            VOLT_DEBUG("Added new evicted %s tuple to block #%x [tuplesEvicted=%d]",
                    table->name().c_str(), block_id, num_tuples_evicted);
            if(block.getSerializedSize() + childTuplesSize >= block_size){
                break;
            }

        } // WHILE
        parentBytes = block.getSerializedSize() - initSize;
        // iterate through the child tuples now
        ////////////////BEGIN CHILD TUPLE ADDING TO BLOCK/////////////////////
        for (std::vector<TableTuple>::iterator it = childTuplesToBeEvicted.begin() ; it != childTuplesToBeEvicted.end(); ++it){
            TableTuple childTuple(childTable->m_schema);
            TableTuple child_evicted_tuple = child_evictedTable->tempTuple();
            VOLT_INFO("Setting %s tuple blockId at offset %d", child_evictedTable->name().c_str(), 0);
            child_evicted_tuple.setNValue(0, ValueFactory::getIntegerValue(block_id));   // Set the ID for this block
            child_evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(0));          // set the tuple offset of this block

            childTuple = *it;
            num_tuples_evicted++;
            //removeTuple(childTable, &childTuple);
            childTuple.setEvictedTrue();

            // Populate the evicted_tuple with the block id and tuple offset
            // Make sure this tuple is marked as evicted, so that we know it is an evicted
            // tuple as we iterate through the index
            child_evicted_tuple.setNValue(0, ValueFactory::getIntegerValue(block_id));
            child_evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(childTuples));
            child_evicted_tuple.setEvictedTrue();
            VOLT_INFO("EvictedTuple: %s", child_evicted_tuple.debug(child_evictedTable->name()).c_str());

            // Then add it to this table's EvictedTable
            const void* evicted_tuple_address = static_cast<EvictedTable*>(child_evictedTable)->insertEvictedTuple(child_evicted_tuple);

            // Change all of the indexes to point to our new evicted tuple
            childTable->setEntryToNewAddressForAllIndexes(&childTuple, evicted_tuple_address, childTuple.address());

            //VOLT_INFO("tuple foreign key id %d", ValuePeeker::peekAsInteger(childTuple.getNValue(foreignKeyIndexColumn)));
            VOLT_INFO("EvictedTuple: %s", childTuple.debug(childTable->name()).c_str());
            block.addTuple(childTuple);
            //write out to block
            VOLT_DEBUG("Before freeObjectColumns");
            childTuple.freeObjectColumns();
            VOLT_DEBUG("Before deleteTupleStorage");
            childTable->deleteTupleStorage(childTuple);
            VOLT_DEBUG("Finish evicting a child!");

            childTuples++;


        }
        childBytes = block.getSerializedSize() - parentBytes - initSize;
        ////////////////END CHILD TUPLE ADDING TO BLOCK/////////////////////

        VOLT_DEBUG("Finished evictable tuple iterator for %s [tuplesEvicted=%d]",
                table->name().c_str(), num_tuples_evicted);
        //         VOLT_INFO("Printing parent's LRU chain");
        //         this->printLRUChain(table, 4, true);
        //         VOLT_INFO("Printing child's LRU chain");
        //         this->printLRUChain(childTable, 4, true);

        std::vector<int> numTuples;
        numTuples.push_back(parentTuples);
        numTuples.push_back(childTuples);
        block.writeHeader(numTuples);

        //#ifdef VOLT_INFO_ENABLED
        // VOLT_DEBUG("Evicted %d tuples / %d bytes.", num_tuples_evicted, block.getSerializedSize());
        // VOLT_DEBUG("Eviction Time: %.2f sec", timer.elapsed());
        // timer.restart();
        // #endif

        // Only write out a bock if there are tuples in it
        if (num_tuples_evicted > 0) {
            // TODO: make this look like
            //          block.flush();
            //          antiCacheDB->writeBlock(block);


            antiCacheDB->writeBlock(table->name(),
                    _block_id,
                    num_tuples_evicted,
                    block.getSerializedData(),
                    block.getSerializedSize(),
                    num_tuples_evicted
                    );
            needs_flush = true;


            // store pointer to AntiCacheDB associated with this block
            //m_db_lookup_table.insert(std::pair<uint32_t, AntiCacheDB*>(block_id, antiCacheDB));

            // Update Stats
            m_tuplesEvicted += num_tuples_evicted - childTuples;
            m_blocksEvicted += 1;
            m_bytesEvicted += parentBytes;

            m_tuplesWritten += num_tuples_evicted - childTuples;
            m_blocksWritten += 1;
            m_bytesWritten += parentBytes;

            table->setTuplesEvicted(m_tuplesEvicted);
            table->setBlocksEvicted(m_blocksEvicted);
            table->setBytesEvicted(m_bytesEvicted);
            table->setTuplesWritten(m_tuplesWritten);
            table->setBlocksWritten(m_blocksWritten);
            table->setBytesWritten(m_bytesWritten);

            // child table stats
            child_tuplesEvicted += childTuples;
            child_blocksEvicted += 1;
            child_bytesEvicted += childBytes;

            child_tuplesWritten += childTuples;
            child_blocksWritten += 1;
            child_bytesWritten += childBytes;

            childTable->setTuplesEvicted(child_tuplesEvicted);
            childTable->setBlocksEvicted(child_blocksEvicted);
            childTable->setBytesEvicted(child_bytesEvicted);
            childTable->setTuplesWritten(child_tuplesWritten);
            childTable->setBlocksWritten(child_blocksWritten);
            childTable->setBytesWritten(child_bytesWritten);

       //     #ifdef VOLT_INFO_ENABLED
        //    VOLT_INFO("AntiCacheDB Time: %.2f sec", timer.elapsed());
        //    VOLT_INFO("Evicted Block #%d for %s [tuples=%d / size=%ld / tupleLen=%d]",
        //              block_id, table->name().c_str(),
        //              num_tuples_evicted, m_bytesEvicted, tuple_length);
//            VOLT_INFO("%s EvictedTable [origCount:%ld / newCount:%ld]",
//                      name().c_str(), (long)origEvictedTableSize, (long)evictedTable->activeTupleCount());
         //   #endif
        } else {
            VOLT_WARN("No tuples were evicted from %s", table->name().c_str());
        }

    }  // FOR

    if (needs_flush) {
        //     #ifdef VOLT_INFO_ENABLED
        //   boost::timer timer;
        //    #endif

        // Tell the AntiCacheDB to flush our new blocks out to disk
        // This will block until the blocks are safely written
        antiCacheDB->flushBlocks();

        //  #ifdef VOLT_INFO_ENABLED
        // VOLT_INFO("Flush Time: %.2f sec", timer.elapsed());
        //  #endif
    }

    // VOLT_INFO("Evicted block to disk...active tuple count difference: %d", (active_tuple_count - (int)table->activeTupleCount()));
    return true;
}

Table* AntiCacheEvictionManager::evictBlockInBatch(PersistentTable *table, PersistentTable *childTable,  long blockSize, int numBlocks) {
    int32_t lastTuplesEvicted = table->getTuplesEvicted();
    int32_t lastBlocksEvicted = table->getBlocksEvicted();
    int64_t lastBytesEvicted  = table->getBytesEvicted();
    int32_t childLastTuplesEvicted = childTable->getTuplesEvicted();
    int32_t childLastBlocksEvicted = childTable->getBlocksEvicted();
    int64_t childLastBytesEvicted  = childTable->getBytesEvicted();

    if (evictBlockToDiskInBatch(table, childTable, blockSize, numBlocks) == false) {
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

    int32_t childTuplesEvicted = childTable->getTuplesEvicted() - childLastTuplesEvicted;
    int32_t childBlocksEvicted = childTable->getBlocksEvicted() - childLastBlocksEvicted;
    int64_t childBytesEvicted = childTable->getBytesEvicted() - childLastBytesEvicted;

    idx = 0;
    tuple.setNValue(idx++, ValueFactory::getStringValue(childTable->name()));
    tuple.setNValue(idx++, ValueFactory::getIntegerValue(static_cast<int32_t>(childTuplesEvicted)));
    tuple.setNValue(idx++, ValueFactory::getIntegerValue(static_cast<int32_t>(childBlocksEvicted)));
    tuple.setNValue(idx++, ValueFactory::getBigIntValue(static_cast<int32_t>(childBytesEvicted)));
    m_evictResultTable->insertTuple(tuple);

    return (m_evictResultTable);
}

// Table* AntiCacheEvictionManager::readBlocks(PersistentTable *table, int numBlocks, int16_t blockIds[], int32_t tuple_offsets[]) {
//     
//     #ifdef VOLT_INFO_ENABLED
//     std::ostringstream buffer;
//     for(int i = 0; i < numBlocks; i++) {
//         if (i > 0) buffer << ", ";
//         buffer << blockIds[i];
//     }
//     VOLT_INFO("Preparing to read %d evicted blocks: [%s]", numBlocks, buffer.str().c_str());
//     #endif
// 
//     for(int i = 0; i < numBlocks; i++)
//         readEvictedBlock(table, blockIds[i], tuple_offsets[i]);
// 
//     return (m_readResultTable);
// }
    
bool AntiCacheEvictionManager::readEvictedBlock(PersistentTable *table, int32_t block_id, int32_t tuple_offset) {

    int already_unevicted = table->isAlreadyUnEvicted(block_id);
    if (already_unevicted && table->mergeStrategy()) { // this block has already been read
        VOLT_WARN("Block %d has already been read.", block_id);
        return true;
    }

    /*
     * Finds the AntiCacheDB* instance associated with the needed block_id
     */
    uint32_t _block_id = (uint32_t)(block_id & 0x0FFFFFFF);
    int16_t ACID = (int16_t)((block_id & 0xE0000000) >> 29);
    bool blocking = (bool)((block_id & 0x10000000) >> 28);
    //VOLT_ERROR("anticacheReadBlock block_id: %8x ACID: %d _block_id: %d blocking: %d\n", block_id, ACID, _block_id, (int)blocking);

    AntiCacheDB* antiCacheDB = m_db_lookup[ACID]; 

    if (!antiCacheDB->validateBlock(_block_id)) {
        // TODO:This is a hack!!
        if (_block_id >= antiCacheDB->nextBlockId()) {
            throw UnknownBlockAccessException(_block_id);
            return false;
        }

        VOLT_ERROR("Block %d has already been read from another table.", block_id);
        return true;
    }

    if (already_unevicted) { // this block has already been read, but it is tuple-merge strategy
        /* This is a HACK!! CHANGE LATER!!
        for (int k = 0; k < (int)table->unevictedBlocksSize(); ++k) {
            if (table->getMergeTupleOffset(k) == tuple_offset) {
                return true;
            }
        }*/

        table->insertUnevictedBlock(table->getUnevictedBlocks(already_unevicted - 1));
        table->insertTupleOffset(tuple_offset);
        table->insertBlockID(block_id);

        antiCacheDB->removeSingleTupleStats(_block_id, 1);

        VOLT_DEBUG("BLOCK %u TUPLE %d - unevicted blocks size is %d",
                block_id, tuple_offset, (int)table->unevictedBlocksSize());

        return true;
    }

    // garbage, remove later MJG
    if (blocking != antiCacheDB->isBlocking()) {
        VOLT_WARN("blocking != antiCacheDB->isBlocking(). Investigate!");
    }

    /*std::map<int16_t, AntiCacheDB*>::iterator it = m_db_lookup_table.find(block_id);
    if (it == m_db_lookup_table.end()) {
        VOLT_WARN("Block %d not found in db_lookup_table.", it->first);
        // TODO MJG: should this be a different kind of Exception to throw?
        throw UnknownBlockAccessException(block_id);
        return false;
    } else {
        VOLT_DEBUG("Found block %d in db_lookup_table.", it->first);
        antiCacheDB = it->second;
    }
    */

    //AntiCacheDB* antiCacheDB = table->getAntiCacheDB();

    try {
        VOLT_DEBUG("BLOCK %u %d - unevicted blocks size is %d - alreadyUevicted %d",
                   _block_id, block_id, static_cast<int>(table->unevictedBlocksSize()), already_unevicted);
        AntiCacheBlock* value = antiCacheDB->readBlock(_block_id, 0);

        // allocate the memory for this block
        char* unevicted_tuples = new char[value->getSize()];
        memcpy(unevicted_tuples, value->getData(), value->getSize());
        /*
        for (int i = 0; i < 200; i++) {
            printf( "%X", unevicted_tuples[i]);
        }
        cout << "\n";*/
        VOLT_INFO("***************** READ EVICTED BLOCK %d *****************", _block_id);
        VOLT_INFO("Block Size = %d / Table = %s", value->getSize(), table->name().c_str());
        ReferenceSerializeInput in(unevicted_tuples, value->getSize());
        
        // Read in all the block meta-data
        int num_tables = in.readInt();
        VOLT_DEBUG("num tables is %d", num_tables);
        std::vector<std::string> tableNames;
        std::vector<int> numTuples;
        for(int j = 0; j < num_tables; j++){
            std::string name = in.readTextString();
            tableNames.push_back(name);
            VOLT_DEBUG("tableName is %s", name.c_str());
            int tuples = in.readInt();
            numTuples.push_back(tuples);
            VOLT_DEBUG("num tuples is %d", tuples);
        }

        table->insertUnevictedBlock(unevicted_tuples);
        table->insertTupleOffset(tuple_offset);
        table->insertBlockID(block_id);


        table->insertUnevictedBlockID(std::pair<int32_t,int32_t>(block_id, table->unevictedBlocksSize()));
        VOLT_DEBUG("after insert: alreadyUnevicted %d - IDs size %ld", table->isAlreadyUnEvicted(block_id), table->getUnevictedBlockIDs().size());
        
        VOLT_DEBUG("BLOCK %u TUPLE %d - unevicted blocks size is %d",
                block_id, tuple_offset, static_cast<int>(table->unevictedBlocksSize()));

        delete value;
    } catch (UnknownBlockAccessException e) {
        throw e;

        VOLT_INFO("UnknownBlockAccessException caught.");
        return false;
    }
    

    //    VOLT_INFO("blocks read: %d", m_blocksRead);
    return true;
}


// stub method that may either be implemented by plug in policies
// or via class inheritance.

int AntiCacheEvictionManager::chooseDB() {
    return 0;
}

// A version of chooseDB that takes blockSize. This will allow us to check if
// a block will fit.

int AntiCacheEvictionManager::chooseDB(long blockSize) {
    int i;
    AntiCacheDB* acdb;
    for (i = 0; i < m_numdbs; i++) {
        acdb = m_db_lookup[i];
        if (acdb->getBlockSize() < blockSize) {
            VOLT_DEBUG("blockSize %ld larger than database %d's block size %ld.",
                    blockSize, i, acdb->getBlockSize());
            continue;
        }

        if (acdb->getFreeBlocks() < 1) {
            VOLT_INFO("AntiCacheDB ACID: %d has %d free blocks", i, acdb->getFreeBlocks());
            VOLT_DEBUG("maxBlocks: %d maxDBSize: %ld numBlocks %d",
                    acdb->getMaxBlocks(), acdb->getMaxDBSize(), acdb->getNumBlocks());
            continue;
        }
        return acdb->getACID();
    }
    throwFatalException("Cannot find free space in %d levels for blocksize: %ld",
            i, blockSize);
    return -1;
}

/*
 * A version of chooseDB that initiates an LRU migrate to the next lower level
 */

int AntiCacheEvictionManager::chooseDB(long blockSize, bool migrate) {
    AntiCacheDB* acdb;
    
    if (migrate) {
        for (int i = 0; i < m_numdbs - 1; i++) {
            acdb = m_db_lookup[i];
            
            // first let's check if the block fits. If it doesn't... I don't know, keep
            // going
            if (acdb->getBlockSize() < blockSize) {
                VOLT_ERROR("blockSize %ld larger than database %d's block size %ld.",
                        blockSize, i, acdb->getBlockSize());
                continue;
            }

            if (acdb->getFreeBlocks() < 1) {
                VOLT_DEBUG("AntiCacheDB ACID: %d has %d free blocks", i, acdb->getFreeBlocks());
                VOLT_DEBUG("maxBlocks: %d maxDBSize: %ld numBlocks %d",
                    acdb->getMaxBlocks(), acdb->getMaxDBSize(), acdb->getNumBlocks());
                AntiCacheDB* dst_acdb = m_db_lookup[i+1];
                int32_t new_block_id = migrateLRUBlock(acdb, dst_acdb);
                if (new_block_id == -1) {
                    // If we fail to migrate, an exception should have been thrown 
                    // before this so this should never happen. But ya never know
                    VOLT_ERROR("Failed to migrate! Shouldn't get here");
                }
            }
            return acdb->getACID();
        }
    } else { // Just find a new place for it in a lower tier, no migration
        return chooseDB(blockSize);
    }
    throwFatalException("Cannot find free space for migrate");
    return -1;
}
/*
 * Function to move a block between DBs. This will take a source and destination and 
 * return the new blockId. It first checks to see if there is room in the destination
 * AntiCacheDB. If there is an error, the function will return -1. 
 *
 * TODO MJG: We probably need to throw a NoFreeDBSpace Exception or something instead of just
 * returning -1.
 *
 * In the future, it might make sense to allow for the return of a list of new tuple 
 * mappings so that blocks could be split or merged depending on the underlying 
 * physical medium
 *
 */

int32_t AntiCacheEvictionManager::migrateBlock(int32_t block_id, AntiCacheDB* dstDB) {
    uint32_t _new_block_id = 0;
    int16_t new_acid;
    int32_t new_block_id = 0;

    if (dstDB->getFreeBlocks() == 0) {
        VOLT_WARN("Our destination is full!");
        throw FullBackingStoreException((uint32_t)block_id, (uint32_t)dstDB->getACID());
    }
    
    uint32_t _block_id = (uint32_t)(block_id & 0x0FFFFFFF);
    int16_t acid = (int16_t)((block_id & 0xE0000000) >> 29);
    bool blocking = (bool)((block_id & 0x10000000) >> 28);
    AntiCacheDB* srcDB = m_db_lookup[acid];

    // garbage. remove later MJG
    if (blocking != srcDB->isBlocking()) {
        VOLT_WARN("blocking != srcDB->isBlocking(). Investigate!");
    }

    VOLT_TRACE("source: block_id: 0x%x _block_id: 0x%x acid: 0x%x blocking: %d",
            block_id, _block_id, acid, (int)blocking);
    VOLT_WARN("If you're using tuple merge, this is BROKEN BROKEN BROKEN");
    AntiCacheBlock* block = srcDB->readBlock(_block_id, 1);    
    //VOLT_DEBUG("oldname: %s\n", block->getTableName().c_str());
    _new_block_id = dstDB->nextBlockId();
    
    //VOLT_DEBUG("tablename: %s newBlockId: %d, data: %s, size: %ld\n", block->getTableName().c_str(), newBlockId,
    //        block->getData(), block->getSize());
    dstDB->writeBlock(block->getTableName(), _new_block_id, 0, block->getData(),
            block->getSize(), 0);

    new_acid = dstDB->getACID();

    new_block_id = (int32_t) _new_block_id;
    new_block_id = new_block_id | ((int32_t)new_acid << 29);
    new_block_id = new_block_id | ((int32_t)dstDB->isBlocking() << 28);

    VOLT_ERROR("block_id: 0x%x _block_id: 0x%x acid: 0x%x blocking: %d new_block_id: 0x%x _new_block_id: 0x%x new_acid: 0x%x new_blocking: %d",
            block_id, _block_id, acid, srcDB->isBlocking(), new_block_id, _new_block_id, new_acid, dstDB->isBlocking());

    std::string tableName = block->getTableName();
    PersistentTable *table = dynamic_cast<PersistentTable*>(m_engine->getTable(tableName));
    if (table) {
        EvictedTable *etable = dynamic_cast<EvictedTable*>(table->getEvictedTable());
        if (etable) {
            TableTuple tuple(etable->m_schema);

            voltdb::TableIterator it(etable);
            unsigned int updated = 0;
            while (it.next(tuple)) {
                if ((int32_t)ValuePeeker::peekInteger(tuple.getNValue(0)) == block_id) {
                    tuple.setNValue(0, ValueFactory::getIntegerValue(new_block_id));
                    VOLT_DEBUG("Updating tuple blockid from %8x to %8x", block_id, new_block_id);
                    ++updated;
                }
            }
            VOLT_INFO("updated %u migrated tuples [#%8x -> #%8x]", updated, block_id, new_block_id);
        } else {
            VOLT_WARN("No evicted table! If this is an EE test, shouldn't be a problem");
        }
    } else {
        VOLT_WARN("No persistent table! If this is an EE test, shouldn't be a problem");
    }

    delete block;
    return new_block_id;
}

/*
 * Get the LRU block from the source AntiCacheDB and move it to the destination
 * AntiCacheDB. It first checks if there is room in the destination AntiCacheDB.
 * If there is an error, the function will return -1. 
 * 
 * TODO MJG: We probably need to throw a NoFreeDBSpace Exception or something instead of just
 * returning -1.
 */

int32_t AntiCacheEvictionManager::migrateLRUBlock(AntiCacheDB* srcDB, AntiCacheDB* dstDB) {
    uint32_t _new_block_id = -1;
    int16_t new_acid;
    int32_t new_block_id = 0;

    if (dstDB->getFreeBlocks() == 0) {
        return (int32_t) _new_block_id;
    }

    AntiCacheBlock* block = srcDB->getLRUBlock();
    uint32_t _block_id = block->getBlockId();
    int32_t block_id = (int32_t)_block_id;
    block_id = ((int32_t)srcDB->getACID() << 29) | block_id;
    block_id = ((int32_t)srcDB->isBlocking() << 28) | block_id;
    _new_block_id = dstDB->nextBlockId();

    int tupleInBlock = srcDB->getTupleInBlock(_block_id);
    int evictedTupleInBlock = srcDB->getEvictedTupleInBlock(_block_id);

    VOLT_DEBUG("migrating LRU block   tuples: %d   evictedTuples: %d", tupleInBlock, evictedTupleInBlock);
    
    //VOLT_ERROR("block_id: %8x _new_block_id: %8x", block_id, _new_block_id);

    // if we don't get a new block_id, we can at least try to write it back
    // then throw an exception. at this point, it's probably best for it to be fatal
    if (_new_block_id == -1) {
        _new_block_id = srcDB->nextBlockId();
        srcDB->writeBlock(block->getTableName(), _new_block_id, tupleInBlock, block->getData(), block->getSize(), evictedTupleInBlock);
        VOLT_ERROR("No room in the destination backing store!");
        throw FullBackingStoreException((int32_t)_new_block_id, -1);
    }
    
    VOLT_DEBUG("tablename: %s _newBlockId: %d, data: %s, size: %ld\n", block->getTableName().c_str(), _new_block_id,
            block->getData(), block->getSize());
    dstDB->writeBlock(block->getTableName(), _new_block_id, tupleInBlock, block->getData(), block->getSize(), evictedTupleInBlock);
    
    new_acid = dstDB->getACID();
    new_block_id = (int32_t) _new_block_id;
    new_block_id = new_block_id | (new_acid << 29);
    new_block_id = new_block_id | (dstDB->isBlocking() << 28);
    //VOLT_ERROR("new_block_id: 0x%x _new_block_id: 0x%x new_acid: 0x%x blocking: %d",
    //        new_block_id, _new_block_id, new_acid, dstDB->isBlocking());

    std::string tableName = block->getTableName();
    PersistentTable *table = dynamic_cast<PersistentTable*>(m_engine->getTable(tableName));
    if (table) {
        EvictedTable *etable = dynamic_cast<EvictedTable*>(table->getEvictedTable());
        if (etable) {
            TableTuple tuple(etable->m_schema);

            voltdb::TableIterator it(etable);
            unsigned int updated = 0;
            while (it.next(tuple)) {
                if ((int32_t)ValuePeeker::peekInteger(tuple.getNValue(0)) == block_id) {
                    tuple.setNValue(0, ValueFactory::getIntegerValue(new_block_id));
                    ++updated;
                    VOLT_TRACE("Updating tuple blockid from %8x to %8x", block_id, new_block_id);
                }
            }
            VOLT_DEBUG("updated %u migrated tuples [#%8x -> #%8x]", updated, block_id, new_block_id);
        } else {
            VOLT_WARN("No evicted table! If this is an EE test, shouldn't be a problem");
        }
    } else {
        VOLT_WARN("No persistent table! If this is an EE test, shouldn't be a problem");
    }

    // MJG TODO!!!: We can't just delete this block willy nilly if we can't get a new_block_id. 
    // Have to do something better than this. XXX
    delete block;

    return new_block_id;
}

/*
 * use this to add an AntiCacheDB* pointer to the table upon initialization
 * of an AntiCacheDB. It will return the ACID.
 */

int16_t AntiCacheEvictionManager::addAntiCacheDB(AntiCacheDB* acdb) {
    int16_t acid;
    acdb->setACID(m_numdbs);
    //printf("add acdb: %d\n", m_numdbs);
    m_db_lookup[m_numdbs] = acdb;
    acid = m_numdbs;
    m_numdbs++;
    if (m_numdbs > 1) {
        m_migrate = true;
    }
    return acid;
}

/*
 * Get a pointer to the AntiCacheDB identified by given ACID
 */
AntiCacheDB* AntiCacheEvictionManager::getAntiCacheDB(int acid) {
    if (acid >= m_numdbs) {
        VOLT_ERROR("invalid acid: %d/%d", acid, m_numdbs);
        return NULL;
    } else {
        return m_db_lookup[acid];
    }
}    

/*
 * Merges the unevicted block into the regular data table
 */
bool AntiCacheEvictionManager::mergeUnevictedTuples(PersistentTable *table) {
    VOLT_TRACE("in merge");
    int num_blocks = table->unevictedBlocksSize();
    int32_t num_tuples_in_block = -1;

    //    for (std::map<int16_t,int16_t>::iterator it=table->getUnevictedBlockIDs().begin(); it!=table->getUnevictedBlockIDs().end(); ++it)
    //        std::cout << it->first << " => " << it->second << '\n';


    if (num_blocks == 0){
        VOLT_WARN("Trying to merge unevicted blocks for table %s but there aren't any available?",
                  table->name().c_str());
        return (false);
    }

    int32_t merge_tuple_offset = 0; // this is the offset of tuple that caused this block to be unevicted

    DefaultTupleSerializer serializer;
    TableTuple unevictedTuple(table->m_schema);
    TableTuple evicted_tuple = table->getEvictedTable()->tempTuple();

    //int active_tuple_count = (int)table->activeTupleCount();
#ifndef ANTICACHE_TIMESTAMPS
    //int tuples_in_eviction_chain = (int)table->getNumTuplesInEvictionChain();
#endif

#ifdef VOLT_INFO_ENABLED
    VOLT_INFO("Merging %d blocks for table %s.", num_blocks, table->name().c_str());
#endif
    VOLT_INFO("Merging %d blocks for table %s.", num_blocks, table->name().c_str());

    for (int i = 0; i < num_blocks; i++) {
        // XXX: have to put block size, which we don't know, so just put something large, like 10MB
        ReferenceSerializeInput in(table->getUnevictedBlocks(i), 10485760);

        merge_tuple_offset = table->getMergeTupleOffset(i); // what to do about this?
        VOLT_DEBUG("Merge Tuple offset is %d", merge_tuple_offset);

        // Read in all the meta-data
        int num_tables = in.readInt();
        std::vector<std::string> tableNames;
        std::vector<int> numTuples;
        for(int j = 0; j < num_tables; j++){
            tableNames.push_back(in.readTextString());
            numTuples.push_back(in.readInt());
            VOLT_TRACE("%s", tableNames[j].c_str());
        }

        // Get ACDB for this tuple. That is used for correct stats tuple-merge strategy
        int32_t block_id = table->getBlockID(i);
        uint32_t _block_id = (uint32_t)(block_id & 0x0FFFFFFF);
        int16_t ACID = (int16_t)((block_id & 0xE0000000) >> 29);
        VOLT_DEBUG("block_id: %8x ACID: %d _block_id: %d blocking: %d\n", block_id, ACID, _block_id, (int)blocking);
        AntiCacheDB* antiCacheDB = m_db_lookup[ACID]; 

        int count = 0;
        for (std::vector<std::string>::iterator it = tableNames.begin() ; it != tableNames.end(); ++it){
            PersistentTable *tableInBlock = dynamic_cast<PersistentTable*>(m_engine->getTable(*it));
            num_tuples_in_block = numTuples.at(count);
            //VOLT_ERROR("Merging %d tuples.", num_tuples_in_block);

            // Now read the actual tuples
            int64_t bytes_unevicted = 0;
            int tuplesRead = 0;
            if(!table->mergeStrategy()) {
                int64_t current_unevicted = tableInBlock->unevictTuple(&in, merge_tuple_offset, merge_tuple_offset, (bool)table->mergeStrategy());
                bytes_unevicted += current_unevicted;
                if (current_unevicted == 0) {
                    antiCacheDB->removeSingleTupleStats(_block_id, -1);
                    //printf("Add back: %u %u\n", ACID, _block_id);
                }
            } else {
                for (int j = 0; j < num_tuples_in_block; j++)
                {
                // if we're using the tuple-merge strategy, only merge in a single tuple

                // NOTICE: As we handle the problem this way, the unevicted bytes from one block can not exceed MAXINT.
                bytes_unevicted += tableInBlock->unevictTuple(&in, (int)bytes_unevicted, merge_tuple_offset, (bool)table->mergeStrategy());
                /*                // get a free tuple and increment the count of tuples current used
                                  voltdb::TableTuple * m_tmpTarget1 = tableInBlock->getTempTarget1();
                                  tableInBlock->nextFreeTuple(m_tmpTarget1);
                                  tableInBlock->m_tupleCount++;

                // deserialize tuple from unevicted block
                //VOLT_INFO("Before deserialize.%d", tableInBlock->m_tupleCount);
                bytes_unevicted += m_tmpTarget1->deserializeWithHeaderFrom(in);
                m_tmpTarget1->setEvictedFalse();
                m_tmpTarget1->setDeletedFalse();


                // Note, this goal of the section below is to get a tuple that points to the tuple in the EvictedTable and has the
                // schema of the evicted tuple. However, the lookup has to be done using the schema of the original (unevicted) version
                voltdb::TableTuple m_tmpTarget2 = tableInBlock->lookupTuple(*m_tmpTarget1);       // lookup the tuple in the table
                //VOLT_INFO("tuple address is %s", m_tmpTarget2.address());
                evicted_tuple.move(m_tmpTarget2.address());
                static_cast<EvictedTable*>(tableInBlock->getEvictedTable())->deleteEvictedTuple(evicted_tuple);             // delete the EvictedTable tuple

                // update the indexes to point to this newly unevicted tuple
                tableInBlock->setEntryToNewAddressForAllIndexes(m_tmpTarget1, m_tmpTarget1->address());

                m_tmpTarget1->setEvictedFalse();

                // re-insert the tuple back into the eviction chain
                if(j == merge_tuple_offset) { // put it at the back of the chain
                VOLT_INFO("matched ofset");
                updateTuple(tableInBlock, m_tmpTarget1, true);
                }
                else{
                VOLT_INFO("others");
                updateUnevictedTuple(tableInBlock, m_tmpTarget1);
                }
                 */
                }
            }
            if(tableInBlock->mergeStrategy())
                tuplesRead += num_tuples_in_block;
            else
                tuplesRead++;
            int m_tuplesEvicted = tableInBlock->getTuplesEvicted();
            m_tuplesEvicted -= tuplesRead;
            tableInBlock->setTuplesEvicted(m_tuplesEvicted);
            int m_tuplesRead = tableInBlock->getTuplesRead();
            m_tuplesRead += tuplesRead;
            tableInBlock->setTuplesRead(m_tuplesRead);
            tableInBlock->m_bytesEvicted-=bytes_unevicted;
            VOLT_INFO("Bytes unevicted: %ld", long(bytes_unevicted));
            tableInBlock->m_bytesRead+=bytes_unevicted;
            tableInBlock->m_blocksEvicted -= 1;
            tableInBlock->m_blocksRead += 1;

            count++;

        }



        if (table->mergeStrategy())
            delete [] table->getUnevictedBlocks(i);
        //table->clearUnevictedBlocks(i);
    }

    VOLT_DEBUG("unevicted blockIDs size %d", static_cast<int>(table->getUnevictedBlockIDs().size()));
    VOLT_DEBUG("unevicted blocks size %d", static_cast<int>(table->unevictedBlocksSize()));
    if (!table->mergeStrategy()) {
        map <int32_t, int32_t> unevictedBlockIDs = table->getUnevictedBlockIDs();
        for (map <int32_t, int32_t>::iterator itr = unevictedBlockIDs.begin(); itr != unevictedBlockIDs.end();
                itr++) {
            //printf("bid:%d idx:%d\n", itr->first, itr->second);
            delete [] table->getUnevictedBlocks(itr->second - 1);
        }
        table->clearUnevictedBlockIDs();
    }
    //table->clearUnevictedBlockIDs();
    table->clearUnevictedBlocks();
    table->clearMergeTupleOffsets();
    table->clearBlockIDs();
    VOLT_DEBUG("unevicted blockIDs size %d", static_cast<int>(table->getUnevictedBlockIDs().size()));
    VOLT_DEBUG("unevicted blocks size %d", static_cast<int>(table->unevictedBlocksSize()));

    //VOLT_ERROR("Active Tuple Count: %d -- %d", (int)active_tuple_count, (int)table->activeTupleCount());
#ifndef ANTICACHE_TIMESTAMPS
    VOLT_INFO("Tuples in Eviction Chain: %d -- %d", (int)tuples_in_eviction_chain, (int)table->getNumTuplesInEvictionChain());
#endif


    return true;
}
// -----------------------------------------
// Evicted Access Tracking Methods
// -----------------------------------------

void AntiCacheEvictionManager::recordEvictedAccess(catalog::Table* catalogTable, TableTuple *tuple) {
    // FIXME: HACK HACK HACK
    if (m_evicted_block_ids.size() > 100000) {
        return;
    }
    // Make sure that this tuple isn't deleted
    if (tuple->isActive() == false) {
        throwFatalException("Trying to access evicted tuple from table '%s' that is also marked as deleted",
                            catalogTable->name().c_str());
    }
    // Create an evicted tuple from the current tuple address
    // NOTE: This is necessary because the original table tuple and the evicted tuple
    // do not have the same schema
    m_evicted_tuple->move(tuple->address()); 
    VOLT_TRACE("%s",m_evicted_tuple->getSchema()->debug().c_str());
    VOLT_TRACE("moved tuple into m_evicted_tuple. Time to get blockId"); 
    VOLT_TRACE("debug tuple: %s", m_evicted_tuple->debug(catalogTable->name()).c_str());
    // Determine the block id and tuple offset in the block using the EvictedTable tuple
    int32_t tuple_id = peeker.peekInteger(m_evicted_tuple->getNValue(1)); 
    VOLT_TRACE("Got tuple_id: %d", tuple_id);
    int32_t block_id = peeker.peekInteger(m_evicted_tuple->getNValue(0));
    VOLT_DEBUG("Got blockId: 0x%x", block_id);
    // Updated internal tracking info
    if (!(block_id & 0x10000000)) {
        m_evicted_tables.push_back(catalogTable);
        m_evicted_block_ids.push_back(block_id); 
        m_evicted_offsets.push_back(tuple_id);
        m_blockable_accesses = false;
    } else {
        m_evicted_tables_sync.push_back(catalogTable);
        m_evicted_block_ids_sync.push_back(block_id); 
        m_evicted_offsets_sync.push_back(tuple_id);
    }

    /*
    if (m_evicted_filter.find(block_id) != m_evicted_filter.end())
        if (m_evicted_filter[block_id].find(tuple_id) != m_evicted_filter[block_id].end()) {
            VOLT_ERROR("try skipping %d %d", block_id, tuple_id);
            return;
        }

    (m_evicted_filter[block_id]).insert(tuple_id);*/
    //VOLT_ERROR("try reading %d %d", block_id, tuple_id);

    /*
    if (m_evicted_block_ids.size() > 10000 && m_evicted_block_ids.size() % 10000 == 0) {
        VOLT_ERROR("Record evicted tuple access size: %ld", m_evicted_block_ids.size());
    }
    */

    VOLT_DEBUG("Recording evicted tuple access [table=%s / blockId=%d / tupleId=%d /blockable = %d]",
               catalogTable->name().c_str(), block_id, tuple_id, m_blockable_accesses);    
    VOLT_TRACE("Evicted Tuple Acccess: %s", m_evicted_tuple->debug(catalogTable->name()).c_str());
}

void AntiCacheEvictionManager::throwEvictedAccessException() {
    // Do we really want to remove all the non-unique blockIds here?
    // m_evicted_block_ids.unique();
        
    int num_block_ids = static_cast<int>(m_evicted_block_ids.size()) + static_cast<int>(m_evicted_block_ids_sync.size()); 
    assert(num_block_ids > 0); 
    
    VOLT_DEBUG("Txn accessed data from %ld %ld evicted blocks", m_evicted_block_ids.size(), m_evicted_block_ids_sync.size());
        
    int32_t* block_ids = new int32_t[num_block_ids];
    int32_t* tuple_ids = new int32_t[num_block_ids];
        
    // copy the block ids into an array 
    int num_blocks = 0; 
    for(vector<int32_t>::iterator itr = m_evicted_block_ids.begin(); itr != m_evicted_block_ids.end(); ++itr) {
        VOLT_TRACE("Marking block %d as being needed for uneviction", *itr); 
        block_ids[num_blocks++] = *itr; 
    }

    // copy the tuple offsets into an array
    int num_tuples = 0; 
    for(vector<int32_t>::iterator itr = m_evicted_offsets.begin(); itr != m_evicted_offsets.end(); ++itr) {
        VOLT_TRACE("Marking tuple %d from %s as being needed for uneviction", *itr, m_evicted_tables[num_tuples]->name().c_str()); 
        tuple_ids[num_tuples++] = *itr;
    }
        
    // copy the block ids into an array 
    for(vector<int32_t>::iterator itr = m_evicted_block_ids_sync.begin(); itr != m_evicted_block_ids_sync.end(); ++itr) {
        VOLT_TRACE("Marking block %d as being needed for uneviction", *itr); 
        block_ids[num_blocks++] = *itr; 
    }

    // copy the tuple offsets into an array
    for(vector<int32_t>::iterator itr = m_evicted_offsets_sync.begin(); itr != m_evicted_offsets_sync.end(); ++itr) {
        VOLT_TRACE("Marking tuple %d from %s as being needed for uneviction", *itr, m_evicted_tables_sync[num_tuples - 
                (int)m_evicted_block_ids.size()]->name().c_str()); 
        tuple_ids[num_tuples++] = *itr;
    }
    
    // HACK
    catalog::Table *catalogTable = m_evicted_tables.front();

    // Do we really want to throw this here?
    // FIXME We need to support multiple tables in the exception data
    VOLT_DEBUG("Throwing EvictedTupleAccessException for table %s (%d) "
              "[num_blocks=%d / num_tuples=%d]",
              catalogTable->name().c_str(), catalogTable->relativeIndex(),
              num_blocks, num_tuples);
    throw EvictedTupleAccessException(catalogTable->relativeIndex(), num_block_ids, block_ids, tuple_ids);
}

bool AntiCacheEvictionManager::blockingMerge() {

    pthread_mutex_lock(&lock);

    int num_block_ids = static_cast<int>(m_evicted_block_ids_sync.size()); 
    //if (num_block_ids % 10000 == 0)
    VOLT_DEBUG("blockingmerge: %d", num_block_ids);
    assert(num_block_ids > 0); 

    int32_t* block_ids = new int32_t[num_block_ids];
    int32_t* tuple_ids = new int32_t[num_block_ids];

    // copy the block ids into an array 
    int num_blocks = 0; 
    for(vector<int32_t>::iterator itr = m_evicted_block_ids_sync.begin(); itr != m_evicted_block_ids_sync.end(); ++itr) {
        VOLT_TRACE("Marking block 0x%x as being needed for uneviction", *itr); 
        block_ids[num_blocks++] = *itr; 
    }

    // copy the tuple offsets into an array
    int num_tuples = 0; 
    for(vector<int32_t>::iterator itr = m_evicted_offsets_sync.begin(); itr != m_evicted_offsets_sync.end(); ++itr) {
        VOLT_TRACE("Marking tuple %d from %s as being needed for uneviction", *itr, m_evicted_tables_sync[num_tuples]->name().c_str()); 
        tuple_ids[num_tuples++] = *itr;
    }
     // HACK
    catalog::Table *catalogTable = m_evicted_tables_sync.front();

    // MJG: If we have only blockable merges, let's skip throwing the exception and just merge here
    // The process from above is:
    // 1. transaction sees tuples need to be fetched
    // 2. method to call exception is called
    // 3. if tuples are in blocking tier, instead of actually throwing the exception,
    //    read the block and merge the tuples
    // 4. don't throw exception, just return and continue transaction as if nothing has happened
    // 5. ???
    // 6. success

    if (hasBlockableEvictedAccesses()) {
        //VOLT_ERROR("We've only got blocks in blockable tier, so let's block and merge");
        VOLT_DEBUG("Preparing to read %d evicted blocks", num_blocks);
        PersistentTable *table = dynamic_cast<PersistentTable*>(m_engine->getTable(catalogTable->relativeIndex())); 
        
        if (table == NULL) { 
            VOLT_ERROR("bad table, prepare for massive failure");
            throw EvictedTupleAccessException(catalogTable->relativeIndex(), num_block_ids, block_ids, tuple_ids);
        }
        
        bool final_result = true;
        try {
            for(int i = 0; i < num_blocks; i++) {
                final_result = readEvictedBlock(table, block_ids[i], tuple_ids[i]) && final_result;
            }
        } catch (SerializableEEException &e) {
            VOLT_ERROR("blocking read failed to read %d blocks for table '%s'\n%s",
                    num_blocks, table->name().c_str(), e.message().c_str());
        }
        //VOLT_ERROR("We've read blocks, now lets merge them");
        try {
            mergeUnevictedTuples(table);
        } catch (SerializableEEException &e) {
            VOLT_INFO("failed to merge blocks for table %s", table->name().c_str());
        }
        VOLT_DEBUG("blocking merge (possibly?) successful, don't throw exception, just return");

        delete [] block_ids;
        delete [] tuple_ids;
        m_evicted_tables_sync.clear();
        m_evicted_block_ids_sync.clear();
        m_evicted_offsets_sync.clear();
        pthread_mutex_unlock(&lock);
        return true;           
    }
    pthread_mutex_unlock(&lock);
    return false;
}     

#ifndef ANTICACHE_TIMESTAMPS

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
#endif

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

