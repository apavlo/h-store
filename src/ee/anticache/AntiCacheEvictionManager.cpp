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
#include "anticache/AntiCacheDB.h"
#include <string>
#include <vector>
#include <time.h>
#include <stdlib.h>
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
    if(table->getEvictedTable() == NULL || table->isBatchEvicted())  // no need to maintain chain for non-evictable tables or batch evicted tables
        return true;

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
    int SAMPLE_RATE = 1000; // aLRU sampling rate
    
    if(table->getEvictedTable() == NULL || table->isBatchEvicted())  // no need to maintain chain for non-evictable tables or batch evicted tables
        return true; 

//VOLT_INFO("updating LRU for %s", table->name().c_str());    
    int tuples_in_chain;

    uint32_t newest_tuple_id;
    uint32_t update_tuple_id = table->getTupleID(tuple->address()); // scan blocks for this tuple
        
    // this is an update, so we have to remove the previous entry in the chain
    if (!is_insert) {
                        
      if(rand() % SAMPLE_RATE != 0)  
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
        
    return true; 
}
    
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
    voltdb::Table* m_evictedTable = table->getEvictedTable();
    int m_tuplesEvicted = table->getTuplesEvicted();
    int m_blocksEvicted = table->getBlocksEvicted();
    int64_t m_bytesEvicted = table->getBytesEvicted();

    int m_tuplesWritten = table->getTuplesWritten();
    int m_blocksWritten = table->getBlocksWritten();
    int64_t m_bytesWritten = table->getBytesWritten();

    if (m_evictedTable == NULL) {
        throwFatalException("Trying to evict block from table '%s' before its "\
                            "EvictedTable has been initialized", table->name().c_str());
    }
    VOLT_DEBUG("Evicting a block of size %ld bytes from table '%s' with %d tuples",
               block_size, table->name().c_str(), (int)table->allocatedTupleCount());
    VOLT_DEBUG("%s Table Schema:\n%s",
              m_evictedTable->name().c_str(), m_evictedTable->schema()->debug().c_str());

    // get the AntiCacheDB instance from the executorContext
    AntiCacheDB* antiCacheDB = table->getAntiCacheDB();
    int tuple_length = -1;
    bool needs_flush = false;


    #ifdef VOLT_INFO_ENABLED
    int active_tuple_count = (int)table->activeTupleCount();
    #endif

    for(int i = 0; i < num_blocks; i++)
    {

        // get a unique block id from the executorContext
        int16_t block_id = antiCacheDB->nextBlockId();

        // create a new evicted table tuple based on the schema for the source tuple
        TableTuple evicted_tuple = m_evictedTable->tempTuple();
        VOLT_DEBUG("Setting %s tuple blockId at offset %d", m_evictedTable->name().c_str(), 0);
        evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));   // Set the ID for this block
        evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(0));          // set the tuple offset of this block

        // Iterate through the table and pluck out tuples to put in our block
        TableTuple tuple(table->m_schema);
        EvictionIterator evict_itr(table);

        #ifdef VOLT_INFO_ENABLED
        boost::timer timer;
//        int64_t origEvictedTableSize = m_evictedTable->activeTupleCount();
        #endif

        //size_t current_tuple_start_position;

        int32_t num_tuples_evicted = 0;
        BerkeleyDBBlock block;
        std::vector<std::string> tableNames;
        tableNames.push_back(table->name());
        block.initialize(block_size, tableNames,
                block_id,
                num_tuples_evicted);
        int initSize = block.getSerializedSize();

        VOLT_DEBUG("Starting evictable tuple iterator for %s", name().c_str());
        while (evict_itr.hasNext() && (block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE < block_size)) {
            if(!evict_itr.next(tuple))
                break;

            // If this is the first tuple, then we need to allocate all of the memory and
            // what not that we're going to need
            if (tuple_length == -1) {
                tuple_length = tuple.tupleLength();
            }

            //current_tuple_start_position = out.position();

            // remove the tuple from the eviction chain
            removeTuple(table, &tuple);
            if (tuple.isEvicted())
            {
                VOLT_INFO("Tuple %d is already evicted. Skipping", table->getTupleID(tuple.address()));
                continue;
            }
            VOLT_DEBUG("Evicting Tuple: %s", tuple.debug(name()).c_str());
            tuple.setEvictedTrue();

            // Populate the evicted_tuple with the block id and tuple offset
            // Make sure this tuple is marked as evicted, so that we know it is an evicted
            // tuple as we iterate through the index
        VOLT_INFO("block id is %d for table %s", block_id, table->name().c_str());
            evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));
            evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(num_tuples_evicted));
            evicted_tuple.setEvictedTrue();
            VOLT_DEBUG("EvictedTuple: %s", evicted_tuple.debug(m_evictedTable->name()).c_str());

            // Then add it to this table's EvictedTable
            const void* evicted_tuple_address = static_cast<EvictedTable*>(m_evictedTable)->insertEvictedTuple(evicted_tuple);
VOLT_INFO("block address is %p", evicted_tuple_address);
            // Change all of the indexes to point to our new evicted tuple
            table->setEntryToNewAddressForAllIndexes(&tuple, evicted_tuple_address);

            block.addTuple(tuple);

            // At this point it's safe for us to delete this mofo
            tuple.freeObjectColumns(); // will return memory for uninlined strings to the heap
            table->deleteTupleStorage(tuple);

            num_tuples_evicted++;
            VOLT_DEBUG("Added new evicted %s tuple to block #%d [tuplesEvicted=%d]",
                       name().c_str(), block_id, num_tuples_evicted);

        } // WHILE
        VOLT_DEBUG("Finished evictable tuple iterator for %s [tuplesEvicted=%d]",
                   table->name().c_str(), num_tuples_evicted);

        std::vector<int> numTuples;
        numTuples.push_back(num_tuples_evicted);
        block.writeHeader(numTuples);
        int64_t bytesWritten = block.getSerializedSize() - initSize;
        #ifdef VOLT_INFO_ENABLED
        VOLT_DEBUG("Evicted %d tuples / %d bytes.", num_tuples_evicted, block.getSerializedSize());
        VOLT_DEBUG("Eviction Time: %.2f sec", timer.elapsed());
        timer.restart();
        #endif

        // Only write out a bock if there are tuples in it
        if (num_tuples_evicted >= 0) {
// TODO: make this look like
//            block.flush();
//            antiCacheDB->writeBlock(block);
            antiCacheDB->writeBlock(table->name(),
                                    block_id,
                                    num_tuples_evicted,
                                    block.getSerializedData(),
                                    block.getSerializedSize());
            needs_flush = true;

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
            VOLT_INFO("Evicted Block #%d for %s [tuples=%d / size=%ld / tupleLen=%d]",
                      block_id, table->name().c_str(),
                      num_tuples_evicted, m_bytesEvicted, tuple_length);
//            VOLT_INFO("%s EvictedTable [origCount:%ld / newCount:%ld]",
//                      name().c_str(), (long)origEvictedTableSize, (long)m_evictedTable->activeTupleCount());
            #endif
        } else {
            VOLT_WARN("No tuples were evicted from %s", table->name().c_str());
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

    VOLT_INFO("Evicted block to disk...active tuple count difference: %d", (active_tuple_count - (int)table->activeTupleCount()));
    return true;
}

bool AntiCacheEvictionManager::evictBlockToDiskInBatch(PersistentTable *table, PersistentTable *childTable, const long block_size, int num_blocks) {
    voltdb::Table* m_evictedTable = table->getEvictedTable();
    voltdb::Table* child_evictedTable = childTable->getEvictedTable();
    int m_tuplesEvicted = table->getTuplesEvicted();
    int m_blocksEvicted = table->getBlocksEvicted();
    int64_t m_bytesEvicted = table->getBytesEvicted();

    int m_tuplesWritten = table->getTuplesWritten();
    int m_blocksWritten = table->getBlocksWritten();
    int64_t m_bytesWritten = table->getBytesWritten();

    if (m_evictedTable == NULL) {
        throwFatalException("Trying to evict block from table '%s' before its "\
                            "EvictedTable has been initialized", table->name().c_str());
    }
    //VOLT_INFO("Evicting a block of size %ld bytes from table '%s' with %d tuples",
    //           block_size, table->name().c_str(), (int)table->allocatedTupleCount());
 //   VOLT_DEBUG("%s Table Schema:\n%s",
 //             m_evictedTable->name().c_str(), m_evictedTable->schema()->debug().c_str());

    // get the AntiCacheDB instance from the executorContext
    AntiCacheDB* antiCacheDB = table->getAntiCacheDB();
    int tuple_length = -1;
    bool needs_flush = false;


   // #ifdef VOLT_INFO_ENABLED
    //int active_tuple_count = (int)table->activeTupleCount();
   // #endif


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

    for(int i = 0; i < num_blocks; i++)
    {
    //    VOLT_INFO("Printing parent's LRU chain");
   //     this->printLRUChain(table, 4, true);
    //    VOLT_INFO("Printing child's LRU chain");
   //     this->printLRUChain(childTable, 4, true);
        // get a unique block id from the executorContext
        int16_t block_id = antiCacheDB->nextBlockId();

        // create a new evicted table tuple based on the schema for the source tuple
        TableTuple evicted_tuple = m_evictedTable->tempTuple();
   //     VOLT_DEBUG("Setting %s tuple blockId at offset %d", m_evictedTable->name().c_str(), 0);
        evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));   // Set the ID for this block
        evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(0));          // set the tuple offset of this block

        // Iterate through the table and pluck out tuples to put in our block
        TableTuple tuple(table->m_schema);
        EvictionIterator evict_itr(table);

       // #ifdef VOLT_INFO_ENABLED
      //  boost::timer timer;
       // #endif



        int32_t num_tuples_evicted = 0;
        std::vector<std::string> tableNames;
        tableNames.push_back(table->name());
        tableNames.push_back(childTable->name());
        BerkeleyDBBlock block;
        block.initialize(block_size, tableNames,
                block_id,
                num_tuples_evicted);
        int initSize = block.getSerializedSize();

     //   VOLT_DEBUG("Starting evictable tuple iterator for %s", name().c_str());
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
      //      VOLT_INFO("after peek !!!! %lld", (long long)pkeyValue);
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
        //    VOLT_INFO("after search key init !!!!");
            searchkey.move(new char[searchkey.tupleLength()]);
        //    VOLT_INFO("after search key memory init !!!!");
            searchkey.
                setNValue(0, ValueFactory::getBigIntValue(pkeyValue));
        //    VOLT_INFO("after search key set value !!!! %lld",(long long)(ValuePeeker::peekBigInt(ValueFactory::getBigIntValue(pkeyValue))));
            bool found = foreignKeyIndex->moveToKey(&searchkey);
        //    VOLT_INFO("found child tuples!!!! %d", found);

            TableTuple childTuple(childTable->m_schema);
            std::vector<TableTuple> buffer;
            bool nomore = false;
            if(found){
                while (!(childTuple = foreignKeyIndex->nextValueAtKey()).isNullTuple())
                {
                    childTuplesSize+= MAX_EVICTED_TUPLE_SIZE;
                    if(block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize >= block_size){
                    // VOLT_INFO("Size of block exceeds!!in child %d", block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize);
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
            }
            if(block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize >= block_size){
                // VOLT_INFO("Size of block exceeds!! %d", block.getSerializedSize() + MAX_EVICTED_TUPLE_SIZE + childTuplesSize);
                break;
            }
            parentTuples++;
       
            // remove the tuple from the eviction chain
            removeTuple(table, &tuple);
            if (tuple.isEvicted()) {
                // VOLT_INFO("Tuple %d is already evicted. Skipping", table->getTupleID(tuple.address()));
                continue;
            }
            //VOLT_INFO("Evicting Tuple: %s", tuple.debug(table->name()).c_str());
            tuple.setEvictedTrue();

            // Populate the evicted_tuple with the block id and tuple offset
            // Make sure this tuple is marked as evicted, so that we know it is an evicted
            // tuple as we iterate through the index
            evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));
            evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(num_tuples_evicted));
            evicted_tuple.setEvictedTrue();
            //VOLT_INFO("EvictedTuple: %s", evicted_tuple.debug(m_evictedTable->name()).c_str());

            // Then add it to this table's EvictedTable
            const void* evicted_tuple_address = static_cast<EvictedTable*>(m_evictedTable)->insertEvictedTuple(evicted_tuple);

            // Change all of the indexes to point to our new evicted tuple
            table->setEntryToNewAddressForAllIndexes(&tuple, evicted_tuple_address);

            block.addTuple(tuple);
 
           // At this point it's safe for us to delete this mofo
            tuple.freeObjectColumns(); // will return memory for uninlined strings to the heap
            table->deleteTupleStorage(tuple);


            num_tuples_evicted++;
       //     VOLT_DEBUG("Added new evicted %s tuple to block #%d [tuplesEvicted=%d]",
        //               name().c_str(), block_id, num_tuples_evicted);
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
//VOLT_INFO("Setting %s tuple blockId at offset %d", child_evictedTable->name().c_str(), 0);
            child_evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));   // Set the ID for this block
            child_evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(0));          // set the tuple offset of this block

            childTuple = *it;
            num_tuples_evicted++;
            //removeTuple(childTable, &childTuple);
            childTuple.setEvictedTrue();

            // Populate the evicted_tuple with the block id and tuple offset
            // Make sure this tuple is marked as evicted, so that we know it is an evicted
            // tuple as we iterate through the index
            child_evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));
            child_evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(childTuples));
            child_evicted_tuple.setEvictedTrue();
  //          VOLT_INFO("EvictedTuple: %s", child_evicted_tuple.debug(child_evictedTable->name()).c_str());

            // Then add it to this table's EvictedTable
            const void* evicted_tuple_address = static_cast<EvictedTable*>(child_evictedTable)->insertEvictedTuple(child_evicted_tuple);

            // Change all of the indexes to point to our new evicted tuple
            childTable->setEntryToNewAddressForAllIndexes(&childTuple, evicted_tuple_address);

            // VOLT_INFO("tuple foreign key id %d", ValuePeeker::peekAsInteger(childTuple.getNValue(foreignKeyIndexColumn)));
            // VOLT_INFO("EvictedTuple: %s", childTuple.debug(childTable->name()).c_str());
            block.addTuple(childTuple);
            //write out to block
            childTuple.freeObjectColumns();
            childTable->deleteTupleStorage(childTuple);

            childTuples++;


        }
        childBytes = block.getSerializedSize() - parentBytes - initSize;
        ////////////////END CHILD TUPLE ADDING TO BLOCK/////////////////////

//         VOLT_DEBUG("Finished evictable tuple iterator for %s [tuplesEvicted=%d]",
//                   table->name().c_str(), num_tuples_evicted);
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
        if (num_tuples_evicted >= 0) {
            // TODO: make this look like
            //          block.flush();
            //          antiCacheDB->writeBlock(block);
            antiCacheDB->writeBlock(table->name(),
                                    block_id,
                                    num_tuples_evicted,
                                    block.getSerializedData(),
                                    block.getSerializedSize());
            needs_flush = true;

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
//                      name().c_str(), (long)origEvictedTableSize, (long)m_evictedTable->activeTupleCount());
         //   #endif
        } else {
   //         VOLT_WARN("No tuples were evicted from %s", table->name().c_str());
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

Table* AntiCacheEvictionManager::readBlocks(PersistentTable *table, int numBlocks, int16_t blockIds[], int32_t tuple_offsets[]) {
    
    VOLT_INFO("Reading %d evicted blocks.", numBlocks);

    for(int i = 0; i < numBlocks; i++)
        readEvictedBlock(table, blockIds[i], tuple_offsets[i]);

    return (m_readResultTable);
}
    
bool AntiCacheEvictionManager::readEvictedBlock(PersistentTable *table, int16_t block_id, int32_t tuple_offset) {

    bool already_unevicted = table->isAlreadyUnEvicted(block_id);
    if(already_unevicted) // this block has already been read
    {
        #ifdef VOLT_INFO_ENABLED
        VOLT_INFO("Block %d has already been read.", block_id);
        #endif
        return true;
    }

    AntiCacheDB* antiCacheDB = table->getAntiCacheDB();

    try
    {
        AntiCacheBlock value = antiCacheDB->readBlock(table->name(), block_id);

        // allocate the memory for this block
        VOLT_INFO("block size is %ld - table Name %s", value.getSize(), table->name().c_str());

    char* unevicted_tuples = new char[value.getSize()];
        memcpy(unevicted_tuples, value.getData(), value.getSize());
    VOLT_INFO("*****************block id ************** %d", block_id);
        ReferenceSerializeInput in(unevicted_tuples, 10485760);

        // Read in all the meta-data
        int num_tables = in.readInt();
        VOLT_INFO("num tables is %d", num_tables);
        std::vector<std::string> tableNames;
        std::vector<int> numTuples;
        for(int j = 0; j < num_tables; j++){
            std::string name = in.readTextString();
            tableNames.push_back(name);
            VOLT_INFO("tableName is %s", name.c_str());
            int tuples = in.readInt();
            numTuples.push_back(tuples);
            VOLT_INFO("num tuples is %d", tuples);
        }

        table->insertUnevictedBlock(unevicted_tuples);
        VOLT_INFO("unevicted blocks size is %d", static_cast<int> (table->unevictedBlocksSize()));
        table->insertTupleOffset(tuple_offset);

    
        table->insertUnevictedBlockID(std::pair<int16_t,int16_t>(block_id, 0));
    }
    catch(UnknownBlockAccessException e)
    {
        throw e;

        VOLT_INFO("UnknownBlockAccessException caught.");
        return false;
    }

//    VOLT_INFO("blocks read: %d", m_blocksRead);
    return true;
}

/*
 * Merges the unevicted block into the regular data table
 */
bool AntiCacheEvictionManager::mergeUnevictedTuples(PersistentTable *table)
{
    VOLT_INFO("in merge");
    int num_blocks = table->unevictedBlocksSize();
    int32_t num_tuples_in_block = -1;

//    for (std::map<int16_t,int16_t>::iterator it=table->getUnevictedBlockIDs().begin(); it!=table->getUnevictedBlockIDs().end(); ++it)
//        std::cout << it->first << " => " << it->second << '\n';


    if(num_blocks == 0){
        VOLT_INFO("num blocks was 0!!!!!");
        return false;
    }

    int32_t merge_tuple_offset = 0; // this is the offset of tuple that caused this block to be unevicted

    DefaultTupleSerializer serializer;
    TableTuple unevictedTuple(table->m_schema);

    TableTuple evicted_tuple = table->getEvictedTable()->tempTuple();


    #ifdef VOLT_INFO_ENABLED
    int active_tuple_count = (int)table->activeTupleCount();
    int tuples_in_eviction_chain = (int)table->getNumTuplesInEvictionChain();
    #endif

    #ifdef VOLT_INFO_ENABLED
    VOLT_INFO("Merging %d blocks for table %s.", num_blocks, table->name().c_str());
    #endif

    for (int i = 0; i < num_blocks; i++)
    {
        // XXX: have to put block size, which we don't know, so just put something large, like 10MB
        ReferenceSerializeInput in(table->getUnevictedBlocks(i), 10485760);

        // Read in all the meta-data
        int num_tables = in.readInt();
        std::vector<std::string> tableNames;
        std::vector<int> numTuples;
        for(int j = 0; j < num_tables; j++){
            tableNames.push_back(in.readTextString());
            numTuples.push_back(in.readInt());
        }

        merge_tuple_offset = table->getMergeTupleOffset(i); // what to do about this?
        //VOLT_INFO("Tuple offset is %d", merge_tuple_offset);

        int count = 0;
        for (std::vector<std::string>::iterator it = tableNames.begin() ; it != tableNames.end(); ++it){
            PersistentTable *tableInBlock = dynamic_cast<PersistentTable*>(m_engine->getTable(*it));
            num_tuples_in_block = numTuples.at(count);
            VOLT_INFO("Merging %d tuples.", num_tuples_in_block);

            // Now read the actual tuples
            int64_t bytes_unevicted = 0;
            int tuplesRead = 0;
            for (int j = 0; j < num_tuples_in_block; j++)
            {
                // if we're using the tuple-merge strategy, only merge in a single tuple
                if(!table->mergeStrategy())
                {
                    if(j != merge_tuple_offset)  // don't merge this tuple
                        continue;
                }

        bytes_unevicted += tableInBlock->unevictTuple(&in, j, merge_tuple_offset);
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
            VOLT_INFO("Bytes unevicted%ld", long(bytes_unevicted));
            tableInBlock->m_bytesRead+=bytes_unevicted;
            tableInBlock->m_blocksEvicted -= 1;
            tableInBlock->m_blocksRead += 1;

            count++;

        }



        delete [] table->getUnevictedBlocks(i);
    //table->clearUnevictedBlocks(i);
    }

VOLT_INFO("unevicted blocks size %d", static_cast<int>(table->unevictedBlocksSize()));
table->clearUnevictedBlocks();
    table->clearMergeTupleOffsets();

    #ifdef VOLT_INFO_ENABLED
    VOLT_INFO("Active Tuple Count: %d -- %d", (int)active_tuple_count, (int)table->activeTupleCount());
    VOLT_INFO("Tuples in Eviction Chain: %d -- %d", (int)tuples_in_eviction_chain, (int)table->getNumTuplesInEvictionChain());
    #endif

    return true;
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

