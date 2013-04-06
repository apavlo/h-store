/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
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

#include <sstream>
#include <cassert>
#include <cstdio>

#include "boost/scoped_ptr.hpp"
#include "storage/persistenttable.h"

#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/FailureInjection.h"
#include "common/tabletuple.h"
#include "common/UndoQuantum.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/types.h"
#include "common/RecoveryProtoMessage.h"
#include "common/ValueFactory.hpp"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/TupleStreamWrapper.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/PersistentTableUndoInsertAction.h"
#include "storage/PersistentTableUndoDeleteAction.h"
#include "storage/PersistentTableUndoUpdateAction.h"
#include "storage/ConstraintFailureException.h"
#include "storage/MaterializedViewMetadata.h"
#include "storage/CopyOnWriteContext.h"

#ifdef ANTICACHE
#include "boost/timer.hpp"
#include "anticache/EvictedTable.h"
#include "anticache/AntiCacheDB.h"
#include "anticache/EvictionIterator.h"
#include "anticache/UnknownBlockAccessException.h"
#endif

#include <map>

namespace voltdb {

void* keyTupleStorage = NULL;
TableTuple keyTuple;

/**
 * This value has to match the value in CopyOnWriteContext.cpp
 */
#define TABLE_BLOCKSIZE 2097152
#define MAX_EVICTED_TUPLE_SIZE 2500

PersistentTable::PersistentTable(ExecutorContext *ctx, bool exportEnabled) :
    Table(TABLE_BLOCKSIZE), m_executorContext(ctx), m_uniqueIndexes(NULL), m_uniqueIndexCount(0), m_allowNulls(NULL),
    m_indexes(NULL), m_indexCount(0), m_pkeyIndex(NULL), m_wrapper(NULL),
    m_tsSeqNo(0), stats_(this), m_exportEnabled(exportEnabled),
    m_COWContext(NULL)
{

    #ifdef ANTICACHE
    m_evictedTable = NULL;
    m_unevictedTuples = NULL; 
    m_numUnevictedTuples = 0;
    m_newestTupleID = 0;
    m_oldestTupleID = 0;
    m_numTuplesInEvictionChain = 0;
    m_blockMerge = true; 
    #endif
    
    if (exportEnabled) {
        m_wrapper = new TupleStreamWrapper(m_executorContext->m_partitionId,
                                           m_executorContext->m_siteId,
                                           m_executorContext->m_lastTickTime);
    }
}

PersistentTable::~PersistentTable() {
    // delete all tuples to free strings
    voltdb::TableIterator ti(this);
    voltdb::TableTuple tuple(m_schema);

    while (ti.next(tuple)) {
        // indexes aren't released as they don't have ownership of strings
        tuple.freeObjectColumns();
        tuple.setDeletedTrue();
    }
    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        if (index != m_pkeyIndex) {
            delete index;
        }
    }
    if (m_pkeyIndex) delete m_pkeyIndex;
    if (m_uniqueIndexes) delete[] m_uniqueIndexes;
    if (m_allowNulls) delete[] m_allowNulls;
    if (m_indexes) delete[] m_indexes;
    
    #ifdef ANTICACHE
    if (m_evictedTable) delete m_evictedTable;
    #endif

    // note this class has ownership of the views, even if they
    // were allocated by VoltDBEngine
    for (int i = 0; i < m_views.size(); i++) {
        delete m_views[i];
    }

    delete m_wrapper;
}
    
// ------------------------------------------------------------------
// ANTI-CACHE
// ------------------------------------------------------------------ 

#ifdef ANTICACHE

void PersistentTable::setEvictedTable(voltdb::Table *evictedTable) {
    VOLT_INFO("Initialized EvictedTable for table '%s'", this->name().c_str());
    m_evictedTable = evictedTable;
}
    
voltdb::Table* PersistentTable::getEvictedTable() {
    return m_evictedTable; 
}

bool PersistentTable::evictBlockToDisk(const long block_size, int num_blocks) {
    if (m_evictedTable == NULL) {
        throwFatalException("Trying to evict block from table '%s' before its "\
                            "EvictedTable has been initialized", this->name().c_str());
    }

    #ifdef VOLT_INFO_ENABLED
    VOLT_DEBUG("Evicting a block of size %ld bytes from table '%s' with %d tuples",
               block_size, this->name().c_str(), (int)this->allocatedTupleCount());
    VOLT_DEBUG("%s Table Schema:\n%s",
              m_evictedTable->name().c_str(), m_evictedTable->schema()->debug().c_str());
    #endif

    // get the AntiCacheDB instance from the executorContext
    AntiCacheDB* antiCacheDB = m_executorContext->getAntiCacheDB();
    int tuple_length = -1;
    bool needs_flush = false;
    
    // get the eviction manager for this tuple
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    if (eviction_manager == NULL) {
        throwFatalException("Trying to evict block from table '%s' before its "\
                            "EvictionManager has been initialized", this->name().c_str());
    }
    
    for(int i = 0; i < num_blocks; i++)
    {
        
        // get a unique block id from the executorContext
        int16_t block_id = antiCacheDB->nextBlockId();
        
        // create a new evicted table tuple based on the schema for the source tuple
        TableTuple evicted_tuple = m_evictedTable->tempTuple();
        VOLT_DEBUG("Setting %s tuple blockId at offset %d", m_evictedTable->name().c_str(), 0);
        evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));   // Set the ID for this block
        evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(0));          // set the tuple offset of this block

        // buffer used for serializing a single tuple
        DefaultTupleSerializer serializer;
        char serialized_data[block_size];
        ReferenceSerializeOutput out(serialized_data, block_size);
        
        // Iterate through the table and pluck out tuples to put in our block
        TableTuple tuple(m_schema);
        EvictionIterator evict_itr(this);         
        
        VOLT_INFO("active tuple count: %d", (int)activeTupleCount()); 
        
        #ifdef VOLT_INFO_ENABLED
        boost::timer timer;
        int64_t origEvictedTableSize = m_evictedTable->activeTupleCount();
        #endif

        size_t current_tuple_start_position;
            
        int32_t num_tuples_evicted = 0;
        out.writeInt(num_tuples_evicted); // reserve first 4 bytes in buffer for number of tuples in block
        
        VOLT_DEBUG("Starting evictable tuple iterator for %s", name().c_str());
        while (evict_itr.hasNext() && (out.size() + MAX_EVICTED_TUPLE_SIZE < block_size)) {
            if(!evict_itr.next(tuple))
                break;
            
            // If this is the first tuple, then we need to allocate all of the memory and
            // what not that we're going to need
            if (tuple_length == -1) {
                tuple_length = tuple.tupleLength();
            }
            
            current_tuple_start_position = out.position();
            
            // remove the tuple from the eviction chain
            eviction_manager->removeTuple(this, &tuple);
                    
            if (tuple.isEvicted())
            {
                VOLT_INFO("Tuple %d is already evicted. Skipping", tuple.getTupleID()); 
                continue;
            } 
            VOLT_DEBUG("Evicting Tuple: %s", tuple.debug(name()).c_str());
            tuple.setEvictedTrue();
            
            // Populate the evicted_tuple with the block id and tuple offset
            // Make sure this tuple is marked as evicted, so that we know it is an evicted 
            // tuple as we iterate through the index
            evicted_tuple.setNValue(0, ValueFactory::getSmallIntValue(block_id));
            evicted_tuple.setNValue(1, ValueFactory::getIntegerValue(num_tuples_evicted));
            evicted_tuple.setEvictedTrue(); 
            VOLT_DEBUG("EvictedTuple: %s", evicted_tuple.debug(m_evictedTable->name()).c_str());

            // Then add it to this table's EvictedTable
            const void* evicted_tuple_address = static_cast<EvictedTable*>(m_evictedTable)->insertEvictedTuple(evicted_tuple);
            
            // Change all of the indexes to point to our new evicted tuple
            setEntryToNewAddressForAllIndexes(&tuple, evicted_tuple_address);

            // Now copy the raw bytes for this tuple into the serialized buffer
            tuple.serializeWithHeaderTo(out);
            
            // At this point it's safe for us to delete this mofo
            tuple.freeObjectColumns(); // will return memory for uninlined strings to the heap
            deleteTupleStorage(tuple);
            
            num_tuples_evicted++;
            VOLT_DEBUG("Added new evicted %s tuple to block #%d [tuplesEvicted=%d]",
                       name().c_str(), block_id, num_tuples_evicted);
            
        } // WHILE
        VOLT_DEBUG("Finished evictable tuple iterator for %s [tuplesEvicted=%d]",
                   name().c_str(), num_tuples_evicted);
        
        // write out the block header (i.e. number of tuples in block)
        out.writeIntAt(0, num_tuples_evicted);
        
        #ifdef VOLT_INFO_ENABLED
        VOLT_DEBUG("Evicted %d tuples / %d bytes.", num_tuples_evicted, (int)out.size());
        VOLT_DEBUG("Eviction Time: %.2f sec", timer.elapsed());
        timer.restart();
        #endif
        
        // Only write out a bock if there are tuples in it
        if (num_tuples_evicted >= 0) {
            antiCacheDB->writeBlock(name(),
                                    block_id,
                                    num_tuples_evicted,
                                    out.data(),
                                    out.size());
            needs_flush = true;
                    
            // Update Stats
            m_tuplesEvicted += num_tuples_evicted;
            m_blocksEvicted += 1;
            m_bytesEvicted += out.size();
            
            m_tuplesWritten += num_tuples_evicted;
            m_blocksWritten += 1;
            m_bytesWritten += out.size();
        
            #ifdef VOLT_INFO_ENABLED
            VOLT_DEBUG("AntiCacheDB Time: %.2f sec", timer.elapsed());
            VOLT_DEBUG("Evicted Block #%d for %s [tuples=%d / size=%ld / tupleLen=%d]",
                      block_id, name().c_str(),
                      num_tuples_evicted, m_bytesEvicted, tuple_length);
            VOLT_DEBUG("%s EvictedTable [origCount:%ld / newCount:%ld]",
                      name().c_str(), (long)origEvictedTableSize, (long)m_evictedTable->activeTupleCount());
            #endif
        } else {
            VOLT_WARN("No tuples were evicted from %s", name().c_str());
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
    
    return true;
}

bool PersistentTable::readEvictedBlock(int16_t block_id, int32_t tuple_offset) {
    
    std::map<int16_t,int16_t>::iterator it;
    it = m_unevictedBlockIDs.find(block_id); 
    if(it != m_unevictedBlockIDs.end()) // this block has already been read
        return true; 
    
    AntiCacheDB* antiCacheDB = m_executorContext->getAntiCacheDB(); 
    
    try
    {
        AntiCacheBlock value = antiCacheDB->readBlock(this->name(), block_id);
        
        // allocate the memory for this block
        char* unevicted_tuples = new char[value.getSize()]; 
        memcpy(unevicted_tuples, value.getData(), value.getSize()); 
        m_unevictedBlocks.push_back(unevicted_tuples);
        m_mergeTupleOffset.push_back(tuple_offset); 
        
        // Update eviction stats
        m_bytesEvicted -= value.getSize(); 
        m_bytesRead += value.getSize();
        
//        m_unevictedBlockIDs.push_back(block_id);
        m_unevictedBlockIDs.insert(std::pair<int16_t,int16_t>(block_id, 0)); 
    }
    catch(UnknownBlockAccessException exception)
    {
        throw exception; 
        
        VOLT_INFO("UnknownBlockAccessException caught."); 
        
        return false; 
    }
    
    return true;
}
    
/*
 * Merges the unevicted block into the regular data table
 */
bool PersistentTable::mergeUnevictedTuples() 
{        
    int num_blocks = static_cast<int> (m_unevictedBlocks.size());
    int32_t num_tuples_in_block = -1;
    int tuplesRead = 0;
    
    if(num_blocks == 0)
        return false;
    
    int32_t merge_tuple_offset = 0; // this is the offset of tuple that caused this block to be unevicted
    
    DefaultTupleSerializer serializer;
    TableTuple unevictedTuple(m_schema);
    
    TableTuple evicted_tuple = m_evictedTable->tempTuple();
    
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    
    VOLT_INFO("Merging %d blocks for table %s.", num_blocks, name().c_str());
    for (int i = 0; i < num_blocks; i++)
    {        
        // XXX: have to put block size, which we don't know, so just put something large, like 10MB
        ReferenceSerializeInput in(m_unevictedBlocks[i], 10485760);
        num_tuples_in_block = in.readInt();
        
        merge_tuple_offset = m_mergeTupleOffset[i]; 
        
        VOLT_INFO("Merging %d tuples.", num_tuples_in_block);
        
        for (int j = 0; j < num_tuples_in_block; j++)
        {
            // if we're using the tuple-merge strategy, only merge in a single tuple
            if(!m_blockMerge)  
            {
                if(j != merge_tuple_offset)  // don't merge this tuple
                    continue; 
            }
            
            // get a free tuple and increment the count of tuples current used
            nextFreeTuple(&m_tmpTarget1);
            m_tupleCount++;
            
            // deserialize tuple from unevicted block
            m_tmpTarget1.deserializeWithHeaderFrom(in);
            m_tmpTarget1.setEvictedFalse();
            m_tmpTarget1.setDeletedFalse(); 
            
//                m_tmpTarget1.copyForPersistentInsert(source); // tuple in freelist must be already cleared
//                m_tmpTarget1.setDeletedFalse();
            
            // Note, this goal of the section below is to get a tuple that points to the tuple in the EvictedTable and has the
            // schema of the evicted tuple. However, the lookup has to be done using the schema of the original (unevicted) version
            m_tmpTarget2 = lookupTuple(m_tmpTarget1);       // lookup the tuple in the table
            evicted_tuple.move(m_tmpTarget2.address());
            static_cast<EvictedTable*>(m_evictedTable)->deleteEvictedTuple(evicted_tuple);             // delete the EvictedTable tuple
            
            // update the indexes to point to this newly unevicted tuple
            setEntryToNewAddressForAllIndexes(&m_tmpTarget1, m_tmpTarget1.address());
            
            // re-insert the tuple back into the eviction chain
            if(j == merge_tuple_offset)  // put it at the back of the chain
                eviction_manager->updateTuple(this, &m_tmpTarget1, true);
            else
                eviction_manager->updateUnevictedTuple(this, &m_tmpTarget1);
        }
        tuplesRead += num_tuples_in_block;
        delete [] m_unevictedBlocks[i];
    }
    
    // Update eviction stats
    m_tuplesEvicted -= tuplesRead;
    m_tuplesRead += tuplesRead;

    m_blocksEvicted -= num_blocks; 
    m_blocksRead += num_blocks;
    
    m_unevictedBlocks.clear();
    m_mergeTupleOffset.clear(); 
//        m_unevictedBlockIDs.clear();  // if we uncomment this the benchmark won't end 

    return true; 
}
    
void PersistentTable::setNumTuplesInEvictionChain(int num_tuples)
{
    m_numTuplesInEvictionChain = num_tuples; 
}
    
int PersistentTable::getNumTuplesInEvictionChain()
{
    return m_numTuplesInEvictionChain;  
}
    
void PersistentTable::setNewestTupleID(uint32_t id)
{
    m_newestTupleID = id; 
}

void PersistentTable::setOldestTupleID(uint32_t id)
{
    m_oldestTupleID = id; 
}

uint32_t PersistentTable::getNewestTupleID()
{
    return m_newestTupleID; 
}

uint32_t PersistentTable::getOldestTupleID()
{
    return m_oldestTupleID; 
}

#endif


// ------------------------------------------------------------------
// OPERATIONS
// ------------------------------------------------------------------
void PersistentTable::deleteAllTuples(bool freeAllocatedStrings) {
    // nothing interesting
    voltdb::TableIterator ti(this);
    voltdb::TableTuple tuple(m_schema);
    while (ti.next(tuple)) {
        deleteTuple(tuple, true);
    }
}

void setSearchKeyFromTuple(TableTuple &source) {
    keyTuple.setNValue(0, source.getNValue(1));
    keyTuple.setNValue(1, source.getNValue(2));
}

/*
 * Regular tuple insertion that does an allocation and copy for
 * uninlined strings and creates and registers an UndoAction.
 */
bool PersistentTable::insertTuple(TableTuple &source) {
    size_t elMark = 0;

    //VOLT_INFO("In insertTuple().");

    // not null checks at first
    FAIL_IF(!checkNulls(source)) {
        throw ConstraintFailureException(this, source, TableTuple(),
                                         voltdb::CONSTRAINT_TYPE_NOT_NULL);
    }

    //
    // First get the next free tuple
    // This will either give us one from the free slot list, or
    // grab a tuple at the end of our chunk of memory
    //
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;

    //
    // Then copy the source into the target
    //
    m_tmpTarget1.copyForPersistentInsert(source); // tuple in freelist must be already cleared
    m_tmpTarget1.setDeletedFalse();

    /**
     * Inserts never "dirty" a tuple since the tuple is new, but...  The
     * COWIterator may still be scanning and if the tuple came from the free
     * list then it may need to be marked as dirty so it will be skipped. If COW
     * is on have it decide. COW should always set the dirty to false unless the
     * tuple is in a to be scanned area.
     */
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(m_tmpTarget1, true);
    } else {
        m_tmpTarget1.setDirtyFalse();
    }
    m_tmpTarget1.isDirty();

    if (!tryInsertOnAllIndexes(&m_tmpTarget1)) {
        // Careful to delete allocated objects
        m_tmpTarget1.freeObjectColumns();
        deleteTupleStorage(m_tmpTarget1);
        throw ConstraintFailureException(this, source, TableTuple(),
                                         voltdb::CONSTRAINT_TYPE_UNIQUE);
    }

    // if EL is enabled, append the tuple to the buffer
    // exportxxx: memoizing this more cache friendly?
    if (m_exportEnabled) {
        elMark =
          appendToELBuffer(m_tmpTarget1, m_tsSeqNo++, TupleStreamWrapper::INSERT);
    }

    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        m_nonInlinedMemorySize += m_tmpTarget1.getNonInlinedMemorySize();
    }
    /*
     * Create and register an undo action.
     */
    voltdb::UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
    assert(undoQuantum);
    voltdb::Pool *pool = undoQuantum->getDataPool();
    assert(pool);
    voltdb::PersistentTableUndoInsertAction *ptuia =
      new (pool->allocate(sizeof(voltdb::PersistentTableUndoInsertAction)))
      voltdb::PersistentTableUndoInsertAction(m_tmpTarget1, this, pool, elMark);
    undoQuantum->registerUndoAction(ptuia);
    VOLT_DEBUG("Registered UndoAction for new tuple in table '%s'", name().c_str());

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        VOLT_DEBUG("Inserting tuple from %s into materialized view %s [%d]",
                   m_name.c_str(), m_views[i]->name().c_str(), i);
        m_views[i]->processTupleInsert(source);
    }

#ifdef ANTICACHE
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    eviction_manager->updateTuple(this, &m_tmpTarget1, true); 
#endif

    return true;
}

/*
 * Insert a tuple but don't allocate a new copy of the uninlineable
 * strings or create an UndoAction or update a materialized view.
 */
void PersistentTable::insertTupleForUndo(TableTuple &source, size_t wrapperOffset) {

    //VOLT_INFO("In insertTupleForUndo()."); 

    // not null checks at first
    if (!checkNulls(source)) {
        throwFatalException("Failed to insert tuple into table %s for undo:"
                            " null constraint violation\n%s\n", m_name.c_str(),
                            source.debugNoHeader().c_str());
    }

    // rollback Export
    if (m_exportEnabled) {
        m_wrapper->rollbackTo(wrapperOffset);
    }


    // First get the next free tuple This will either give us one from
    // the free slot list, or grab a tuple at the end of our chunk of
    // memory
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;

    // Then copy the source into the target
    m_tmpTarget1.copy(source);
    m_tmpTarget1.setDeletedFalse();

    /**
     * See the comments in insertTuple for why this has to be done. The same situation applies here
     * in the undo case. When the tuple was deleted a copy was made for the COW. Even though it is being
     * reintroduced here it should be considered a new tuple and marked as dirty if the COWIterator will scan it
     * otherwise two copies will appear. The one reintroduced by the undo action and the copy made when the tuple
     * was originally deleted.
     */
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(m_tmpTarget1, true);
    } else {
        m_tmpTarget1.setDirtyFalse();
    }
    m_tmpTarget1.isDirty();

    if (!tryInsertOnAllIndexes(&m_tmpTarget1)) {
        deleteTupleStorage(m_tmpTarget1);
        throwFatalException("Failed to insert tuple into table %s for undo:"
                            " unique constraint violation\n%s\n", m_name.c_str(),
                            m_tmpTarget1.debugNoHeader().c_str());
    }

    if (m_exportEnabled) {
        m_wrapper->rollbackTo(wrapperOffset);
    }

#ifdef ANTICACHE
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    eviction_manager->updateTuple(this, &source, false); 
#endif
}

/*
 * Regular tuple update function that does a copy and allocation for
 * updated strings and creates an UndoAction.
 */
bool PersistentTable::updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes) {
    size_t elMark = 0;
    
#ifdef ANTICACHE
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    eviction_manager->updateTuple(this, &source, false); 
#endif

    /*
     * Create and register an undo action and then use the copy of
     * the target (old value with no updates)
     */
     voltdb::UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
     assert(undoQuantum);
     voltdb::Pool *pool = undoQuantum->getDataPool();
     assert(pool);
     voltdb::PersistentTableUndoUpdateAction *ptuua =
       new (pool->allocate(sizeof(voltdb::PersistentTableUndoUpdateAction)))
       voltdb::PersistentTableUndoUpdateAction(target, this, pool);

     if (m_COWContext.get() != NULL) {
         m_COWContext->markTupleDirty(target, false);
     }

    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        m_nonInlinedMemorySize -= target.getNonInlinedMemorySize();
        m_nonInlinedMemorySize += source.getNonInlinedMemorySize();
    }

     source.setDeletedFalse();
     //Copy the dirty status that was set by markTupleDirty.
     if (target.isDirty()) {
         source.setDirtyTrue();
     } else {
         source.setDirtyFalse();
     }
     target.copyForPersistentUpdate(source);

     ptuua->setNewTuple(target, pool);

     if (!undoQuantum->isDummy()) {
         //DummyUndoQuantum calls destructor upon register.
         undoQuantum->registerUndoAction(ptuua);
     }

    // the planner should determine if this update can affect indexes.
    // if so, update the indexes here
    if (updatesIndexes) {
        if (!tryUpdateOnAllIndexes(ptuua->getOldTuple(), target)) {
            throw ConstraintFailureException(this, ptuua->getOldTuple(),
                                             target,
                                             voltdb::CONSTRAINT_TYPE_UNIQUE);
        }

        //If the CFE is thrown the Undo action should not attempt to revert the
        //indexes.
        ptuua->needToRevertIndexes();
        updateFromAllIndexes(ptuua->getOldTuple(), target);
    }

    // if EL is enabled, append the tuple to the buffer
    if (m_exportEnabled) {
        // only need the earliest mark
        elMark = appendToELBuffer(ptuua->getOldTuple(), m_tsSeqNo, TupleStreamWrapper::DELETE);
        appendToELBuffer(target, m_tsSeqNo++, TupleStreamWrapper::INSERT);
        ptuua->setELMark(elMark);
    }

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleUpdate(ptuua->getOldTuple(), target);
    }

    /**
     * Check for nulls after the update has been performed because the source tuple may have garbage in
     * some columns
     */
    FAIL_IF(!checkNulls(target)) {
        throw ConstraintFailureException(this, ptuua->getOldTuple(),
                                         target,
                                         voltdb::CONSTRAINT_TYPE_NOT_NULL);
    }

    if (undoQuantum->isDummy()) {
        //DummyUndoQuantum calls destructor upon register so it can't be called
        //earlier
        undoQuantum->registerUndoAction(ptuua);
    }

    return true;
}

/*
 * Source contains the tuple before the update and target is a
 * reference to the updated tuple including the actual table
 * storage. First backup the target to a temp tuple so it will be
 * available for updating indexes. Then revert the tuple to the
 * original preupdate values by copying the source to the target. Then
 * update the indexes to use the new key value (if the key has
 * changed). The backup is necessary because the indexes expect the
 * data ptr that will be used as the value in the index.
 */
void PersistentTable::updateTupleForUndo(TableTuple &source, TableTuple &target,
                                         bool revertIndexes, size_t wrapperOffset) {
    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        m_nonInlinedMemorySize -= target.getNonInlinedMemorySize();
        m_nonInlinedMemorySize += source.getNonInlinedMemorySize();
    }

    //Need to back up the updated version of the tuple to provide to
    //the indexes when updating The indexes expect source's data Ptr
    //to point into the table so it is necessary to copy source to
    //target. Without this backup the target would be lost and it
    //there would be nothing to provide to the index to lookup. In
    //regular updateTuple this storage is provided by the undo
    //quantum.
    TableTuple targetBackup = tempTuple();
    targetBackup.copy(target);

    bool dirty = target.isDirty();
    // this is the actual in-place revert to the old version
    target.copy(source);
    if (dirty) {
        target.setDirtyTrue();
    } else {
        target.setDirtyFalse();
    }
    target.isDirty();

    //If the indexes were never updated there is no need to revert them.
    if (revertIndexes) {
        if (!tryUpdateOnAllIndexes(targetBackup, target)) {
            // TODO: this might be too strict. see insertTuple()
            throwFatalException("Failed to update tuple in table %s for undo:"
                                " unique constraint violation\n%s\n%s\n", m_name.c_str(),
                                targetBackup.debugNoHeader().c_str(),
                                target.debugNoHeader().c_str());
        }
        updateFromAllIndexes(targetBackup, target);
    }

    if (m_exportEnabled) {
        m_wrapper->rollbackTo(wrapperOffset);
    }
}

bool PersistentTable::deleteTuple(TableTuple &target, bool deleteAllocatedStrings) {
    // May not delete an already deleted tuple.
    assert(target.isActive());

    // The tempTuple is forever!
    assert(&target != &m_tempTuple);
    
#ifdef ANTICACHE
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    eviction_manager->removeTuple(this, &target); 
#endif

    // Just like insert, we want to remove this tuple from all of our indexes
    deleteFromAllIndexes(&target);

    /**
     * A user initiated delete needs to have the tuple "marked dirty" so that the copy is made.
     */
    if (m_COWContext.get() != NULL) {
        m_COWContext->markTupleDirty(target, false);
    }

    /*
     * Create and register an undo action.
     */
    voltdb::UndoQuantum *undoQuantum = m_executorContext->getCurrentUndoQuantum();
    assert(undoQuantum);
    voltdb::Pool *pool = undoQuantum->getDataPool();
    assert(pool);
    voltdb::PersistentTableUndoDeleteAction *ptuda = new (pool->allocate(sizeof(voltdb::PersistentTableUndoDeleteAction))) voltdb::PersistentTableUndoDeleteAction( target, this, pool);

    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleDelete(target);
    }

    // if EL is enabled, append the tuple to the buffer
    if (m_exportEnabled) {
        size_t elMark = appendToELBuffer(target, m_tsSeqNo++, TupleStreamWrapper::DELETE);
        ptuda->setELMark(elMark);
    }

    undoQuantum->registerUndoAction(ptuda);
    deleteTupleStorage(target);
    return true;
}

/*
 * Delete a tuple by looking it up via table scan or a primary key
 * index lookup. An undo initiated delete like deleteTupleForUndo
 * is in response to the insertion of a new tuple by insertTuple
 * and that by definition is a tuple that is of no interest to
 * the COWContext. The COWContext set the tuple to have the
 * correct dirty setting when the tuple was originally inserted.
 * TODO remove duplication with regular delete. Also no view updates.
 */
void PersistentTable::deleteTupleForUndo(voltdb::TableTuple &tupleCopy, size_t wrapperOffset) {
    TableTuple target = lookupTuple(tupleCopy);
    if (target.isNullTuple()) {
        throwFatalException("Failed to delete tuple from table %s:"
                            " tuple does not exist\n%s\n", m_name.c_str(),
                            tupleCopy.debugNoHeader().c_str());
    }
    else {
        // Make sure that they are not trying to delete the same tuple twice
        assert(target.isActive());

        // Also make sure they are not trying to delete our m_tempTuple
        assert(&target != &m_tempTuple);

        // rollback Export
        if (m_exportEnabled) {
            m_wrapper->rollbackTo(wrapperOffset);
        }

        // Just like insert, we want to remove this tuple from all of our indexes
        deleteFromAllIndexes(&target);

        if (m_schema->getUninlinedObjectColumnCount() != 0)
        {
            m_nonInlinedMemorySize -= tupleCopy.getNonInlinedMemorySize();
        }

        // Delete the strings/objects
        target.freeObjectColumns();
        deleteTupleStorage(target);
    }
}

voltdb::TableTuple PersistentTable::lookupTuple(TableTuple tuple) {
    voltdb::TableTuple nullTuple(m_schema);//Null tuple

    voltdb::TableIndex *pkeyIndex = primaryKeyIndex();
    if (pkeyIndex == NULL) {
        /*
         * Do a table scan.
         */
        TableTuple tableTuple(m_schema);
        int tableIndex = 0;
        for (int tupleCount = 0; tupleCount < m_tupleCount; tupleCount++) {
            /*
             * Find the next active tuple
             */
            do {
                tableTuple.move(dataPtrForTuple(tableIndex++));
            } while (!tableTuple.isActive());

            if (tableTuple.equalsNoSchemaCheck(tuple)) {
                return tableTuple;
            }
        }
        return nullTuple;
    }

    bool foundTuple = pkeyIndex->moveToTuple(&tuple);
    if (!foundTuple) {
        return nullTuple;
    }

    return pkeyIndex->nextValueAtKey();
}

void PersistentTable::insertIntoAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->addEntry(tuple)) {
            throwFatalException("Failed to insert tuple into index");
        }
    }
}

void PersistentTable::deleteFromAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->deleteEntry(tuple)) {
            throwFatalException("Failed to delete tuple from index %s.%s [%s]",
                                name().c_str(), m_indexes[i]->getName().c_str(),
                                m_indexes[i]->getTypeName().c_str());
        }
    }
}

void PersistentTable::updateFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_indexCount - 1; i >= 0;--i) {
        if (!m_indexes[i]->replaceEntry(&targetTuple, &sourceTuple)) {
            throwFatalException("Failed to update tuple in index");
        }
    }
}
    
void PersistentTable::setEntryToNewAddressForAllIndexes(const TableTuple *tuple, const void* address) {
    for (int i = m_indexCount - 1; i >= 0; --i) {
        VOLT_DEBUG("Updating tuple address in index %s.%s [%s]",
                   name().c_str(), m_indexes[i]->getName().c_str(), m_indexes[i]->getTypeName().c_str());
        
        //if(!m_indexes[i]->exists(tuple))
        //    VOLT_INFO("ERROR: Cannot find tuple in index!");
        
        if (!m_indexes[i]->setEntryToNewAddress(tuple, address)) {
            throwFatalException("Failed to update tuple to new address in index %s.%s [%s]",
                                name().c_str(), m_indexes[i]->getName().c_str(),
                                m_indexes[i]->getTypeName().c_str());
        }
    }
}

bool PersistentTable::tryInsertOnAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0; --i) {
        FAIL_IF(!m_indexes[i]->addEntry(tuple)) {
            VOLT_DEBUG("Failed to insert into index %s.%s [%s]",
                       name().c_str(), m_indexes[i]->getName().c_str(),
                       m_indexes[i]->getTypeName().c_str());
            for (int j = i + 1; j < m_indexCount; ++j) {
                m_indexes[j]->deleteEntry(tuple);
            }
            return false;
        }
    }
    return true;
}

bool PersistentTable::tryUpdateOnAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple) {
    for (int i = m_uniqueIndexCount - 1; i >= 0;--i) {
        if (m_uniqueIndexes[i]->checkForIndexChange(&targetTuple, &sourceTuple) == false)
            continue; // no update is needed for this index

        // if there is a change, the new_key has to be checked
        FAIL_IF (m_uniqueIndexes[i]->exists(&sourceTuple)) {
            VOLT_WARN("Unique Index '%s' complained to the update",
                      m_uniqueIndexes[i]->debug().c_str());
            return false; // cannot insert the new value
        }
    }
    return true;
}

bool PersistentTable::checkNulls(TableTuple &tuple) const {
    assert (m_columnCount == tuple.sizeInValues());
    for (int i = m_columnCount - 1; i >= 0; --i) {
        if (tuple.isNull(i) && !m_allowNulls[i]) {
            VOLT_TRACE ("%d th attribute was NULL. It is non-nillable attribute.", i);
            return false;
        }
    }
    return true;
}

/*
 * claim ownership of a view. table is responsible for this view*
 */
void PersistentTable::addMaterializedView(MaterializedViewMetadata *view) {
    m_views.push_back(view);
}

// ------------------------------------------------------------------
// UTILITY
// ------------------------------------------------------------------
std::string PersistentTable::tableType() const {
    return "PersistentTable";
}

std::string PersistentTable::debug() {
    std::ostringstream buffer;
    buffer << Table::debug();
    buffer << "\tINDEXES: " << m_indexCount << "\n";

    // Indexes
    buffer << "===========================================================\n";
    for (int index_ctr = 0; index_ctr < m_indexCount; ++index_ctr) {
        if (m_indexes[index_ctr]) {
            buffer << "\t[" << index_ctr << "] " << m_indexes[index_ctr]->debug();
            //
            // Primary Key
            //
            if (m_pkeyIndex != NULL && m_pkeyIndex->getName().compare(m_indexes[index_ctr]->getName()) == 0) {
                buffer << " [PRIMARY KEY]";
            }
            buffer << "\n";
        }
    }

    return buffer.str();
}

// ------------------------------------------------------------------
// Accessors
// ------------------------------------------------------------------
// Index
TableIndex *PersistentTable::index(std::string name) {
    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        if (index->getName().compare(name) == 0) {
            return index;
        }
    }
    std::stringstream errorString;
    errorString << "Could not find Index with name " << name << std::endl;
    for (int i = 0; i < m_indexCount; ++i) {
        TableIndex *index = m_indexes[i];
        errorString << index->getName() << std::endl;
    }
    throwFatalException( "%s", errorString.str().c_str());
}

std::vector<TableIndex*> PersistentTable::allIndexes() const {
    std::vector<TableIndex*> retval;
    for (int i = 0; i < m_indexCount; i++)
        retval.push_back(m_indexes[i]);

    return retval;
}

void PersistentTable::onSetColumns() {
    if (m_allowNulls != NULL) delete[] m_allowNulls;
    m_allowNulls = new bool[m_columnCount];
    for (int i = m_columnCount - 1; i >= 0; --i) {
        m_allowNulls[i] = m_schema->columnAllowNull(i);
    }
}

/*
 * Implemented by persistent table and called by Table::loadTuplesFrom
 * to do additional processing for views and Export
 */
void PersistentTable::processLoadedTuple(bool allowExport, TableTuple &tuple) {
    
    //VOLT_INFO("in processLoadedTuple()."); 
    
#ifdef ANTICACHE
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    eviction_manager->updateTuple(this, &m_tmpTarget1, true); 
#endif
    
    // handle any materialized views
    for (int i = 0; i < m_views.size(); i++) {
        m_views[i]->processTupleInsert(m_tmpTarget1);
    }

    // if EL is enabled, append the tuple to the buffer
    if (allowExport && m_exportEnabled) {
        appendToELBuffer(m_tmpTarget1, m_tsSeqNo++,
                         TupleStreamWrapper::INSERT);
    }
    
    // Account for non-inlined memory allocated via bulk load or recovery
    if (m_schema->getUninlinedObjectColumnCount() != 0)
    {
        m_nonInlinedMemorySize += tuple.getNonInlinedMemorySize();
    }
}

/*
 * Implemented by persistent table and called by Table::loadTuplesFrom
 * to do add tuples to indexes
 */
void PersistentTable::populateIndexes(int tupleCount) 
{
    // populate indexes. walk the contiguous memory in the inner loop.
    for (int i = m_indexCount - 1; i >= 0;--i) {
        TableIndex *index = m_indexes[i];
        for (int j = 0; j < tupleCount; ++j) {
            m_tmpTarget1.move(dataPtrForTuple((int) m_usedTuples + j));
            index->addEntry(&m_tmpTarget1);
        }
    }
}

size_t PersistentTable::appendToELBuffer(TableTuple &tuple, int64_t seqNo,
                                         TupleStreamWrapper::Type type) {

    return m_wrapper->appendTuple(m_executorContext->m_lastCommittedTxnId,
                                  m_executorContext->currentTxnId(),
                                  seqNo,
                                  m_executorContext->currentTxnTimestamp(),
                                  tuple, type);
}

/**
 * Flush tuple stream wrappers. A negative time instructs an
 * immediate flush.
 */
void PersistentTable::flushOldTuples(int64_t timeInMillis)
{
    if (m_exportEnabled && m_wrapper) {
        m_wrapper->periodicFlush(timeInMillis,
                                 m_executorContext->m_lastTickTime,
                                 m_executorContext->m_lastCommittedTxnId,
                                 m_executorContext->currentTxnId());
    }
}

StreamBlock*
PersistentTable::getCommittedExportBytes()
{
    if (m_exportEnabled && m_wrapper)
    {
        return m_wrapper->getCommittedExportBytes();
    }
    return NULL;
}

bool
PersistentTable::releaseExportBytes(int64_t releaseOffset)
{
    if (m_exportEnabled && m_wrapper)
    {
        return m_wrapper->releaseExportBytes(releaseOffset);
    }
    return false;
}

void
PersistentTable::resetPollMarker()
{
    if (m_exportEnabled && m_wrapper)
    {
        m_wrapper->resetPollMarker();
    }
}

voltdb::TableStats* PersistentTable::getTableStats() {
    return &stats_;
}

/**
 * Switch the table to copy on write mode. Returns true if the table was already in copy on write mode.
 */
bool PersistentTable::activateCopyOnWrite(TupleSerializer *serializer, int32_t partitionId) {
    if (m_COWContext != NULL) {
        return true;
    }
    if (m_tupleCount == 0) {
        return false;
    }
    m_COWContext.reset(new CopyOnWriteContext( this, serializer, partitionId));
    return false;
}

/**
 * Attempt to serialize more tuples from the table to the provided output stream.
 * Returns true if there are more tuples and false if there are no more tuples waiting to be
 * serialized.
 */
bool PersistentTable::serializeMore(ReferenceSerializeOutput *out) {
    if (m_COWContext == NULL) {
        return false;
    }

    const bool hasMore = m_COWContext->serializeMore(out);
    if (!hasMore) {
        m_COWContext.reset(NULL);
    }

    return hasMore;
}

/**
 * Create a recovery stream for this table. Returns true if the table already has an active recovery stream
 */
bool PersistentTable::activateRecoveryStream(int32_t tableId) {
    if (m_recoveryContext != NULL) {
        return true;
    }
    m_recoveryContext.reset(new RecoveryContext( this, tableId ));
    return false;
}

/**
 * Serialize the next message in the stream of recovery messages. Returns true if there are
 * more messages and false otherwise.
 */
void PersistentTable::nextRecoveryMessage(ReferenceSerializeOutput *out) {
    if (m_recoveryContext == NULL) {
        return;
    }

    const bool hasMore = m_recoveryContext->nextMessage(out);
    if (!hasMore) {
        m_recoveryContext.reset(NULL);
    }
}

/**
 * Process the updates from a recovery message
 */
void PersistentTable::processRecoveryMessage(RecoveryProtoMsg* message, Pool *pool, bool allowExport) {
    switch (message->msgType()) {
    case voltdb::RECOVERY_MSG_TYPE_SCAN_TUPLES: {
        if (activeTupleCount() == 0) {
            uint32_t tupleCount = message->totalTupleCount();
            for (int i = 0; i < m_indexCount; i++) {
                m_indexes[i]->ensureCapacity(tupleCount);
            }
        }
        loadTuplesFromNoHeader( allowExport, *message->stream(), pool);
        break;
    }
    default:
        throwFatalException("Attempted to process a recovery message of unknown type %d", message->msgType());
    }
}

/**
 * Create a tree index on the primary key and then iterate it and hash
 * the tuple data.
 */
size_t PersistentTable::hashCode() {
    TableIndexScheme sourceScheme = m_pkeyIndex->getScheme();
    sourceScheme.setTree();
    boost::scoped_ptr<TableIndex> pkeyIndex(TableIndexFactory::getInstance(sourceScheme));
    TableIterator iter(this);
    TableTuple tuple(schema());
    while (iter.next(tuple)) {
        pkeyIndex->addEntry(&tuple);
    }

    pkeyIndex->moveToEnd(true);

    size_t hashCode = 0;
    while (true) {
         tuple = pkeyIndex->nextValue();
         if (tuple.isNullTuple()) {
             break;
         }
         tuple.hashCode(hashCode);
    }
    return hashCode;
}

}
