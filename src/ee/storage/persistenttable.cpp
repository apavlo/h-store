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
#include "common/Pool.hpp"
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
    Table(TABLE_BLOCKSIZE,ctx->isMMAPEnabled()), m_executorContext(ctx), m_uniqueIndexes(NULL), m_uniqueIndexCount(0), m_allowNulls(NULL),
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
    m_blockMerge = ctx->isBlockMerge();
    m_batchEvicted = false;
#endif

    if (exportEnabled) {
        m_wrapper = new TupleStreamWrapper(m_executorContext->m_partitionId,
                m_executorContext->m_siteId,
                m_executorContext->m_lastTickTime);
    }

    m_pool = new Pool();
}

PersistentTable::PersistentTable(ExecutorContext *ctx, const std::string name, bool exportEnabled) :
    Table(TABLE_BLOCKSIZE,ctx->isMMAPEnabled()), m_executorContext(ctx), m_uniqueIndexes(NULL), m_uniqueIndexCount(0), m_allowNulls(NULL),
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
    m_blockMerge = ctx->isBlockMerge();
    m_batchEvicted = false;
#endif

    if (exportEnabled) {
        m_wrapper = new TupleStreamWrapper(m_executorContext->m_partitionId,
                m_executorContext->m_siteId,
                m_executorContext->m_lastTickTime);
    }

    /**
     *  Choosing whether to use malloc Pool or MMAP Pool
     */
    const size_t DEFAULT_MMAP_SIZE = 256 * 1024 * 1024;

    if(m_executorContext->isMMAPEnabled() == false)
        m_pool = new Pool();
    else
        m_pool = new Pool(DEFAULT_MMAP_SIZE, 1024, m_executorContext->getDBDir()+"/"+name, true); // Need a name - backed by a file
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
//     if (m_evictedTable) delete m_evictedTable;
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

void PersistentTable::setBatchEvicted(bool batchEvicted) {
    VOLT_INFO("Marked batch evicted value as '%d' for table '%s'", batchEvicted, this->name().c_str());
    m_batchEvicted = batchEvicted;
}

bool PersistentTable::isBatchEvicted(){
    return m_batchEvicted;
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

AntiCacheDB* PersistentTable::getAntiCacheDB(int level)
{
    return m_executorContext->getAntiCacheDB(level);
}

std::vector<AntiCacheDB*> PersistentTable::allACDBs() const {
    std::vector<AntiCacheDB*> retval;
    int numDBs = (int)m_executorContext->getAntiCacheEvictionManager()->getNumAntiCacheDBs();
    for (int i = 0; i < numDBs; ++i)
        retval.push_back(m_executorContext->getAntiCacheDB(i));

    return retval;
}

int32_t PersistentTable::getTuplesEvicted()
{
    return m_tuplesEvicted;
}

int32_t PersistentTable::getBlocksEvicted()
{
    return m_blocksEvicted;
}

int64_t PersistentTable::getBytesEvicted()
{
    return m_bytesEvicted;
}

int32_t PersistentTable::getTuplesWritten()
{
    return m_tuplesWritten;
}

int32_t PersistentTable::getBlocksWritten()
{
    return m_blocksWritten;
}

int64_t PersistentTable::getBytesWritten()
{
    return m_bytesWritten;
}

void PersistentTable::setTuplesEvicted(int32_t tuplesEvicted)
{
    m_tuplesEvicted = tuplesEvicted ;
}

void PersistentTable::setBlocksEvicted(int32_t blocksEvicted)
{
    m_blocksEvicted = blocksEvicted ;
}

void PersistentTable::setBytesEvicted(int64_t bytesEvicted)
{
    m_bytesEvicted = bytesEvicted ;
}

void PersistentTable::setTuplesWritten(int32_t tuplesWritten)
{
    m_tuplesWritten = tuplesWritten ;
}

void PersistentTable::setBlocksWritten(int32_t blocksWritten)
{
    m_blocksWritten = blocksWritten ;
}

void PersistentTable::setBytesWritten(int64_t bytesWritten)
{
    m_bytesWritten = bytesWritten;
}

std::map<int32_t, int32_t> PersistentTable::getUnevictedBlockIDs()
{
    return m_unevictedBlockIDs;
}

int PersistentTable::isAlreadyUnEvicted(int32_t blockId)
{
    if (m_unevictedBlockIDs.find(blockId) != m_unevictedBlockIDs.end())
        return m_unevictedBlockIDs[blockId];
    else
        return 0;
}

void PersistentTable::insertUnevictedBlockID(std::pair<int32_t,int32_t> pair)
{
    VOLT_INFO("Unevicted pair is %d", pair.first);
    m_unevictedBlockIDs[pair.first] = pair.second;
}

bool PersistentTable::removeUnevictedBlockID(int32_t blockId) {
    if (isAlreadyUnEvicted(blockId)) {
        VOLT_INFO("Reusing blockID %x, so we need to remove it from list", blockId);
        m_unevictedBlockIDs.erase(m_unevictedBlockIDs.find(blockId));
        return true;
    }
    return false;
}

std::vector<char*> PersistentTable::getUnevictedBlocks()
{
    return m_unevictedBlocks;
}

void PersistentTable::insertUnevictedBlock(char* unevicted_tuples)
{
    m_unevictedBlocks.push_back(unevicted_tuples);
}

int32_t PersistentTable::getMergeTupleOffset(int i)
{
    return m_mergeTupleOffset[i];
}

int32_t PersistentTable::getBlockID(int i)
{
    return m_blockIDs[i];
}

char* PersistentTable::getUnevictedBlocks(int i)
{
    return m_unevictedBlocks[i];
}

int PersistentTable::unevictedBlocksSize(){
    return static_cast<int> (m_unevictedBlocks.size());
}

void PersistentTable::insertTupleOffset(int32_t tuple_offset)
{
    m_mergeTupleOffset.push_back(tuple_offset);
}

void PersistentTable::insertBlockID(int32_t ACID)
{
    m_blockIDs.push_back(ACID);
}

int32_t PersistentTable::getTuplesRead()
{
    return m_tuplesRead;
}

void PersistentTable::setTuplesRead(int32_t tuplesRead)
{
    m_tuplesRead = tuplesRead ;
}

bool PersistentTable::mergeStrategy()
{
    return m_blockMerge;
}

voltdb::TableTuple * PersistentTable::getTempTarget1()
{
    return &m_tmpTarget1;
}
void PersistentTable::clearUnevictedBlocks()
{
    m_unevictedBlocks.clear();
}
void PersistentTable::clearUnevictedBlockIDs()
{
    m_unevictedBlockIDs.clear();
}
void PersistentTable::clearUnevictedBlocks(int i)
{
    m_unevictedBlocks.erase(m_unevictedBlocks.begin()+i);
}

void PersistentTable::clearMergeTupleOffsets()
{
    m_mergeTupleOffset.clear();
}

void PersistentTable::clearBlockIDs()
{
    m_blockIDs.clear();
}

int64_t PersistentTable::unevictTuple(ReferenceSerializeInput * in, int j, int merge_tuple_offset, bool blockMerge){
    TableTuple evicted_tuple = m_evictedTable->tempTuple();
    // get a free tuple and increment the count of tuples current used
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;

    int64_t bytesUnevicted = 0;
    
    // This only works if we either merge a single tuple or an entire block. If,
    // in the future, we would like to merge more than one tuple but less than
    // an entire block, this will need to be changed because it destroys the buffer
    // for the single tuple.
    if (!blockMerge) {
        in->getRawPointer(merge_tuple_offset);
    }
        
    bytesUnevicted = m_tmpTarget1.deserializeWithHeaderFrom(*in);

    // Note, this goal of the section below is to get a tuple that points to the tuple in the EvictedTable and has the
    // schema of the evicted tuple. However, the lookup has to be done using the schema of the original (unevicted) version
    m_tmpTarget2 = lookupTuple(m_tmpTarget1);       // lookup the tuple in the table
    //printf("%d\n", m_tmpTarget2.isEvicted());
    if (!m_tmpTarget2.isEvicted()) {
        deleteTupleStorage(m_tmpTarget1);
        return 0;
    }
    evicted_tuple.move(m_tmpTarget2.address());
    static_cast<EvictedTable*>(m_evictedTable)->deleteEvictedTuple(evicted_tuple);             // delete the EvictedTable tuple

    m_tmpTarget1.setEvictedFalse();
    m_tmpTarget1.setDeletedFalse();
    // update the indexes to point to this newly unevicted tuple
    VOLT_TRACE("BEFORE: tuple.isEvicted() = %d", m_tmpTarget1.isEvicted());
    setEntryToNewAddressForAllIndexes(&m_tmpTarget1, m_tmpTarget1.address(), m_tmpTarget2.address());
    updateStringMemory((int)m_tmpTarget1.getNonInlinedMemorySize());

    //deleteFromAllIndexes(&m_tmpTarget1);
    //insertTuple(m_tmpTarget1);

    m_tmpTarget1.setEvictedFalse();
    VOLT_TRACE("AFTER: tuple.isEvicted() = %d", m_tmpTarget1.isEvicted());
    VOLT_TRACE("Merged Tuple: %s", m_tmpTarget1.debug(name()).c_str());
    //VOLT_INFO("tuple size: %d, non-inlined memory size: %d", m_tmpTarget1.tupleLength(), m_tmpTarget1.getNonInlinedMemorySize());
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    // re-insert the tuple back into the eviction chain
    if(j == merge_tuple_offset)  // put it at the back of the chain
        eviction_manager->updateTuple(this, &m_tmpTarget1, true);
    else
        eviction_manager->updateUnevictedTuple(this, &m_tmpTarget1);
    return bytesUnevicted;
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
    /** Don't use MMAP pool **/
    m_tmpTarget1.copyForPersistentInsert(source, NULL); // tuple in freelist must be already cleared
    m_tmpTarget1.setDeletedFalse();
    m_tmpTarget1.setEvictedFalse();

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
    VOLT_TRACE("Registered UndoAction for new tuple in table '%s'", name().c_str());

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
    eviction_manager->updateTuple(this, &source, true);
#endif
}

/*
 * Regular tuple update function that does a copy and allocation for
 * updated strings and creates an UndoAction.
 */
bool PersistentTable::updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes) {
    size_t elMark = 0;

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

    /** TODO : Not Using MMAP pool **/
    target.copyForPersistentUpdate(source, NULL);

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

#ifdef ANTICACHE
    if(m_evictedTable != NULL)
    {
        AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
        eviction_manager->updateTuple(this, &target, false);
    }
#endif

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
#ifndef ANTICACHE_TIMESTAMPS
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    eviction_manager->removeTuple(this, &target); 
#endif
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

        // try secondary indexes
        for (int i = m_indexCount - 1; i >= 0;--i) {
            if (m_indexes[i]->moveToTuple(&tuple)) {
                return m_indexes[i]->nextValueAtKey();
            }
        }

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
            VOLT_ERROR("Failed to update indexes"); 
            throwFatalException("Failed to update tuple in index");
        }
    }
}

void PersistentTable::setEntryToNewAddressForAllIndexes(const TableTuple *tuple, const void* address, const void* oldAddress) {
    for (int i = m_indexCount - 1; i >= 0; --i) {
        VOLT_TRACE("Updating tuple address in index %s.%s [%s]",
                   name().c_str(), m_indexes[i]->getName().c_str(), m_indexes[i]->getTypeName().c_str());
        VOLT_TRACE("address is %p", address);
        if (!m_indexes[i]->setEntryToNewAddress(tuple, address, oldAddress)) {
            VOLT_ERROR("ERROR: Failed to update tuple to new address!");
            throwFatalException("Failed to update tuple to new address in index %s.%s [%s]",
                    name().c_str(), m_indexes[i]->getName().c_str(),
                    m_indexes[i]->getTypeName().c_str());
        }
    }
}

bool PersistentTable::tryInsertOnAllIndexes(TableTuple *tuple) {
    for (int i = m_indexCount - 1; i >= 0; --i) {
        FAIL_IF(!m_indexes[i]->addEntry(tuple)) {
            VOLT_ERROR("Failed to insert into index %s.%s [%s]",
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

void PersistentTable::updateStringMemory(int tupleStringMemorySize) {
    m_nonInlinedMemorySize += tupleStringMemorySize;
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
