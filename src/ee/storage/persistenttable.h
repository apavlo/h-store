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

#ifndef HSTOREPERSISTENTTABLE_H
#define HSTOREPERSISTENTTABLE_H

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <string>
#include <map>
#include <vector>
#include "boost/shared_ptr.hpp"
#include "boost/scoped_ptr.hpp"
#include "common/ids.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/TupleStreamWrapper.h"
#include "storage/TableStats.h"
#include "storage/PersistentTableStats.h"
#include "storage/CopyOnWriteContext.h"
#include "storage/RecoveryContext.h"


namespace voltdb {

class TableColumn;
class TableIndex;
class TableIterator;
class TableFactory;
class TupleSerializer;
class SerializeInput;
class Topend;
class ReferenceSerializeOutput;
class ExecutorContext;
class MaterializedViewMetadata;
class RecoveryProtoMsg;
    
#ifdef ANTICACHE
class EvictedTable;
class AntiCacheEvictionManager; 
class EvictionIterator;
#endif

/**
 * Represents a non-temporary table which permanently resides in
 * storage and also registered to Catalog (see other documents for
 * details of Catalog). PersistentTable has several additional
 * features to Table.  It has indexes, constraints to check NULL and
 * uniqueness as well as undo logs to revert changes.
 *
 * PersistentTable can have one or more Indexes, one of which must be
 * Primary Key Index. Primary Key Index is same as other Indexes except
 * that it's used for deletion and updates. Our Execution Engine collects
 * Primary Key values of deleted/updated tuples and uses it for specifying
 * tuples, assuming every PersistentTable has a Primary Key index.
 *
 * Currently, constraints are not-null constraint and unique
 * constraint.  Not-null constraint is just a flag of TableColumn and
 * checked against insertion and update. Unique constraint is also
 * just a flag of TableIndex and checked against insertion and
 * update. There's no rule constraint or foreign key constraint so far
 * because our focus is performance and simplicity.
 *
 * To revert changes after execution, PersistentTable holds UndoLog.
 * PersistentTable does eager update which immediately changes the
 * value in data and adds an entry to UndoLog. We chose eager update
 * policy because we expect reverting rarely occurs.
 */
class PersistentTable : public Table {
    friend class TableFactory;
    friend class TableTuple;
    friend class TableIndex;
    friend class TableIterator;
    friend class PersistentTableStats;
    
#ifdef ANTICACHE
    friend class AntiCacheEvictionManager;
    friend class IndexScanExecutor;
#endif

  private:
    // no default ctor, no copy, no assignment
    PersistentTable();
    PersistentTable(PersistentTable const&);
    PersistentTable operator=(PersistentTable const&);

#ifdef STORAGE_MMAP
    uint32_t m_tableRequestCount;
#endif

  public:
    virtual ~PersistentTable();

    // ------------------------------------------------------------------
    // OPERATIONS
    // ------------------------------------------------------------------
    void deleteAllTuples(bool freeAllocatedStrings);
    bool insertTuple(TableTuple &source);

    /*
     * Inserts a Tuple without performing an allocation for the
     * uninlined strings.
     */
    void insertTupleForUndo(TableTuple &source, size_t elMark);

    /*
     * Note that inside update tuple the order of sourceTuple and
     * targetTuple is swapped when making calls on the indexes. This
     * is just an inconsistency in the argument ordering.
     */
    bool updateTuple(TableTuple &sourceTuple, TableTuple &targetTuple,
                     bool updatesIndexes);

    /*
     * Identical to regular updateTuple except no memory management
     * for unlined columns is performed because that will be handled
     * by the UndoAction.
     */
    void updateTupleForUndo(TableTuple &sourceTuple, TableTuple &targetTuple,
                            bool revertIndexes, size_t elMark);

    /*
     * Delete a tuple by looking it up via table scan or a primary key
     * index lookup.
     */
    bool deleteTuple(TableTuple &tuple, bool freeAllocatedStrings);
    void deleteTupleForUndo(voltdb::TableTuple &tupleCopy, size_t elMark);

    /*
     * Lookup the address of the tuple that is identical to the specified tuple.
     * Does a primary key lookup or table scan if necessary.
     */
    voltdb::TableTuple lookupTuple(TableTuple tuple);

    // ------------------------------------------------------------------
    // INDEXES
    // ------------------------------------------------------------------
    virtual int indexCount() const { return m_indexCount; }
    virtual int uniqueIndexCount() const { return m_uniqueIndexCount; }
    virtual std::vector<TableIndex*> allIndexes() const;
    virtual TableIndex *index(std::string name);
    virtual TableIndex *primaryKeyIndex() { return m_pkeyIndex; }
    virtual const TableIndex *primaryKeyIndex() const { return m_pkeyIndex; }

    // ------------------------------------------------------------------
    // UTILITY
    // ------------------------------------------------------------------
    std::string tableType() const;
    virtual std::string debug();

    int partitionColumn() { return m_partitionColumn; }

    /** inlined here because it can't be inlined in base Table, as it
     *  uses Tuple.copy.
     */
    TableTuple& getTempTupleInlined(TableTuple &source);

    // Export-related inherited methods
    virtual void flushOldTuples(int64_t timeInMillis);
    virtual StreamBlock* getCommittedExportBytes();
    virtual bool releaseExportBytes(int64_t releaseOffset);
    virtual void resetPollMarker();

    /** Add a view to this table */
    void addMaterializedView(MaterializedViewMetadata *view);

    /**
     * Switch the table to copy on write mode. Returns true if the table was already in copy on write mode.
     */
    bool activateCopyOnWrite(TupleSerializer *serializer, int32_t partitionId);

    /**
     * Create a recovery stream for this table. Returns true if the table already has an active recovery stream
     */
    bool activateRecoveryStream(int32_t tableId);

    /**
     * Serialize the next message in the stream of recovery messages. Returns true if there are
     * more messages and false otherwise.
     */
    void nextRecoveryMessage(ReferenceSerializeOutput *out);

    /**
     * Process the updates from a recovery message
     */
    void processRecoveryMessage(RecoveryProtoMsg* message, Pool *pool, bool allowExport);

    /**
     * Attempt to serialize more tuples from the table to the provided
     * output stream.  Returns true if there are more tuples and false
     * if there are no more tuples waiting to be serialized.
     */
    bool serializeMore(ReferenceSerializeOutput *out);

    /**
     * Create a tree index on the primary key and then iterate it and hash
     * the tuple data.
     */
    size_t hashCode();

    /**
     * Get the current offset in bytes of the export stream for this Table
     * since startup.
     */
    void getExportStreamSequenceNo(long &seqNo, size_t &streamBytesUsed) {
        seqNo = m_exportEnabled ? m_tsSeqNo : -1;
        streamBytesUsed = m_wrapper ? m_wrapper->bytesUsed() : 0;
    }

    /**
     * Set the current offset in bytes of the export stream for this Table
     * since startup (used for rejoin/recovery).
     */
    virtual void setExportStreamPositions(int64_t seqNo, size_t streamBytesUsed) {
        // assume this only gets called from a fresh rejoined node
        assert(m_tsSeqNo == 0);
        m_tsSeqNo = seqNo;
        if (m_wrapper)
            m_wrapper->setBytesUsed(streamBytesUsed);
    }
    
    // ------------------------------------------------------------------
    // ANTI-CACHING OPERATIONS
    // ------------------------------------------------------------------
    #ifdef ANTICACHE
    void setEvictedTable(voltdb::Table *evictedTable);
    voltdb::Table* getEvictedTable(); 
    bool evictBlockToDisk(const long block_size, int num_blocks);
    bool readEvictedBlock(int16_t block_id, int32_t tuple_offset);
    bool mergeUnevictedTuples();
    
    // needed for LRU chain eviction
    void setNewestTupleID(uint32_t id); 
    void setOldestTupleID(uint32_t id); 
    uint32_t getNewestTupleID(); 
    uint32_t getOldestTupleID();
    void setNumTuplesInEvictionChain(int num_tuples);
    int getNumTuplesInEvictionChain(); 
    #endif
    
    void setEntryToNewAddressForAllIndexes(const TableTuple *tuple, const void* address);

protected:

#ifdef STORAGE_MMAP
    void allocateNextBlock();
#endif
    
    size_t allocatedBlockCount() const {
        return m_data.size();
    }
    
    // ------------------------------------------------------------------
    // FROM PIMPL
    // ------------------------------------------------------------------
    void insertIntoAllIndexes(TableTuple *tuple);
    void deleteFromAllIndexes(TableTuple *tuple);
    void updateFromAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);

    bool tryInsertOnAllIndexes(TableTuple *tuple);
    bool tryUpdateOnAllIndexes(TableTuple &targetTuple, const TableTuple &sourceTuple);

    bool checkNulls(TableTuple &tuple) const;
    
    size_t appendToELBuffer(TableTuple &tuple, int64_t seqNo, TupleStreamWrapper::Type type);

    PersistentTable(ExecutorContext *ctx, bool exportEnabled);
    void onSetColumns();

    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do additional processing for views and Export
     */
    virtual void processLoadedTuple(bool allowExport, TableTuple &tuple);

    /*
     * Implemented by persistent table and called by Table::loadTuplesFrom
     * to do add tuples to indexes
     */
    virtual void populateIndexes(int tupleCount);

    // pointer to current transaction id and other "global" state.
    // abstract this out of VoltDBEngine to avoid creating dependendencies
    // between the engine and the storage layers - which complicate test.
    ExecutorContext *m_executorContext;

    // CONSTRAINTS
    TableIndex** m_uniqueIndexes;
    int m_uniqueIndexCount;
    bool* m_allowNulls;

    // INDEXES
    TableIndex** m_indexes;
    int m_indexCount;
    TableIndex *m_pkeyIndex;

    // temporary for tuplestream stuff
    TupleStreamWrapper *m_wrapper;
    int64_t m_tsSeqNo;
    
    // ANTI-CACHE VARIABLES
    #ifdef ANTICACHE
    voltdb::Table *m_evictedTable;
    
    std::map<int16_t, int16_t> m_unevictedBlockIDs; 
//    std::vector<int16_t> m_unevictedBlockIDs;
    std::vector<char*> m_unevictedBlocks;
    std::vector<int32_t> m_mergeTupleOffset; 
    
    std::map<int, int> m_unevictedTuplesPerBlocks;
    
    char* m_unevictedTuples; 
    int m_numUnevictedTuples; 
    
    uint32_t m_oldestTupleID; 
    uint32_t m_newestTupleID; 
    
    int m_numTuplesInEvictionChain;
    bool m_blockMerge;
    
    #endif
    
    // partition key
    int m_partitionColumn;
    
    // TODO: Partition id of where this table is stored in
    int32_t m_partitionId;

    // list of materialized views that are sourced from this table
    std::vector<MaterializedViewMetadata *> m_views;

    // STATS
    voltdb::PersistentTableStats stats_;
    voltdb::TableStats* getTableStats();

    // is Export enabled
    bool m_exportEnabled;
    
    // Snapshot stuff
    boost::scoped_ptr<CopyOnWriteContext> m_COWContext;

    //Recovery stuff
    boost::scoped_ptr<RecoveryContext> m_recoveryContext;
};

inline TableTuple& PersistentTable::getTempTupleInlined(TableTuple &source) {
    assert (m_tempTuple.m_data);
    m_tempTuple.copy(source);
    return m_tempTuple;
}

#ifdef STORAGE_MMAP
inline void PersistentTable::allocateNextBlock() {
#ifdef MEMCHECK
    int bytes = m_schema->tupleLength() + TUPLE_HEADER_SIZE;
#else
    int bytes = m_tableAllocationTargetSize;
#endif

    int MMAP_fd, ret ;
    char* memory = NULL ;
    string MMAP_Dir, MMAP_file_name; 
    //long file_size;
    const string NVM_fileType(".nvm");

    /** Get location for mmap'ed files **/
    MMAP_Dir = m_executorContext->getDBDir();
    //file_size = m_executorContext->getFileSize();

    VOLT_WARN("MMAP : DBdir:: %s\n", MMAP_Dir.c_str());
    //VOLT_WARN("MMAP : File Size :: %ld\n", file_size);

#ifdef _WIN32
    const std::string pathSeparator("\\");
#else
    const std::string pathSeparator("/");
#endif

    std::stringstream m_tableRequestCountStringStream;
    m_tableRequestCountStringStream << m_tableRequestCount;

    /** Get an unique file object for each <Table,Table_Request_Index> **/
    MMAP_file_name  = MMAP_Dir + pathSeparator ;
    MMAP_file_name += this->name() + "_" + m_tableRequestCountStringStream.str();
    MMAP_file_name += NVM_fileType ;
    
    /** Increment Table Request Count **/
    m_tableRequestCount++;
    
    VOLT_WARN("MMAP : MMAP_file_name :: %s\n", MMAP_file_name.c_str());
    
    MMAP_fd = open(MMAP_file_name.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP );
    if (MMAP_fd < 0) {
        VOLT_ERROR("MMAP : initialization error.");
        VOLT_ERROR("Failed to open file %s : %s", MMAP_file_name.c_str(), strerror(errno));
        throwFatalException("Failed to open file in directory %s.", MMAP_Dir.c_str());
    }
    
    ret = ftruncate(MMAP_fd, bytes) ;
    if(ret < 0){
        VOLT_ERROR("MMAP : initialization error.");
        VOLT_ERROR("Failed to truncate file %d : %s", MMAP_fd, strerror(errno));
        throwFatalException("Failed to truncate file in directory %s.", MMAP_Dir.c_str());
    }
    
    /** Allocation **/
    memory = (char*) mmap(0, bytes , PROT_READ | PROT_WRITE , MAP_PRIVATE, MMAP_fd, 0);
    
    if (memory == MAP_FAILED) {
        VOLT_ERROR("MMAP : initialization error.");
        VOLT_ERROR("Failed to map file into memory %d : %s", MMAP_fd, strerror(errno));
        throwFatalException("Failed to map file in directory %s.", MMAP_Dir.c_str());
    }
    
    /** Push into m_data **/
    m_data.push_back(memory);
    
    /** Deallocation **/
    // TODO: Where to deallocate ?
    // munmap(addr, bytes);
    
    VOLT_WARN("MMAP : Host:: %d Site:: %d PId:: %d Index:: %d Table: %s  Bytes:: %d \n",
          m_executorContext->getHostId(), m_executorContext->getSiteId(), m_executorContext->getPartitionId(),
          m_tableRequestCount, this->name().c_str(), bytes);
    
    m_allocatedTuples += m_tuplesPerBlock;
    
    /**
    * Closing the file descriptor does not unmap the region as map() automatically adds a reference
    */
    close(MMAP_fd);
    
    }
#endif // STORAGE_MMAP

}

#endif
