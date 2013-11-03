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

#ifndef HSTOREMMAPPERSISTENTTABLE_H
#define HSTOREMMAPPERSISTENTTABLE_H

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
class MMAP_PersistentTable : public PersistentTable {
    friend class TableFactory;

    private:
    // no default ctor, no copy, no assignment
    MMAP_PersistentTable();
    MMAP_PersistentTable(MMAP_PersistentTable const&);
    MMAP_PersistentTable operator=(MMAP_PersistentTable const&);

    uint32_t m_tableRequestCount;

    protected:
    MMAP_PersistentTable(ExecutorContext *ctx, bool exportEnabled);

    void allocateNextBlock();

};

#define PATH_LENGTH 1024

inline void MMAP_PersistentTable::allocateNextBlock() {
#ifdef MEMCHECK
    int bytes = m_schema->tupleLength() + TUPLE_HEADER_SIZE;
#else
    int bytes = m_tableAllocationTargetSize;
#endif

    int MMAP_fd, iret ;
    char* memory = NULL ;
    string MMAP_Dir, MMAP_file_name; 
    //long file_size;
    const string NVM_fileType(".nvm");

    /** Get location for mmap'ed files **/
    MMAP_Dir = m_executorContext->getDBDir();
    //file_size = m_executorContext->getFileSize();

    VOLT_WARN("MMAP : DBdir:: --%s--\n", MMAP_Dir.c_str());
    //VOLT_WARN("MMAP : File Size :: %ld\n", file_size);

#ifdef _WIN32
    const std::string pathSeparator("\\");
#else
    const std::string pathSeparator("/");
#endif

    std::stringstream m_tableRequestCountStringStream;
    m_tableRequestCountStringStream << m_tableRequestCount;

    // Initialize MMAP_Dir to local dir if it is not initialized
    if(MMAP_Dir.empty())
        MMAP_Dir = ".";

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

    iret = ftruncate(MMAP_fd, bytes) ;
    if(iret < 0){
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

}

#endif
