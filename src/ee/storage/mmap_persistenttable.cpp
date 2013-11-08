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
#include "storage/mmap_persistenttable.h"
#include <sys/stat.h>
#include <unistd.h>

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

  MMAP_PersistentTable::MMAP_PersistentTable(ExecutorContext *ctx, const std::string &name, bool exportEnabled) :
  PersistentTable(ctx,exportEnabled),
  m_dataBlockCount(0), m_poolBlockCount(0),
  m_dataStoragePointer(NULL), m_poolStoragePointer(NULL),
  m_name(name)
  {

    initDataStorage();

  }

  /*
   * Invoked only once to set up MMAP Data Storage
   */
  void MMAP_PersistentTable::initDataStorage(){

    int MMAP_fd, ret;
    std::string MMAP_Dir, MMAP_file_name;
    //long file_size;
    char* memory;

    const std::string NVM_fileType(".nvm");
    const std::string pathSeparator("/");

    off_t file_size = 16 * 1024 * 1024 ; // 16 MB

    /** Get location for mmap'ed files **/
    MMAP_Dir = m_executorContext->getDBDir();
    //file_size = m_executorContext->getFileSize();

    VOLT_WARN("MMAP : DBdir:: --%s--\n", MMAP_Dir.c_str());
    //VOLT_WARN("MMAP : File Size :: %ld\n", file_size);

    if(MMAP_Dir.empty()){
      VOLT_ERROR("MMAP : initialization error.");
      VOLT_ERROR("MMAP_Dir is empty \n");
      throwFatalException("Failed to get DB Dir.");
    }

    /** Get an unique file object for the table **/
    MMAP_file_name  = MMAP_Dir + pathSeparator ;
    MMAP_file_name += m_name ;
    MMAP_file_name += NVM_fileType ;

    VOLT_WARN("MMAP : MMAP_file_name :: %s ", MMAP_file_name.c_str());

    MMAP_fd = open(MMAP_file_name.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP );
    if (MMAP_fd < 0) {
      VOLT_ERROR("MMAP : initialization error.");
      VOLT_ERROR("Failed to open file %s : %s", MMAP_file_name.c_str(), strerror(errno));
      throwFatalException("Failed to open file in directory %s.", MMAP_Dir.c_str());
    }

    /*
    struct stat m_info;
    
    if (fstat(MMAP_fd, &m_info) != 0){
      VOLT_ERROR("fstat() error");
    }
    else {
      VOLT_WARN("FD: %d Inode:  %d Dev : %d Links : %d ",
		MMAP_fd, (int) m_info.st_ino, (int) m_info.st_dev, (int)m_info.st_nlink);
    }
    */
    
    ret = ftruncate(MMAP_fd, file_size) ;
    if(ret < 0){
      VOLT_ERROR("MMAP : initialization error.");
      VOLT_ERROR("Failed to truncate file %d : %s", MMAP_fd, strerror(errno));
      throwFatalException("Failed to truncate file in directory %s.", MMAP_Dir.c_str());
    }

    /*
     * Create a single large mapping and chunk it later during block allocation
     */
    memory = (char*) mmap(0, file_size , PROT_READ | PROT_WRITE , MAP_PRIVATE, MMAP_fd, 0);

    if (memory == MAP_FAILED) {
      VOLT_ERROR("MMAP : initialization error.");
      VOLT_ERROR("Failed to map file into memory.");
      throwFatalException("Failed to map file.");
    }

    // Setting mmap pointer in table
    setDataStoragePointer(memory);

    /**
     * Closing the file descriptor does not unmap the region as mmap() automatically adds a reference
     */
    close(MMAP_fd);

  }

  inline void MMAP_PersistentTable::allocateNextBlock() {
    #ifdef MEMCHECK
    int bytes = m_schema->tupleLength() + TUPLE_HEADER_SIZE;
    #else
    int bytes = m_tableAllocationTargetSize;
    #endif

    int m_index, fileOffset;
    char* memory = NULL ;

    m_index = getDataBlockCount();
    VOLT_TRACE("MMAP : PId:: %d Index:: %d Table: %s  Bytes:: %d ",
    m_executorContext->getPartitionId(), m_index, this->name().c_str(), bytes);

    /**
     * Allocation at different offsets
     * We assume that the index never overflows in a single execution !
     * No munmap done - this will happen when process exits
     */
    if(m_dataStorageMap.size() == 0)
      fileOffset = 0;
    else{
      // current file offset = past file offset + past allocation size
      fileOffset = m_dataStorageMap[m_index-1].first + m_dataStorageMap[m_index-1].second ;
    }

    /** Update METADATA map and do the allocation **/
    m_dataStorageMap[m_index] = std::make_pair(fileOffset, bytes);

    memory = ((char*)getDataStoragePointer()) + fileOffset;

    // DEBUG
    printMetadata();
    
    VOLT_TRACE("MMAP : Index:: %d Table: %s :: Memory Pointer : %p ",
    m_index, this->name().c_str(), memory);

    if (memory == NULL) {
      VOLT_ERROR("MMAP : initialization error.");
      throwFatalException("Failed to map file.");
    }

    /** Push into m_data **/
    m_data.push_back(memory);

    m_allocatedTuples += m_tuplesPerBlock;
  }


}
