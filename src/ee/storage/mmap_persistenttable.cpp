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
  PersistentTable(ctx,name,exportEnabled), // enable MMAP'ed pool
  m_name(name)
  {
    const unsigned int DEFAULT_MMAP_SIZE = 256*1024*1024;

    m_data_manager = new MMAPMemoryManager(DEFAULT_MMAP_SIZE, m_executorContext->getDBDir()+"/"+name+"_Data", true); // backed by a file
  }

  inline void MMAP_PersistentTable::allocateNextBlock() {
    #ifdef MEMCHECK
    int bytes = m_schema->tupleLength() + TUPLE_HEADER_SIZE;
    #else
    int bytes = m_tableAllocationTargetSize;
    #endif

    char* memory = NULL ;

    VOLT_WARN("MMAP : PId:: %d Table: %s  Bytes:: %d ",
	       m_executorContext->getPartitionId(), this->name().c_str(), bytes);

    memory = (char*)m_data_manager->allocate(bytes);
    
    VOLT_WARN("MMAP : Table: %s :: Memory Pointer : %p ",
	       this->name().c_str(), memory);

    if (memory == NULL) {
      VOLT_ERROR("MMAP : initialization error.");
      throwFatalException("Failed to map file.");
    }

    /** Push into m_data **/
    m_data.push_back(memory);

    m_allocatedTuples += m_tuplesPerBlock;
  }


}
