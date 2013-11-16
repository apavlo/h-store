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
#include <utility>
#include "boost/shared_ptr.hpp"
#include "boost/scoped_ptr.hpp"
#include "common/ids.h"
#include "common/valuevector.h"
#include "common/tabletuple.h"
#include "common/MMAPMemoryManager.h"
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

  class PersistentTable ;

  #ifdef ANTICACHE
  class EvictedTable;
  class AntiCacheEvictionManager;
  class EvictionIterator;
  #endif

  /**
   * PersistentTable Doc ::
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
    friend class ExecutorContext;

  public:
    // no default ctor, no copy, no assignment
    MMAP_PersistentTable();
    MMAP_PersistentTable(MMAP_PersistentTable const&);
    MMAP_PersistentTable operator=(MMAP_PersistentTable const&);
    
  protected:
    MMAP_PersistentTable(ExecutorContext *ctx, const std::string &name, bool exportEnabled);

    void allocateNextBlock();

  private:  
    const std::string m_name;    

  };

}

#endif /* HSTOREMMAPPERSISTENTTABLE_H */
