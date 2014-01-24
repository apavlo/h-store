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

#ifndef HSTORETIMEWINDOW_H
#define HSTORETIMEWINDOW_H

#include "storage/persistenttable.h"
#include "streaming/WindowTableTemp.h"
#include "streaming/WindowIterator.h"

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
class TableTuple;

class TimeWindow : public WindowTableTemp {
	friend class TableFactory;
	friend class TableTuple;
	friend class TableIndex;
	friend class TableIterator;
	friend class PersistentTableStats;

  private:
	// no default ctor, no copy, no assignment
	TimeWindow();
	TimeWindow(TimeWindow const&);

  public:
	~TimeWindow();
	TimeWindow(ExecutorContext *ctx, bool exportEnabled, int windowSize, int slideSize = 1);

	int findTSColumn();
	int getTSColumn();
	/**
	// ------------------------------------------------------------------
	// OPERATIONS
	// ------------------------------------------------------------------
	bool insertTuple(TableTuple &source);
	void insertTupleForUndo(TableTuple &source, size_t elMark);
	//bool updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes);
	//void updateTupleForUndo(TableTuple &sourceTuple, TableTuple &targetTuple,
	//						bool revertIndexes, size_t elMark);
	bool deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings);
	void deleteTupleForUndo(voltdb::TableTuple &tupleCopy, size_t elMark);

	std::string debug();
	*/

  protected:
	int m_tsColumn;

};
}

#endif
