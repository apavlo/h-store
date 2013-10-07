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
#include <list>

#include "boost/scoped_ptr.hpp"
#include "storage/WindowTable.h"

#ifdef ANTICACHE
#include "boost/timer.hpp"
#include "anticache/EvictedTable.h"
#include "anticache/AntiCacheDB.h"
#include "anticache/EvictionIterator.h"
#include "anticache/UnknownBlockAccessException.h"
#endif

#include <map>

namespace voltdb {
/**
 * This value has to match the value in CopyOnWriteContext.cpp
 */
#define TABLE_BLOCKSIZE 2097152
#define MAX_EVICTED_TUPLE_SIZE 2500

WindowTable::WindowTable(ExecutorContext *ctx, bool exportEnabled, int windowSize) : PersistentTable(ctx, exportEnabled)
{
	windowQueue = std::list<TableTuple *>();
	this->windowSize = windowSize;
}

WindowTable::~WindowTable()
{
	//~PersistentTable();
}

void WindowTable::deleteAllTuples(bool freeAllocatedStrings)
{
	PersistentTable::deleteAllTuples(freeAllocatedStrings);
	windowQueue.clear();
}

/**
 *
 */
bool WindowTable::insertTuple(TableTuple &source)
{
	TableTuple* t;
	while(windowQueue.size() >= windowSize)
	{
		t = windowQueue.front();
		PersistentTable::deleteTuple(*t, true);
		windowQueue.pop_front();
	}
	windowQueue.push_back(&source);
	return PersistentTable::insertTuple(source);
}

void WindowTable::insertTupleForUndo(TableTuple &source, size_t elMark)
{
	TableTuple* t;
	while(windowQueue.size() >= windowSize)
	{
		t = windowQueue.front();
		PersistentTable::deleteTuple(*t, elMark);
		windowQueue.pop_front();
	}
	windowQueue.push_back(&source);
	PersistentTable::insertTupleForUndo(source, elMark);
}

bool WindowTable::updateTuple(TableTuple &source, TableTuple &target, bool updatesIndexes)
{
	for(std::list<TableTuple *>::iterator it = windowQueue.begin(); it != windowQueue.end(); it++)
	{
		if(*it == &source)
		{
			*it = &target;
		}
	}
	return PersistentTable::updateTuple(source, target, updatesIndexes);
}

void WindowTable::updateTupleForUndo(TableTuple &sourceTuple, TableTuple &targetTuple,
							bool revertIndexes, size_t elMark)
{
	for(std::list<TableTuple *>::iterator it = windowQueue.begin(); it != windowQueue.end(); it++)
	{
		if(*it == &sourceTuple)
		{
			*it = &targetTuple;
		}
	}
	return PersistentTable::updateTupleForUndo(sourceTuple, targetTuple, revertIndexes, elMark);
}

bool WindowTable::deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings)
{
	for(std::list<TableTuple *>::iterator it = windowQueue.begin(); it != windowQueue.end(); it++)
	{
		if(*it == &tuple)
		{
			it = windowQueue.erase(it);
		}
	}
	return PersistentTable::deleteTuple(tuple, deleteAllocatedStrings);
}

void WindowTable::deleteTupleForUndo(voltdb::TableTuple &tupleCopy, size_t elMark)
{
	for(std::list<TableTuple *>::iterator it = windowQueue.begin(); it != windowQueue.end(); it++)
	{
		if(*it == &tupleCopy)
		{
			it = windowQueue.erase(it);
		}
	}
	return PersistentTable::deleteTupleForUndo(tupleCopy, elMark);
}


}

