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

#include "streaming/TupleWindow.h"
#include "streaming/WindowIterator.h"

namespace voltdb {
/**
 * This value has to match the value in CopyOnWriteContext.cpp
 */
#define TABLE_BLOCKSIZE 2097152
#define MAX_EVICTED_TUPLE_SIZE 2500

TupleWindow::TupleWindow(ExecutorContext *ctx, bool exportEnabled, int windowSize, int slideSize)
	: WindowTableTemp(ctx, exportEnabled, windowSize, slideSize)
{
}

TupleWindow::~TupleWindow()
{
}


/**
 *
 */
bool TupleWindow::insertTuple(TableTuple &source)
{
	if(!(PersistentTable::insertTuple(source)))
		return false;
	markTupleForStaging(m_tmpTarget1);

	if(m_numStagedTuples >= m_slideSize)
	{
		//delete all tuples from the chain until there are exactly the window size of tuples
		while((m_numStagedTuples + m_tupleCount) > m_windowSize)
		{
			if(!(this->removeOldestTupleID()))
				return false;
		}

		TableTuple tuple(m_schema);
		WindowIterator win_itr(this);
		while(win_itr.hasNext())
		{
			win_itr.next(tuple);
			markTupleForWindow(tuple);
		}
		if(hasTriggers())
			setFireTriggers(true);
	}
	return true;
}


void TupleWindow::insertTupleForUndo(TableTuple &source, size_t elMark)
{
	PersistentTable::insertTupleForUndo(source, elMark);
	markTupleForStaging(m_tmpTarget1);

	if(m_numStagedTuples >= m_slideSize)
	{
		//delete all tuples from the chain until there are exactly the window size of tuples
		while((m_numStagedTuples + m_tupleCount) > m_windowSize)
		{
			if(!(this->removeOldestTupleID()))
				return false;
		}

		TableTuple tuple(m_schema);
		WindowIterator win_itr(this);
		while(win_itr.hasNext())
		{
			win_itr.next(tuple);
			markTupleForWindow(tuple);
		}
		if(hasTriggers())
			setFireTriggers(true);
	}
}

bool TupleWindow::deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings)
{
	WindowIterator win_itr(this);
	TableTuple curtup = this->getTempTuple();
	uint32_t currentTupleID = tuple.getTupleID();
	uint32_t nextTupleID = tuple.getNextTupleInChain();

	if(!win_itr.hasNext())
		return false;

	win_itr.next(curtup);
	while(win_itr.hasNext() && curtup.getNextTupleInChain() != currentTupleID)
	{
		win_itr.next(curtup);
	}

	curtup.setNextTupleInChain(nextTupleId);

	markTupleForWindow(tuple);
	return PersistentTable::deleteTuple(tuple, deleteAllocatedStrings);
}

void TupleWindow::deleteTupleForUndo(voltdb::TableTuple &tupleCopy, size_t elMark)
{
	WindowIterator win_itr(this);
	TableTuple curtup = this->getTempTuple();
	uint32_t currentTupleID = tuple.getTupleID();
	uint32_t nextTupleID = tuple.getNextTupleInChain();

	if(!win_itr.hasNext())
		return;

	win_itr.next(curtup);
	while(win_itr.hasNext() && curtup.getNextTupleInChain() != currentTupleID)
	{
		win_itr.next(curtup);
	}

	curtup.setNextTupleInChain(nextTupleId);

	markTupleForWindow(tuple);

	PersistentTable::deleteTupleForUndo(tupleCopy, elMark);
}

void TupleWindow::setFireTriggers(bool fire)
{
	m_fireTriggers = fire;
}

std::string TupleWindow::debug()
{
	std::ostringstream output;
	WindowIterator win_itr(this);
	TableTuple tuple(m_schema);
	int stageID = 0;
	int winID = 0;

	output << "DEBUG TABLE SIZE: " << int(m_tupleCount) << " tuples, " << int(m_numStagedTuples) << " staged\n";
	while(win_itr.hasNext())
	{
		win_itr.next(tuple);
		if(stagedTuple(tuple))
		{
			output << "STAGED " << stageID << ": ";
			stageID++;
		}
		else
		{
			output << "WINDOW " << winID << ": ";
			winID++;
		}
		output << tuple.debug("").c_str() << "\n";

	}
	return output.str();
}

}

