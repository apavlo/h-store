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

#include "streaming/TimeWindow.h"
#include "streaming/WindowTableTemp.h"
#include "streaming/WindowIterator.h"

namespace voltdb {
/**
 * This value has to match the value in CopyOnWriteContext.cpp
 */
#define TABLE_BLOCKSIZE 2097152
#define MAX_EVICTED_TUPLE_SIZE 2500
#define TIMESTAMP_COLUMN "TS"

TimeWindow::TimeWindow(ExecutorContext *ctx, bool exportEnabled, int windowSize, int slideSize) : WindowTableTemp(ctx, exportEnabled, windowSize, slideSize)
{
	m_tsColumn = findTSColumn();
}

TimeWindow::~TimeWindow()
{
}

int TimeWindow::findTSColumn()
{
	return columnIndex("TS");
}

int TimeWindow::getTSColumn()
{
	return m_tsColumn;
}

/**
bool TimeWindow::insertTuple(TableTuple &source)
{
	VOLT_DEBUG("TimeWindow: Entering insertTuple");
	if(!(PersistentTable::insertTuple(source)))
	{
		VOLT_DEBUG("TimeWindow: PersistentTable insertTuple failed!!!!!!!!!");
		return false;
	}

	m_tmpTarget1.setTupleID(this->getTupleID(m_tmpTarget1.address()));
	if(!m_firstTuple)
	{
		TableTuple newestTuple = this->tempTuple();
		newestTuple.move(this->dataPtrForTuple(m_newestTupleID));
		newestTuple.setNextTupleInChain(m_tmpTarget1.getTupleID());
	}

	VOLT_DEBUG("tupleID: %d", m_tmpTarget1.getTupleID());

	if(m_firstTuple)
		this->m_oldestTupleID = m_tmpTarget1.getTupleID();
	this->m_newestTupleID = m_tmpTarget1.getTupleID();

	markTupleForStaging(m_tmpTarget1);

	if(m_firstTuple)
		m_firstTuple = false;

	if(m_numStagedTuples >= m_slideSize)
	{
		//delete all tuples from the chain until there are exactly the window size of tuples
		while((m_numStagedTuples + m_tupleCount) > m_windowSize)
		{
			if(!(this->removeOldestTuple()))
			{
				VOLT_DEBUG("TimeWindow: removeOldestTuple failed!!!!!!!!!!!!!!");
				return false;
			}
		}

		TableTuple tuple(m_schema);
		WindowIterator win_itr(this);
		while(win_itr.hasNext())
		{
			win_itr.next(tuple);
			markTupleForWindow(tuple);
		}
		setNewestWindowTupleID(tuple.getTupleID());
		if(hasTriggers())
			setFireTriggers(true);
	}
	VOLT_DEBUG("stagedTuples: %d, tupleCount: %d", m_numStagedTuples, m_tupleCount);
	return true;
}


void TimeWindow::insertTupleForUndo(TableTuple &source, size_t elMark)
{
	PersistentTable::insertTupleForUndo(source, elMark);
	markTupleForStaging(m_tmpTarget1);

	if(m_numStagedTuples >= m_slideSize)
	{
		//delete all tuples from the chain until there are exactly the window size of tuples
		while((m_numStagedTuples + m_tupleCount) > m_windowSize)
		{
			this->removeOldestTuple();
		}

		TableTuple tuple(m_schema);
		WindowIterator win_itr(this);
		while(win_itr.hasNext())
		{
			win_itr.next(tuple);
			markTupleForWindow(tuple);
		}
		setNewestWindowTupleID(tuple.getTupleID());
		if(hasTriggers())
			setFireTriggers(true);
	}
}

//TODO: the tuple pointers aren't quite working right.  Fortunately we rarely delete from windows.
bool TimeWindow::deleteTuple(TableTuple &tuple, bool deleteAllocatedStrings)
{
	VOLT_DEBUG("TimeWindow DELETETUPLE");
	WindowIterator win_itr(this);
	TableTuple curtup = tempTuple();
	uint32_t currentTupleID = tuple.getTupleID();
	uint32_t nextTupleID = tuple.getNextTupleInChain();

	//if there are no tuples to delete
	if(!win_itr.hasNext())
		return false;
	//if there's only one tuple left
	else if(getOldestTupleID() == getNewestTupleID())
	{
		setOldestTupleID(0);
		setNewestTupleID(0);
		setNewestWindowTupleID(0);
		m_firstTuple = true;
	}
	//if the tuple to delete is the first one
	else if(currentTupleID == m_oldestTupleID)
	{
		setOldestTupleID(tuple.getNextTupleInChain());
	}
	//otherwise reorganize the chain
	else
	{
		win_itr.next(curtup);
		while(win_itr.hasNext() && curtup.getNextTupleInChain() != currentTupleID)
		{
			win_itr.next(curtup);
		}

		curtup.setNextTupleInChain(nextTupleID);
	}

	//TODO: must mark tuple for window before it can be deleted
	markTupleForWindow(tuple);
	bool deletedTuple = PersistentTable::deleteTuple(tuple, deleteAllocatedStrings);

	//if(deletedTuple)
	//	resetWindow();

	return deletedTuple;
}

void TimeWindow::deleteTupleForUndo(voltdb::TableTuple &tupleCopy, size_t elMark)
{
	VOLT_DEBUG("TimeWindow DELETETUPLE");
	WindowIterator win_itr(this);
	TableTuple curtup = tempTuple();
	uint32_t currentTupleID = tupleCopy.getTupleID();
	uint32_t nextTupleID = tupleCopy.getNextTupleInChain();

	//if there are no tuples to delete
	if(!win_itr.hasNext())
		return;
	//if there's only one tuple left
	else if(getOldestTupleID() == getNewestTupleID())
	{
		setOldestTupleID(0);
		setNewestTupleID(0);
		setNewestWindowTupleID(0);
		m_firstTuple = true;
	}
	//if the tuple to delete is the first one
	else if(currentTupleID == m_oldestTupleID)
	{
		setOldestTupleID(tupleCopy.getNextTupleInChain());
	}
	//otherwise reorganize the chain
	else
	{
		win_itr.next(curtup);
		while(win_itr.hasNext() && curtup.getNextTupleInChain() != currentTupleID)
		{
			win_itr.next(curtup);
		}

		curtup.setNextTupleInChain(nextTupleID);
	}

	//TODO: must mark tuple for window before it can be deleted
	markTupleForWindow(tupleCopy);

	//if(deletedTuple)
	//	resetWindow();

	PersistentTable::deleteTupleForUndo(tupleCopy, elMark);
}

std::string TimeWindow::debug()
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
		if(tupleStaged(tuple))
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
*/
}

