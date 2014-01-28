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
#include "streaming/WindowTableTemp.h"
#include "streaming/WindowIterator.h"

#include <map>

namespace voltdb {
/**
 * This value has to match the value in CopyOnWriteContext.cpp
 */
#define TABLE_BLOCKSIZE 2097152
#define MAX_EVICTED_TUPLE_SIZE 2500

WindowTableTemp::WindowTableTemp(ExecutorContext *ctx, bool exportEnabled, int windowSize, int slideSize) : PersistentTable(ctx, exportEnabled)
{
	this->m_windowSize = windowSize;
	this->m_slideSize = slideSize;
	this->m_numStagedTuples = 0;
	this->m_newestTupleID = 0;
	this->m_newestWindowTupleID = 0;
	this->m_oldestTupleID = 0;
	this->m_firstTuple = true;
}

WindowTableTemp::~WindowTableTemp()
{
}

void WindowTableTemp::markTupleForStaging(TableTuple &source)
{
	//TODO: window tuples should probably have their own stagingFlag. Using deletedFlag as a hack.
	//setting deleted = true, but not actually freeing the memory
	if(!(tupleStaged(source)))
	{
		source.setDeletedTrue();
		m_tupleCount--;
		m_numStagedTuples++;
		//VOLT_DEBUG("markTupleForStaging: marked (tuples: %d, staged: %d)", m_tupleCount, m_numStagedTuples);
	}
	//else
		//VOLT_DEBUG("markTupleForStaging: NOT marked (tuples: %d, staged: %d)", m_tupleCount, m_numStagedTuples);
}

void WindowTableTemp::markTupleForWindow(TableTuple &source)
{
	//TODO: window tuples should probably have their own stagingFlag. Using deletedFlag as a hack.
	//setting deleted = true, but not actually freeing the memory
	if(tupleStaged(source))
	{
		source.setDeletedFalse();
		m_tupleCount++;
		m_numStagedTuples--;
		//VOLT_DEBUG("markTupleForWindow: marked (tuples: %d, staged: %d)", m_tupleCount, m_numStagedTuples);
	}
	//else
		//VOLT_DEBUG("markTupleForWindow: NOT marked (tuples: %d, staged: %d)", m_tupleCount, m_numStagedTuples);
}

bool WindowTableTemp::removeOldestTuple()
{
	//if there are no tuples, then we can't remove any tuples
	if(m_tupleCount == 0 && m_numStagedTuples == 0)
	{
		VOLT_DEBUG("WindowTableTemp: no tuples to delete");
		return false;
	}

	TableTuple tuple = this->tempTuple();
	tuple.move(this->dataPtrForTuple(m_oldestTupleID));
	VOLT_DEBUG("DELETING OLDEST TUPLE: %d", m_oldestTupleID);

	if(m_oldestTupleID == m_newestTupleID)
	{
		m_oldestTupleID = 0;
		m_newestTupleID = 0;
		m_newestWindowTupleID = 0;
		m_firstTuple = true;
	}
	else
		m_oldestTupleID = tuple.getNextTupleInChain();
	VOLT_DEBUG("OLDEST TUPLE: %d  NEWEST TUPLE: %d", m_oldestTupleID, m_newestTupleID);

	//TODO: This is a hack due to the fact that we're using the deletedFlag.  We're setting this tuple
	//to a non-deleted state so that the delete command will work.  This is horrible and needs to be fixed.
	markTupleForWindow(tuple);

	return PersistentTable::deleteTuple(tuple, true);
}

void WindowTableTemp::setNewestTupleID(uint32_t id)
{
	m_newestTupleID = id;
}

void WindowTableTemp::setNewestWindowTupleID(uint32_t id)
{
	m_newestWindowTupleID = id;
}

void WindowTableTemp::setOldestTupleID(uint32_t id)
{
	m_oldestTupleID = id;
}

uint32_t WindowTableTemp::getNewestTupleID()
{
	return m_newestTupleID;
}

uint32_t WindowTableTemp::getNewestWindowTupleID()
{
	return m_newestWindowTupleID;
}

uint32_t WindowTableTemp::getOldestTupleID()
{
	return m_oldestTupleID;
}


void WindowTableTemp::setFireTriggers(bool fire)
{
	m_fireTriggers = fire;
}

bool WindowTableTemp::tupleStaged(TableTuple &source)
{
	//TODO: this is using the deleted flag.  Should have a staging flag instead.
	return !(source.isActive());
}

bool WindowTableTemp::tuplesInStaging()
{
	if(m_numStagedTuples == 0)
		return false;
	return true;
}

void WindowTableTemp::setNumStagedTuples(int numTuples)
{
	m_numStagedTuples = numTuples;
}

int WindowTableTemp::getNumStagedTuples()
{
	return m_numStagedTuples;
}

std::string WindowTableTemp::printChain()
{
	std::ostringstream output;
	WindowIterator win_itr(this);
	TableTuple tuple(m_schema);
	while(win_itr.hasNext())
	{
		win_itr.next(tuple);
		output << tuple.getTupleID();
		if(tupleStaged(tuple))
			output << "(S)";
		output << ",";
	}
	output << "\n";
	return output.str();
}

}

