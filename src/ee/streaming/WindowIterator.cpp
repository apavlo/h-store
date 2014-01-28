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


#include "streaming/WindowIterator.h"
#include "streaming/WindowTableTemp.h"

namespace voltdb {


WindowIterator::WindowIterator(WindowTableTemp *t)
{
    wtable = t;
    current_tuple_id = -1;
    current_tuple = new TableTuple(wtable->schema());
    is_first = true;
}

WindowIterator::~WindowIterator()
{
}

bool WindowIterator::hasNext()
{
    //WindowTableTemp* wtable = static_cast<WindowTableTemp*>(table);

    if(current_tuple_id == wtable->getNewestTupleID())
        return false;
    if(wtable->usedTupleCount() == 0)
        return false;

    return true;
}

bool WindowIterator::next(TableTuple &tuple)
{
    //WindowTableTemp* wtable = static_cast<WindowTableTemp*>(table);

    if(current_tuple_id == wtable->getNewestTupleID()) // we've already returned the last tuple in the chain
    {
        //VOLT_DEBUG("No more tuples in the chain.");
        return false;
    }

    if(is_first) // this is the first call to next
    {
        is_first = false;
        //VOLT_DEBUG("This is the first tuple in the chain.");

        current_tuple_id = wtable->getOldestTupleID();
    }
    else  // advance the iterator to the next tuple in the chain
    {
        current_tuple_id = current_tuple->getNextTupleInChain();
    }

    current_tuple->move(wtable->dataPtrForTuple(current_tuple_id));
    tuple.move(current_tuple->address());
    VOLT_DEBUG("CURRENT_TUPLE_ID: %d, CURRENT_TUPLE: %d, TUPLE: %d", current_tuple_id, current_tuple->getTupleID(), tuple.getTupleID());

    //VOLT_DEBUG("current_tuple_id = %d", current_tuple_id);

    return true;
}

}
