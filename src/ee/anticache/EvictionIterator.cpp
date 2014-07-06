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


#include "anticache/EvictionIterator.h"
#include "storage/persistenttable.h"

#define RANDOM_SCALE 4

namespace voltdb {
    
EvictionIterator::EvictionIterator(Table *t)
{
    //ptable = static_cast<PersistentTable*>(table); 
    table = t; 
    current_tuple_id = 0;
    current_tuple = new TableTuple(table->schema());
    is_first = true; 
}

#ifdef ANTICACHE_TIMESTAMPS
void EvictionIterator::reserve(int64_t amount) {
    //printf("amount: %ld\n", amount);
    char* addr = NULL;
    PersistentTable* ptable = static_cast<PersistentTable*>(table);
    int tuple_size = ptable->m_schema->tupleLength() + TUPLE_HEADER_SIZE;
    int evict_num = (int)(amount / tuple_size);
    //printf("Count: %lu %lu\n", ptable->usedTupleCount(), ptable->activeTupleCount());

    int used_tuple = (int)ptable->usedTupleCount(); // should be more careful here. what's the typical size? Answer: 256K
    if (evict_num > used_tuple)
        evict_num = used_tuple;

    int pick_num = evict_num * RANDOM_SCALE;
    // TODO: If pick_num is too big or evict_num is too small, should we use scanning instead?

    int block_num = (int)ptable->m_data.size();
    int block_size = ptable->m_tuplesPerBlock;

    // TODO: This should be valid in the final version
    // srand(time(0));
    TableTuple *tmpTuple = new TableTuple(ptable->m_schema);


    candidates = priority_queue <pair <uint32_t, char*> >();

    //printf("evict pick num: %d %d\n", evict_num, pick_num);

    for (int i = 0; i < pick_num; i++) {
        // should we use a faster random generator?
        addr = ptable->m_data[rand() % block_num];
        addr += rand() % block_size * tuple_size;

        tmpTuple->move(addr);

        //printf("addr: %p\n", addr);

        if (!tmpTuple->isActive() || tmpTuple->isEvicted())
            continue;

        //printf("here!!\n");

        candidates.push(make_pair(tmpTuple->getTimeStamp(), addr));
        if (candidates.size() > evict_num) candidates.pop();
    }
    //printf("Size: %lu\n", candidates.size());
}
#endif


EvictionIterator::~EvictionIterator()
{
#ifdef ANTICACHE_TIMESTAMPS
    while (!candidates.empty())
        candidates.pop();
#endif
}
    
bool EvictionIterator::hasNext()
{        
    //printf("Size: %lu\n", candidates.size());
    PersistentTable* ptable = static_cast<PersistentTable*>(table);

    //printf("Count: %lu %lu\n", ptable->usedTupleCount(), ptable->activeTupleCount());
    
    if(ptable->usedTupleCount() == 0)
        return false; 

#ifndef ANTICACHE_TIMESTAMPS
    if(current_tuple_id == ptable->getNewestTupleID())
        return false;
    if(ptable->getNumTuplesInEvictionChain() == 0) { // there are no tuples in the chain
        VOLT_DEBUG("There are no tuples in the eviction chain.");
        return false; 
    }
#else
    if (candidates.empty())
        return false;
#endif
    
    return true; 
}

bool EvictionIterator::next(TableTuple &tuple)
{    
#ifndef ANTICACHE_TIMESTAMPS
    PersistentTable* ptable = static_cast<PersistentTable*>(table);

    if(current_tuple_id == ptable->getNewestTupleID()) // we've already returned the last tuple in the chain
    {
        VOLT_DEBUG("No more tuples in the chain.");
        return false; 
    }

    if(is_first) // this is the first call to next
    {
        is_first = false; 
        VOLT_DEBUG("This is the first tuple in the chain.");

        if(ptable->getNumTuplesInEvictionChain() == 0)  // there are no tuples in the chain
        {
            VOLT_DEBUG("There are no tuples in the eviction chain.");
            return false; 
        }
        
        current_tuple_id = ptable->getOldestTupleID(); 
    }
    else  // advance the iterator to the next tuple in the chain
    {        
        current_tuple_id = current_tuple->getNextTupleInChain();
    }

    current_tuple->move(ptable->dataPtrForTuple(current_tuple_id)); 
    tuple.move(current_tuple->address()); 
    
    VOLT_DEBUG("current_tuple_id = %d", current_tuple_id);
#else
    tuple.move(candidates.top().second);
    candidates.pop();
#endif
    
    return true; 
}
    
}
