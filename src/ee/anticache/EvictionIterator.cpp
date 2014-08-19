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
    int active_tuple = (int)ptable->activeTupleCount(); // should be more careful here. what's the typical size? Answer: 256K
    int evict_num = 0;

    if (active_tuple)	
        evict_num = (int)(amount / (tuple_size + ptable->nonInlinedMemorySize() / active_tuple));
    else 
        evict_num = (int)(amount / tuple_size);
    //printf("Count: %lu %lu\n", ptable->usedTupleCount(), ptable->activeTupleCount());

    if (evict_num > active_tuple)
        evict_num = active_tuple;

    int pick_num = evict_num * RANDOM_SCALE;
    // TODO: If pick_num is too big or evict_num is too small, should we use scanning instead?

    int block_num = (int)ptable->m_data.size();
    int block_size = ptable->m_tuplesPerBlock;
    int location_size;
    int block_location;

    srand((unsigned int)time(0));

    //VOLT_ERROR("evict pick num: %d %d\n", evict_num, pick_num);
    //VOLT_ERROR("active_tuple: %d\n", active_tuple);
    //VOLT_ERROR("block number: %d\n", block_num);

    int activeN = 0, evictedN = 0;
    m_size = 0;
    current_tuple_id = 0;

    if (evict_num < active_tuple) {
        candidates = new EvictionTuple[pick_num];
        for (int i = 0; i < pick_num; i++) {
            // should we use a faster random generator?
            block_location = rand() % block_num;
            addr = ptable->m_data[block_location];
            if ((block_location + 1) * block_size > ptable->usedTupleCount())
                location_size = (int)(ptable->usedTupleCount() - block_location * block_size);
            else
                location_size = block_size;
            addr += (rand() % location_size) * tuple_size;

            current_tuple->move(addr);

            VOLT_DEBUG("Flip addr: %p\n", addr);

            if (current_tuple->isActive()) activeN++;
            if (current_tuple->isEvicted()) evictedN++;

            if (!current_tuple->isActive() || current_tuple->isEvicted())
                continue;

            //printf("here!!\n");

            candidates[m_size].setTuple(current_tuple->getTimeStamp(), addr);
            m_size++;
        }
    } else {
        candidates = new EvictionTuple[active_tuple];
        for (int i = 0; i < block_num; ++i) { 
            addr = ptable->m_data[i];
            if ((i + 1) * block_size > ptable->usedTupleCount())
                location_size = (int)(ptable->usedTupleCount() - i * block_size);
            else
                location_size = block_size;
            for (int j = 0; j < location_size; j++) {
                current_tuple->move(addr);

                if (current_tuple->isActive()) activeN++;
                if (current_tuple->isEvicted()) evictedN++;

                if (!current_tuple->isActive() || current_tuple->isEvicted()) {
                    addr += tuple_size;
                    continue;
                }

                VOLT_DEBUG("Flip addr: %p\n", addr);

                candidates[m_size].setTuple(current_tuple->getTimeStamp(), addr);
                m_size++;
               
                addr += tuple_size;
            }
        }
    }
    sort(candidates, candidates + m_size, less <EvictionTuple>());
    VOLT_INFO("Size of eviction candidates: %lu %d %d\n", m_size, activeN, evictedN);
}
#endif


EvictionIterator::~EvictionIterator()
{
#ifdef ANTICACHE_TIMESTAMPS
    delete[] candidates;
#endif
    delete current_tuple;
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
    if (current_tuple_id == m_size)
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
    tuple.move(candidates[current_tuple_id].m_addr);
    current_tuple_id++;
    while (candidates[current_tuple_id].m_addr == candidates[current_tuple_id - 1].m_addr) {
        current_tuple_id++;
        if (current_tuple_id == m_size) break;
    }
#endif

    return true; 
}

}
