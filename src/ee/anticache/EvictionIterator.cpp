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

// that's the factor indicate how many times the number of the tuple we'll sample comparing to we request
#define RANDOM_SCALE 4

namespace voltdb {

#ifdef ANTICACHE_TIMESTAMPS_PRIME
/**
 * if we use a prime-offset sampling strategy, here are the prime numbers we want to choose from:
 * Notice that those numbers are sufficient, because their product is bigger than block size.
 */
const int EvictionIterator::prime_list[prime_size] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97};
#endif

EvictionIterator::EvictionIterator(Table *t)
{
    //ptable = static_cast<PersistentTable*>(table); 
    table = t; 
    current_tuple_id = 0;
    current_tuple = new TableTuple(table->schema());
    is_first = true; 
}

#ifdef ANTICACHE_TIMESTAMPS
/**
 * Reserve some tuples when an eviction requested.
 */
void EvictionIterator::reserve(int64_t amount) {
    VOLT_DEBUG("amount: %ld\n", amount);

    char* addr = NULL;
    PersistentTable* ptable = static_cast<PersistentTable*>(table);
    int tuple_size = ptable->m_schema->tupleLength() + TUPLE_HEADER_SIZE;
    int active_tuple = (int)ptable->activeTupleCount();
    int evict_num = 0;
    int64_t used_tuple = ptable->usedTupleCount();
#ifdef ANTICACHE_TIMESTAMPS_PRIME
    uint32_t tuples_per_block = ptable->m_tuplesPerBlock;
#endif

    if (active_tuple)   
        evict_num = (int)(amount / (tuple_size + ptable->nonInlinedMemorySize() / active_tuple));
    else 
        evict_num = (int)(amount / tuple_size);

    VOLT_DEBUG("Count: %lu %lu\n", ptable->usedTupleCount(), ptable->activeTupleCount());

    if (evict_num > active_tuple)
        evict_num = active_tuple;

    int pick_num = evict_num * RANDOM_SCALE;

    int block_num = (int)ptable->m_data.size();
    int block_size = ptable->m_tuplesPerBlock;
    int location_size;
#ifndef ANTICACHE_TIMESTAMPS_PRIME
    int block_location;
#endif

    srand((unsigned int)time(0));

    VOLT_INFO("evict pick num: %d %d\n", evict_num, pick_num);
    VOLT_INFO("active_tuple: %d\n", active_tuple);
    VOLT_INFO("block number: %d\n", block_num);

    m_size = 0;
    current_tuple_id = 0;

#ifdef ANTICACHE_TIMESTAMPS_PRIME
    int pick_num_block = (int)(((int64_t)pick_num * tuples_per_block) / used_tuple);
    int last_full_block = (int)(used_tuple / block_size);
    VOLT_INFO("LOG: %d %d %ld\n", last_full_block, tuples_per_block, used_tuple);
    int last_block_size = (int)(used_tuple % block_size);
    int pick_num_last_block = pick_num - pick_num_block * last_full_block;
#endif

    // If we'll evict the entire table, we should do a scan instead of sampling.
    // The main reason we should do that is to past the test...
    if (evict_num < active_tuple) {
        candidates = new EvictionTuple[pick_num];
#ifdef ANTICACHE_TIMESTAMPS_PRIME
        for (int i = 0; i < last_full_block; ++i) {
            
            /**
             * if this is a beginning of a loop of scan, find a proper step to let it sample tuples from almost the whole block
             * TODO: Here we use a method that every time try a different prime number from what we use last time. Is it better?
             *       That would need further analysis.
             */  
            if (ptable->m_stepPrime[i] < 0) {
                int ideal_step = (rand() % 5) * tuples_per_block / pick_num_block;
                int old_prime = - ptable->m_stepPrime[i];
                for (int j = prime_size - 1; j >= 0; --j) {
                    if (prime_list[j] != old_prime && (tuples_per_block % prime_list[j]) > 0) {
                        ptable->m_stepPrime[i] = prime_list[j];
                        VOLT_TRACE("DEBUG: %d %d\n", tuples_per_block, ptable->m_stepPrime[i]);
                    }
                    if (prime_list[j] <= ideal_step)
                        break;
                }
                VOLT_INFO("Prime of block %d: %d %d\n", i, tuples_per_block, ptable->m_stepPrime[i]);
            }

            // now scan the block with a step of we select.
            // if we go across the boundry, minus it back to the beginning (like a mod operation)
            int step_prime = ptable->m_stepPrime[i];
            int step_offset = step_prime * tuple_size;
            int block_size_bytes = block_size * tuple_size;
            addr = ptable->m_data[i] + ptable->m_evictPosition[i];
            uint64_t end_of_block = (uint64_t)ptable->m_data[i] + block_size_bytes;
            bool flag_new = false;
            for (int j = 0; j < pick_num_block; ++j) {
                VOLT_TRACE("Flip addr: %p %p %lu\n", addr, ptable->m_data[i], ((uint64_t)addr - (uint64_t)ptable->m_data[i]) / 1024);

                current_tuple->move(addr);

                if (current_tuple->isActive()) {
                    candidates[m_size].setTuple(current_tuple->getTimeStamp(), addr);
                    m_size++;
                }

                addr += step_offset;
                if ((uint64_t)addr >= end_of_block)
                    addr -= block_size_bytes;
                if (addr == ptable->m_data[i])
                    flag_new = true;
            }
            int new_position = (int)((uint64_t)addr - (uint64_t)ptable->m_data[i]);
            ptable->m_evictPosition[i] = new_position;
            if (flag_new)
                ptable->m_stepPrime[i] = - ptable->m_stepPrime[i];
        }
        if (last_full_block < block_num) {
            addr = ptable->m_data[last_full_block];
            char* current_addr;
            for (int j = 0; j < pick_num_last_block; ++j) {
                current_addr = addr + (rand() % last_block_size) * tuple_size;
                current_tuple->move(current_addr);
                if (!current_tuple->isActive() || current_tuple->isEvicted())
                    continue;

                candidates[m_size].setTuple(current_tuple->getTimeStamp(), current_addr);
                m_size++;
            }
        }

#else
        for (int i = 0; i < pick_num; i++) {
            // should we use a faster random generator?
            block_location = rand() % block_num;
            addr = ptable->m_data[block_location];
            if ((block_location + 1) * block_size > used_tuple)
                location_size = (int)(used_tuple - block_location * block_size);
            else
                location_size = block_size;
            addr += (rand() % location_size) * tuple_size;

            current_tuple->move(addr);

            VOLT_DEBUG("Flip addr: %p\n", addr);

            if (!current_tuple->isActive() || current_tuple->isEvicted())
                continue;

            candidates[m_size].setTuple(current_tuple->getTimeStamp(), addr);
            m_size++;
        }
#endif
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

                if (!current_tuple->isActive() || current_tuple->isEvicted()) {
                    addr += tuple_size;
                    continue;
                }

                VOLT_TRACE("Flip addr: %p\n", addr);

                candidates[m_size].setTuple(current_tuple->getTimeStamp(), addr);
                m_size++;

                addr += tuple_size;
            }
        }
    }
    sort(candidates, candidates + m_size, less <EvictionTuple>());

    //VOLT_INFO("Size of eviction candidates: %lu %d %d\n", (long unsigned int)m_size, activeN, evictedN);
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
    VOLT_TRACE("Size: %lu\n", (long unsigned int)m_size);
    PersistentTable* ptable = static_cast<PersistentTable*>(table);

    VOLT_TRACE("Count: %lu %lu\n", ptable->usedTupleCount(), ptable->activeTupleCount());

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
