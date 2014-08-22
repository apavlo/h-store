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


#ifndef EVICTIONITERATOR_H
#define EVICTIONITERATOR_H

#include "storage/TupleIterator.h"
#include "storage/table.h"
#include <set>

namespace voltdb {
 
class EvictionIterator : public TupleIterator
{
    
public: 
    
    EvictionIterator(Table* t); 
    ~EvictionIterator(); 
    
    bool hasNext(); 
    bool next(TableTuple &out);
#ifdef ANTICACHE_TIMESTAMPS
    void reserve(int64_t amount);
#endif

    class EvictionTuple {
    public:
        uint32_t m_timestamp;
        char *m_addr;

        bool operator < (const EvictionTuple &b) const {
            if (b.m_timestamp == m_timestamp)
                return (long)m_addr < (long)b.m_addr;
            return m_timestamp < b.m_timestamp;
        }

        void setTuple(uint32_t timestamp, char* addr) {
            m_timestamp = timestamp;
            m_addr = addr;
        }
    };
    
private: 
    
    Table *table;     
    uint32_t current_tuple_id;
    TableTuple* current_tuple;
    bool is_first; 
#ifdef ANTICACHE_TIMESTAMPS
    EvictionTuple *candidates;
    int32_t m_size;
#endif

#ifdef ANTICACHE_TIMESTAMPS_PRIME
    enum { prime_size = 25 };
    static const int prime_list[prime_size];
#endif

}; 

}

#endif
