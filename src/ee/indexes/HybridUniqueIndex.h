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

#ifndef BINARYTREEUNIQUEINDEX_H_
#define BINARYTREEUNIQUEINDEX_H_

//#include <map>
#include "stx-compact/btree_map.h"
#include "stx-compact/btree.h"
#include <iostream>
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"

namespace voltdb {

/**
 * Index implemented as a Binary Unique Map.
 * @see TableIndex
 */
template<typename KeyType, class KeyComparator, class KeyEqualityChecker>
class BinaryTreeUniqueIndex : public TableIndex
{
    friend class TableIndexFactory;

    //typedef std::map<KeyType, const void*, KeyComparator> MapType;
    typedef h_index::AllocatorTracker<pair<const KeyType, const void*> > AllocatorType;
    typedef stx_hybrid_compact::btree_map<KeyType, const void*, KeyComparator, stx_hybrid_compact::btree_default_map_traits<KeyType, const void*>, AllocatorType> MapType;

public:

    ~BinaryTreeUniqueIndex() {
        delete m_entries;
        delete m_allocator;
    };

    bool addEntry(const TableTuple* tuple)
    {
      //std::cout << "addEntry\n";
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return addEntryPrivate(tuple, m_tmp1);
    }

    bool deleteEntry(const TableTuple* tuple)
    {
      //std::cout << "deleteEntry\n";
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        return deleteEntryPrivate(m_tmp1);
    }

    bool replaceEntry(const TableTuple* oldTupleValue,
                      const TableTuple* newTupleValue)
    {
      //std::cout << "replaceEntry\n";
        m_tmp1.setFromTuple(oldTupleValue, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(newTupleValue, column_indices_, m_keySchema);

        if (m_eq(m_tmp1, m_tmp2))
            return true;

        bool deleted = deleteEntryPrivate(m_tmp1);
        bool inserted = addEntryPrivate(newTupleValue, m_tmp2);
        --m_deletes;
        --m_inserts;
        ++m_updates;
        return (deleted && inserted);
    }
    
    bool setEntryToNewAddress(const TableTuple *tuple, const void* address, const void *oldAddress) {
      //std::cout << "setEntry\n";
        m_tmp1.setFromTuple(tuple, column_indices_, m_keySchema);
        ++m_updates; 
        
        m_entries->erase_one_hybrid(m_tmp1); 
        std::pair<typename MapType::iterator, bool> retval = m_entries->insert(std::pair<KeyType, const void*>(m_tmp1, address));
        return retval.second;
    }

    bool checkForIndexChange(const TableTuple* lhs, const TableTuple* rhs)
    {
      //std::cout << "check\n";
        m_tmp1.setFromTuple(lhs, column_indices_, m_keySchema);
        m_tmp2.setFromTuple(rhs, column_indices_, m_keySchema);
        return !(m_eq(m_tmp1, m_tmp2));
    }

    bool exists(const TableTuple* values)
    {
      //std::cout << "exists\n";
        ++m_lookups;
        m_tmp1.setFromTuple(values, column_indices_, m_keySchema);
        return (!(m_entries->find_hybrid(m_tmp1).isEnd()));
    }

    bool moveToKey(const TableTuple* searchKey)
    {
      //std::cout << "moveToKey\n";
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries->find_hybrid(m_tmp1);
        if (m_keyIter.isEnd()) {
            m_match.move(NULL);
            return false;
        }
        m_match.move(const_cast<void*>(m_keyIter->second));
        return !m_match.isNullTuple();
    }

    bool moveToTuple(const TableTuple* searchTuple)
    {
      //std::cout << "moveToTuple\n";
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromTuple(searchTuple, column_indices_, m_keySchema);
        m_keyIter = m_entries->find_hybrid(m_tmp1);
        if (m_keyIter.isEnd()) {
            m_match.move(NULL);
            return false;
        }
        m_match.move(const_cast<void*>(m_keyIter->second));
        return !m_match.isNullTuple();
    }

    void moveToKeyOrGreater(const TableTuple* searchKey)
    {
      //std::cout << "moveToKeyOrGreater\n";
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries->lower_bound_hybrid(m_tmp1);
    }

    void moveToGreaterThanKey(const TableTuple* searchKey)
    {
      //std::cout << "moveToGreaterThanKey\n";
        ++m_lookups;
        m_begin = true;
        m_tmp1.setFromKey(searchKey);
        m_keyIter = m_entries->upper_bound_hybrid(m_tmp1);
    }

    void moveToEnd(bool begin)
    {
      //std::cout << "moveToEnd\n";
        ++m_lookups;
        //m_begin = begin;
        //if (begin)
	m_keyIter = m_entries->hybrid_begin();
	++m_keyIter;
        //else
	//m_keyRIter = m_entries->rbegin();
	  
    }

    TableTuple nextValue()
    {
      //std::cout << "nextValue\n";
        TableTuple retval(m_tupleSchema);

	if (m_keyIter.isEnd())
	  return TableTuple();
	retval.move(const_cast<void*>(m_keyIter->second));
	if (!m_keyIter.isComplete())
	  m_keyIter = m_entries->lower_bound_hybrid(m_keyIter->first);
	++m_keyIter;

        return retval;
    }

    TableTuple nextValueAtKey()
    {
      //std::cout << "nextValueAtKey\n";
        TableTuple retval = m_match;
        m_match.move(NULL);
        return retval;
    }

    bool advanceToNextKey()
    {
      //std::cout << "advanceToNextKey\n";
      if (!m_keyIter.isComplete())
	m_keyIter = m_entries->lower_bound_hybrid(m_keyIter->first);
      ++m_keyIter;
      if (m_keyIter.isEnd())
	{
	  m_match.move(NULL);
	  return false;
	}
      m_match.move(const_cast<void*>(m_keyIter->second));

      return !m_match.isNullTuple();
    }

    size_t getSize() const { return m_entries->size(); }
    int64_t getMemoryEstimate() const {
      return m_memoryEstimate + m_entries->get_bloom_filter_size();
    }
    
    std::string getTypeName() const { return "HybridUniqueIndex"; };

protected:
    BinaryTreeUniqueIndex(const TableIndexScheme &scheme) :
        TableIndex(scheme),
	m_begin(true),
	m_eq(m_keySchema)
    {
        m_match = TableTuple(m_tupleSchema);
        m_allocator = new AllocatorType(&m_memoryEstimate);
        m_entries = new MapType(KeyComparator(m_keySchema), (*m_allocator));
    }

    inline bool addEntryPrivate(const TableTuple* tuple, const KeyType &key)
    {
        ++m_inserts;
        std::pair<typename MapType::iterator, bool> retval = 
	  m_entries->insert(key, tuple->address());
        return retval.second;
    }

    inline bool deleteEntryPrivate(const KeyType &key)
    {
        ++m_deletes;
        return m_entries->erase_one_hybrid(key);
    }

    MapType *m_entries;
    AllocatorType *m_allocator;
    KeyType m_tmp1;
    KeyType m_tmp2;

    // iteration stuff
    bool m_begin;
    typename MapType::hybrid_iterator m_keyIter;
    TableTuple m_match;

    // comparison stuff
    KeyEqualityChecker m_eq;
};

}

#endif // HYBRIDUNIQUEINDEX_H_
