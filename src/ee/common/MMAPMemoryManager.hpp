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
#ifndef _MMAPMEMORYMANAGER_HPP_
#define _MMAPMEMORYMANAGER_HPP_

#include "common/debuglog.h"
#include "common/FatalException.hpp"

#include <iostream>
#include <set>
#include <string>
#include <pthread.h>
#include <map>
#include <utility>
#include <algorithm>

using namespace std;

namespace voltdb {
  
    class MMAPMemoryManager {
    public:
	MMAPMemoryManager();
        MMAPMemoryManager(size_t size, const std::string fileName, bool persistent);

        ~MMAPMemoryManager();

        // no copy, no assignment
	MMAPMemoryManager(MMAPMemoryManager const&);
	MMAPMemoryManager operator=(MMAPMemoryManager const&);

        void* alloc(size_t chunkSize) ;
        void showMetadata();

        // Sync in-memory changes with file synchronously
        bool sync();
        
        // Sync in-memory changes with file asynchronously
        bool async();

    private:
        void init();
        
        // Base Ptr to allocated memory
        void *m_base;

        // Bookkeeping
        size_t m_size;
        size_t m_allocated;

	// For persistent Map
        std::string m_fileName;
        bool m_persistent;
        
        // METADATA :: Index -> (Offset, Size)
        map<int, pair<int,int> > m_metadata;
        int m_index;

        static pthread_mutex_t m_mutex;

    };

}

#endif /* _MMAPMEMORYMANAGER_HPP_) */
