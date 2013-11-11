 /* Copyright (C) 2013 by H-Store Project
 * Brown University
 * Carnegie Mellon University
 * Massachusetts Institute of Technology
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
