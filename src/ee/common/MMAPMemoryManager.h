#ifndef _MEMORYMANAGER_H_
#define _MEMORYMANAGER_H_

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

        ~MMAPMemoryManager();

        MMAPMemoryManager(size_t size, const std::string fileName, bool persistent);

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
        
        // For persistent Map
        bool m_persistent;
        std::string m_fileName;
        
        // Bookkeeping 
        size_t m_size;
        size_t m_allocated;

        // METADATA :: Index -> (Offset, Size)
        map<int, pair<int,int> > m_metadata;
        int m_index;

        static pthread_mutex_t m_mutex;
    };

}

#endif /* _MEMORYMANAGER_H_) */
