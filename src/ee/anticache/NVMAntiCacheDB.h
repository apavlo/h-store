
/* Copyright (C) 2012 by H-Store Project
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

#ifndef NVMANTICACHE_H
#define NVMANTICACHE_H

#include <db_cxx.h>

#include "common/debuglog.h"
#include "common/DefaultTupleSerializer.h"
#include "anticache/AntiCacheDB.h"

#include <map>
#include <vector>

using namespace std;

namespace voltdb {

class ExecutorContext;
class AntiCacheDB;
class NVMAntiCacheDB;

class NVMAntiCacheBlock: public AntiCacheBlock {
    friend class NVMAntiCacheDB;
//    public:
//        ~NVMAntiCacheBlock();
    private:
        NVMAntiCacheBlock(int16_t blockId, char* block, long size);
};

class NVMAntiCacheDB: public AntiCacheDB {
    public:
        NVMAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize);
        ~NVMAntiCacheDB();
        void writeBlock(const std::string tableName,
                        int16_t blockID,
                        const int tupleCount,
                        const char* data,
                        const long size);
        
        void initialize();
        NVMAntiCacheBlock readBlock(std::string tableName, int16_t blockId);
        
        void flushBlocks();
    private:
        
        // gotta go 
        static const off_t NVM_FILE_SIZE = 1073741824/2; 
        static const int NVM_BLOCK_SIZE = 524288 + 1000; 
        static const int MMAP_PAGE_SIZE = 2 * 1024 * 1024; 

        FILE* nvm_file;     
        char* m_NVMBlocks;  
        int nvm_fd; 

        /**
         *  List of free block indexes before the end of the last allocated block.
         */
        std::vector<int> m_NVMBlockFreeList; 

        void shutdownDB();  
        

        AntiCacheBlock readBlockNVM(std::string tableName, int16_t blockId); 

        /**
         *   Returns a pointer to the start of the block at the specified index. 
         */
        char* getNVMBlock(int index); 
        
        /**
         *    Adds the index to the free block list. 
         */
        void freeNVMBlock(int index);
        
        /**
         *   Returns the index of a free slot in the NVM block array. 
         */
        int getFreeNVMBlockIndex(); 
}; //class

} //namespace
#endif
