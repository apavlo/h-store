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

#ifndef HSTOREANTICACHE_H
#define HSTOREANTICACHE_H

#include <db_cxx.h>

#include "common/debuglog.h"
#include "common/DefaultTupleSerializer.h"

#include <map>
#include <vector>


//#define ANTICACHE_DB_NAME "anticache.db"

using namespace std;

namespace voltdb {
    
class ExecutorContext;
class AntiCacheDB;

/**
 * Wrapper class for an evicted block that has been read back in 
 * from the AntiCacheDB
 */
class AntiCacheBlock {
    friend class AntiCacheDB;
    
    public:
        virtual ~AntiCacheBlock() {};
        
        inline int16_t getBlockId() const {
        	return m_blockId;
        }

        inline std::string getTableName() const {
            return (m_payload.tableName);
        }

        inline long getSize() const {
        	return m_size;
        }
        inline char* getData() const {
	    return m_block;
        }

        struct payload{
        	int16_t blockId;
        	std::string tableName;
            char * data;
            long size;
        };

        inline int getBlockType() const {
            return m_blockType;
        }
    
    protected:
        AntiCacheBlock(int16_t blockId);
        //AntiCacheBlock(int16_t blockId, char* block, long size);
        int16_t m_blockId;
        payload m_payload;
	    long m_size;
	    char * m_block;
	    char * m_buf;
        
        int m_blockType;
}; // CLASS

class AntiCacheDB {
        
    public: 
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize);
        virtual ~AntiCacheDB();

		//void initializeNVM(); 
		
		//void initializeBerkeleyDB(); 

        /**
         * Write a block of serialized tuples out to the anti-cache database
         */
        virtual void writeBlock(const std::string tableName,
                                int16_t blockId,
                                const int tupleCount,
                                const char* data,
                                const long size) = 0;
        /**
         * Read a block and return its contents
         */
        virtual AntiCacheBlock* readBlock(std::string tableName, int16_t blockId) = 0;


        /**
         * Flush the buffered blocks to disk.
         */
        virtual void flushBlocks() = 0;

        /**
         * Return the next BlockId to use in the anti-cache database
         * This is guaranteed to be unique per partition
         */
        inline int16_t nextBlockId() {
            return (++m_nextBlockId);
        }
        
    protected:
        
        /**
         * NVM constants
         */
        /*
        static const off_t NVM_FILE_SIZE = 1073741824/2; 
        static const int NVM_BLOCK_SIZE = 524288 + 1000; 
	    static const int MMAP_PAGE_SIZE = 2 * 1024 * 1024; 
        */
        ExecutorContext *m_executorContext;
        string m_dbDir;
        long m_blockSize;
        int16_t m_nextBlockId;
	    int m_partitionId; 

        /*
        FILE* nvm_file;
        char* m_NVMBlocks; 
        int nvm_fd; 
        */
        /**
         *  Maps a block id to a <index, size> pair
         */
		std::map<int16_t, pair<int, int32_t> > m_blockMap; 
		
		/**
		 *  List of free block indexes before the end of the last allocated block.
		 */
        //std::vector<int> m_NVMBlockFreeList; 
		
	    int m_totalBlocks; 
        int m_nextFreeBlock; 
		
		//void shutdownNVM(); 
		
		//void shutdownBerkeleyDB();
		
        virtual void shutdownDB() = 0;


				
        /**
         *   Returns a pointer to the start of the block at the specified index. 
         */
        //char* getNVMBlock(int index); 
        
        /**
         *    Adds the index to the free block list. 
         */
        //void freeNVMBlock(int index);
        
        /**
         *   Returns the index of a free slot in the NVM block array. 
         */
        //int getFreeNVMBlockIndex(); 
        
}; // CLASS

}
#endif
