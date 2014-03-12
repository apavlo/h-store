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
#include <map>
#include <vector>

#define ANTICACHE_DB_NAME "anticache.db"

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
        ~AntiCacheBlock();
        
        inline int16_t getBlockId() const {
            return (m_blockId);
        }
        inline long getSize() const {
	  //return (m_value.get_size());
	  return m_size; 
        }
        inline char* getData() const {

	    return m_block; 
        }
    
    private:
        AntiCacheBlock(int16_t blockId, char* block, long size);
        
        int16_t m_blockId;
	char* m_block;
	long m_size; 
}; // CLASS

/**
 *
 */
class AntiCacheDB {
        
    public: 
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize);
        ~AntiCacheDB();

		void initializeNVM(); 
		
		void initializeBerkeleyDB(); 

        /**
         * Write a block of serialized tuples out to the anti-cache database
         */
        void writeBlock(const std::string tableName,
                        int16_t blockId,
                        const int tupleCount,
                        const char* data,
                        const long size);
        /**
         * Read a block and return its contents
         */
        AntiCacheBlock readBlock(std::string tableName, int16_t blockId);

        /**
         * Flush the buffered blocks to disk.
         */
        void flushBlocks();

        /**
         * Return the next BlockId to use in the anti-cache database
         * This is guaranteed to be unique per partition
         */
        inline int16_t nextBlockId() {
            return (++m_nextBlockId);
        }
        
    private:
        
        /**
         * NVM constants
         */
        static const off_t NVM_FILE_SIZE = 1073741824/2; 
        static const int NVM_BLOCK_SIZE = 524288 + 1000; 
	static const int MMAP_PAGE_SIZE = 2 * 1024 * 1024; 
        
        ExecutorContext *m_executorContext;
        string m_dbDir;
        long m_blockSize;
        DbEnv* m_dbEnv;
        Db* m_db; 
        int16_t m_nextBlockId;
	int m_partitionId; 

        FILE* nvm_file;
        char* m_NVMBlocks; 
        int nvm_fd; 

        /**
         *  Maps a block id to a <index, size> pair
         */
		std::map<int16_t, pair<int, int32_t> > m_blockMap; 
		
		/**
		 *  List of free block indexes before the end of the last allocated block.
		 */
        std::vector<int> m_NVMBlockFreeList; 
		
	int m_totalBlocks; 
        int m_nextFreeBlock; 
		
		void shutdownNVM(); 
		
		void shutdownBerkeleyDB();
		
		void writeBlockNVM(const std::string tableName, 
				   int16_t blockID, 
				   const int tupleCount, 
				   const char* data, 
				   const long size); 
				
		void writeBlockBerkeleyDB(	const std::string tableName, 
					   int16_t blockID, 
					   const int tupleCount, 
					   const char* data, 
					   const long size);
		
        AntiCacheBlock readBlockNVM(std::string tableName, int16_t blockId); 

        AntiCacheBlock readBlockBerkeleyDB(std::string tableName, int16_t blockId);
        
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
        
}; // CLASS

}
#endif
