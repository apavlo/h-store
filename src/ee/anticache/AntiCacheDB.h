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
#include "common/types.h"
#include "common/DefaultTupleSerializer.h"
#include "anticache/UnknownBlockAccessException.h"
#include "anticache/FullBackingStoreException.h"

#include <deque>
#include <map>
#include <vector>


//#define ANTICACHE_DB_NAME "anticache.db"

using namespace std;

namespace voltdb {
    
class ExecutorContext;
class AntiCacheDB;
class AntiCacheStats;

/**
 * Wrapper class for an evicted block that has been read back in 
 * from the AntiCacheDB
 */
class AntiCacheBlock {
    friend class AntiCacheDB;
    
    public:
        virtual ~AntiCacheBlock() {};
        
        inline uint32_t getBlockId() const {
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
            uint32_t blockId;
            std::string tableName;
            char * data;
            long size;
        };

        inline AntiCacheDBType getBlockType() const {
            return m_blockType;
        }
    
    protected:
        // Why is this private/protected?
        AntiCacheBlock(uint32_t blockId);
        uint32_t m_blockId;
        payload m_payload;
        int32_t m_size;
        char * m_block;
        char * m_buf;
        // probably should be changed to a final/const
        AntiCacheDBType m_blockType;
}; // CLASS

class AntiCacheDB {
        
    public: 
       
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize);
        virtual ~AntiCacheDB();

        /**
         * Write a block of serialized tuples out to the anti-cache database
         */
        virtual void writeBlock(const std::string tableName,
                                uint32_t blockId,
                                const int tupleCount,
                                const char* data,
                                const long size, const int evictedTupleCount) = 0;
        /**
         * Read a block and return its contents
         */
        virtual AntiCacheBlock* readBlock(uint32_t blockId, bool isMigrate) = 0;

        virtual bool validateBlock(uint32_t blockId) = 0;


        /**
         * Flush the buffered blocks to disk.
         */
        virtual void flushBlocks() = 0;
        
        virtual void setStatsSource();

        /**
         * Return the next BlockId to use in the anti-cache database
         */
        virtual uint32_t nextBlockId() = 0;
        /**
         * Return the AntiCacheDBType of the database
         */
        inline AntiCacheDBType getDBType() {
            return m_dbType;
        }
        /**
         * Return the blockSize of stored blocks
         */
        inline long getBlockSize() {
            return m_blockSize;
        }
        /**
         * Return the number of blocks stored in the database
         */
        inline int getNumBlocks() {
            return m_totalBlocks;
        }
        /**
         * Return the maximum size of the database in bytes
         */
        inline int64_t getMaxDBSize() {
            return m_maxDBSize;
        }
        /**
         * Return the maximum number of blocks that can be stored 
         * in the database.
         */
        inline int getMaxBlocks() {
            return (int)(m_maxDBSize/m_blockSize);
        }
        /**
         * Return the number of free (available) blocks
         */
        inline int getFreeBlocks() {
            return getMaxBlocks()-getNumBlocks();
        }
        /**
         * Return the LRU block from the database. This *removes* the block
         * from the database. If you take it, it's yours, it exists nowhere else.
         * If a migrate or merge fails, you have to write it back. 
         *
         * It also updates removes the blockId from the LRU deque.
         */
        AntiCacheBlock* getLRUBlock();       

        /**
         * Removes a blockId from the LRU queue. This is used when reading a
         * specific block.
         */
        void removeBlockLRU(uint32_t blockId);
        
        /** 
         * Adds a blockId to the LRU deque.
         */
        void pushBlockLRU(uint32_t blockId);

        /**
         * Pops and returns the LRU blockID from the deque. This isn't a 
         * peek. When this function finishes, the block is in the database
         * but the blockId is no longer in the LRU. This shouldn't necessarily
         * be fatal, but it should be avoided.
         */
        inline uint32_t popBlockLRU();

        /**
         * Set the AntiCacheID number. This should be done on initialization and
         * should also match the the level in VoltDBEngine/executorcontext
         */
        inline void setACID(int16_t ACID) {
            m_ACID = ACID;
        }

        virtual inline AntiCacheStats* getACDBStats() {
            return m_stats;
        }

        /**
         * Change anticache memory stats for the removal of a single tuple.
         * Because of the structure of AnticacheDB we have to have this another
         * function.
         */
        void removeSingleTupleStats(uint32_t blockId, int32_t sign);
        
        /**
         * Return the AntiCacheID number.
         */
        inline int16_t getACID() {
            return m_ACID;
        }

        /**
         * return true if we block to fetch a block, false if we abort and issue a merge
         */
        inline bool isBlocking() {
            return m_blocking;
        }
        
        /**
         * Set whether we block or abort
         */
        inline void setBlocking(bool blocking) {
            m_blocking = blocking;
        }

        /*
         * return current count of evicted blocks
         */
        inline int32_t getBlocksEvicted() {
            return m_blocksEvicted;
        }
        
        /* 
         * clear current count of evicted blocks
         */
        inline void clearBlocksEvicted() {
            m_blocksEvicted = 0;
        }
        
        /*
         * return current count of evicted bytes
         */

        inline int64_t getBytesEvicted() {
            return m_bytesEvicted;
        }

        /*
         * clear current count of evicted bytes
         */
        inline void clearBytesEvicted() {
            m_bytesEvicted = 0;
        }
        
        /* 
         * return current count of unevicted blocks
         */
        inline int32_t getBlocksUnevicted() {
            return m_blocksUnevicted;
        }

        /*
         * clear current count of unevicted blocks
         */
        inline void clearBlocksUnevicted() {
            m_blocksUnevicted = 0;
        }

        /* 
         * return current count of unevicted bytes
         */
        inline int64_t getBytesUnevicted() {
            return m_bytesUnevicted;
        }

        /*
         * clear current couont of unevicted bytes
         */
        inline void clearBytesUnevicted() {
            m_bytesUnevicted = 0;
        }
        
        /*
         * Set to block merge
         */
        inline void setBlockMerge(bool block_merge) {
            m_block_merge = block_merge;
        }

        /*
         * Are we merging the entire block or just a single tuple?
         */
        inline bool isBlockMerge() {
            return m_block_merge;
        }

        inline int getTupleInBlock(uint32_t blockId) {
            return tupleInBlock[blockId];
        }
        
        inline int getEvictedTupleInBlock(uint32_t blockId) {
            return evictedTupleInBlock[blockId];
        }

    protected:
        ExecutorContext *m_executorContext;
        string m_dbDir;

        uint32_t m_nextBlockId;
        int16_t m_ACID;
        long m_blockSize;
        int m_partitionId; 
        int m_totalBlocks; 
        

        bool m_blocking;
        bool m_block_merge;

        AntiCacheDBType m_dbType;
        int64_t m_maxDBSize;
        

        /*
         * stats
         */
        int64_t m_bytesEvicted;
        int32_t m_blocksEvicted;
        int64_t m_bytesUnevicted;
        int32_t m_blocksUnevicted;

        std::map <uint32_t, int> tupleInBlock;
        std::map <uint32_t, int> evictedTupleInBlock;
        std::map <uint32_t, long> blockSize;

        voltdb::AntiCacheStats* m_stats;
        
        /* we need to test whether a deque or list is better. If we push/pop more than we
         * remove, this is better. otherwise, let's use a list
         */

        std::deque<uint32_t> m_block_lru;

        /*
         * DB specific method of shutting down the database on destructor call
         */
        virtual void shutdownDB() = 0;

        
}; // CLASS

}
#endif
