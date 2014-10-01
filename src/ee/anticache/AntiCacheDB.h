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

#include <deque>
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

        inline AntiCacheDBType getBlockType() const {
            return m_blockType;
        }
    
    protected:
        // Why is this private/protected?
        AntiCacheBlock(int16_t blockId);
        int16_t m_blockId;
        payload m_payload;
        long m_size;
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
                                int16_t blockId,
                                const int tupleCount,
                                const char* data,
                                const long size) = 0;
        /**
         * Read a block and return its contents
         */
        virtual AntiCacheBlock* readBlock(int16_t blockId) = 0;


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
        /**
         * Return the AntiCacheDBType of the database
         */
        inline AntiCacheDBType getDBType() {
            return m_dbType;
        }

        inline long getMaxBlockSize() {
            return m_maxBlockSize;
        }

        inline long getMaxDBSize() {
            return m_maxDBSize;
        }

        inline int getMaxBlocks() {
            return (int)(m_maxDBSize/m_blockSize);
        }
        
        inline AntiCacheBlock* getLRUBlock() {
            uint16_t lru_block_id;
            AntiCacheBlock* lru_block;

            if (m_block_lru.empty()) {
                VOLT_ERROR("LRU Blocklist Empty!");
                throw UnknownBlockAccessException(0);
            } else {
                lru_block_id = m_block_lru.front();
                m_block_lru.pop_front();
                lru_block = readBlock(lru_block_id);

                return lru_block;
            }
        }

        inline void removeBlockLRU(uint16_t blockId) {
            std::deque<uint16_t>::iterator it;
            bool found = false;
           
    
            for (it = m_block_lru.begin(); it != m_block_lru.end(); ++it) {
                if (*it == blockId) {
                    VOLT_INFO("Found block id %d == blockId %d", *it, blockId);
                    m_block_lru.erase(it);
                    found = true;
                    break;
                }
            }

            if (!found) {
                VOLT_ERROR("Found block but didn't find blockId %d in LRU!", blockId);
                //throw UnknownBlockAccessException(blockId);
            }
        }

        inline void pushBlockLRU(uint16_t blockId) {
            VOLT_INFO("Pushing blockId %d into LRU", blockId);
            m_block_lru.push_back(blockId);
        }

        inline uint16_t popBlockLRU() {
            uint16_t blockId = m_block_lru.front();
            m_block_lru.pop_front();
            return blockId;
        }


    protected:
        ExecutorContext *m_executorContext;
        string m_dbDir;

        int16_t m_nextBlockId;
        long m_blockSize;
        int m_partitionId; 
        int m_totalBlocks; 

        AntiCacheDBType m_dbType;
        long m_maxBlockSize;        
        long m_maxDBSize;

        /* we need to test whether a deque or list is better. If we push/pop more than we
         * remove, this is better. otherwise, let's use a list
         */

        std::deque<uint16_t> m_block_lru;

        /*
         * DB specific method of shutting down the database on destructor call
         */
        virtual void shutdownDB() = 0;

        
}; // CLASS

}
#endif
