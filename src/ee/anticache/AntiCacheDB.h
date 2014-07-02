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
//#include "common/executorcontext.hpp"
#include <map>
#include <vector>

#define BERKELEYDB 0
#define NVM 1

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
        AntiCacheBlock(int16_t blockId, Dbt value);
        AntiCacheBlock();
        ~AntiCacheBlock();
        
        inline int16_t getBlockType() {
            return m_blockType;
        } 
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
    
    protected:
        AntiCacheBlock(int16_t blockId, char* block, long size);
        int16_t m_blockId;
        payload m_payload;
        long m_size;
        char * m_block;
        char * m_buf;
        int16_t m_blockType;

}; // CLASS

/**
 *
 */
class AntiCacheDB {
        
    public: 
        // probably want to remove db_dir and add a "db_type" field
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize);
        ~AntiCacheDB();

        void initialize();

        int inline getDBType() {
            return m_dbType;
        }

        /*
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
        virtual void flushBlocks();

        /**
         * Return the next BlockId to use in the anti-cache database
         * This is guaranteed to be unique per partition
         */
        inline int16_t nextBlockId() {
            return (++m_nextBlockId);
        }
        
    protected:
        ExecutorContext *m_executorContext;
        string m_dbDir;     // probably move
        long m_blockSize;
        int16_t m_nextBlockId;
        int m_partitionId; 

        /**
         *  Maps a block id to a <index, size> pair
         */
        std::map<int16_t, pair<int, int32_t> > m_blockMap; 
        
        
        int m_totalBlocks; 
        int m_nextFreeBlock; 

        virtual void shutdownDB();      
        
        int m_dbType;
        
        
        
        // these may not need to be generic?
        // char* getBlock(int index);
        
        
}; // CLASS

}
#endif
