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
        inline int getSize() const {
            return (m_value.get_size());
        }
        inline char* getData() const {
            return (static_cast<char*>(m_value.get_data()));
        }
    
    private:
        AntiCacheBlock(int16_t blockId, Dbt value);
        
        int16_t m_blockId;
        Dbt m_value;
}; // CLASS

/**
 *
 */
class AntiCacheDB {
        
    public: 
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize);
        ~AntiCacheDB();

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
        ExecutorContext *m_executorContext;
        string m_dbDir;
        long m_blockSize;
        DbEnv* m_dbEnv;
        Db* m_db; 
        int16_t m_nextBlockId;
}; // CLASS

}
#endif












