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
    
    private:
        AntiCacheBlock(int16_t blockId, Dbt value);
        int16_t m_blockId;
        payload m_payload;
		long m_size;
		char * m_block;
		char * m_buf;

}; // CLASS

// Encapsulates a block that is flushed out to BerkeleyDB
// TODO: merge it with AntiCacheBlock
class BerkeleyDBBlock{
public:
	~BerkeleyDBBlock();

    inline void initialize(long blockSize, std::string tableName, int16_t blockId, int numTuplesEvicted){
        DefaultTupleSerializer serializer;
        // buffer used for serializing a single tuple
        serialized_data = new char[blockSize];
        out.initializeWithPosition(serialized_data, blockSize, 0);
        out.writeInt(numTuplesEvicted);// reserve first 4 bytes in buffer for number of tuples in block

    }

    inline void addTuple(TableTuple tuple){
    	// Now copy the raw bytes for this tuple into the serialized buffer
        tuple.serializeWithHeaderTo(out);
    }

    inline void writeHeader(int num_tuples_evicted){
    	// write out the block header (i.e. number of tuples in block)
    	out.writeIntAt(0, num_tuples_evicted);
    }

    inline int getSerializedSize(){
    	return (int)out.size();
    }

    inline const char* getSerializedData(){
    	return out.data();
    }
private:
    ReferenceSerializeOutput out;
    char * serialized_data;

};
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
        AntiCacheBlock readBlock(int16_t blockId);
    
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












