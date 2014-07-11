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
<<<<<<< HEAD
#include "common/types.h"
//#include "common/executorcontext.hpp"
#include <map>
#include <vector>

=======

#include <map>
#include <vector>


#define ANTICACHE_DB_NAME "anticache.db"

>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.
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
<<<<<<< HEAD
//        AntiCacheBlock(int16_t blockId, Dbt value);
//        AntiCacheBlock(int16_t blockId, char* block, long size);
=======
>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.
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
        inline AntiCacheDBType getBlockType() {
            return m_blockType; 
        }
        struct payload{
            int16_t blockId;
            std::string tableName;
            char * data;
            long size;
        };
    
    private:
        AntiCacheBlock(int16_t blockId, Dbt value);
        AntiCacheBlock(int16_t blockId, char* block, long size);
        int16_t m_blockId;
        payload m_payload;
<<<<<<< HEAD
        long m_size;
        char * m_block;
        char * m_buf;
        AntiCacheDBType m_blockType;
=======
	long m_size;
	char * m_block;
	char * m_buf;
>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.

}; // CLASS

// Encapsulates a block that is flushed out to BerkeleyDB
// TODO: merge it with AntiCacheBlock
class BerkeleyDBBlock{
public:
	~BerkeleyDBBlock();

    inline void initialize(long blockSize, std::vector<std::string> tableNames, int16_t blockId, int numTuplesEvicted){
        DefaultTupleSerializer serializer;
        // buffer used for serializing a single tuple
        serialized_data = new char[blockSize];
        out.initializeWithPosition(serialized_data, blockSize, 0);
        out.writeInt((int)tableNames.size());
        for (std::vector<std::string>::iterator it = tableNames.begin() ; it != tableNames.end(); ++it){
			out.writeTextString(*it);
			// note this offset since we need to write at this again later on
			offsets.push_back(getSerializedSize());
			out.writeInt(numTuplesEvicted);// reserve first 4 bytes in buffer for number of tuples in block
        }


    }

    inline void addTuple(TableTuple tuple){
    	// Now copy the raw bytes for this tuple into the serialized buffer
        tuple.serializeWithHeaderTo(out);
    }

    inline void writeHeader(std::vector<int> num_tuples_evicted){
    	// write out the block header (i.e. number of tuples in block)
    	int count = 0;
    	for (std::vector<int>::iterator it = num_tuples_evicted.begin() ; it != num_tuples_evicted.end(); ++it){
        	out.writeIntAt(offsets.at(count), *it);
        	count++;
    	}
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
    std::vector<int> offsets;

};
/**
 *
 */
class AntiCacheDB {
        
    public: 
<<<<<<< HEAD
        // probably want to remove db_dir and add a "db_type" field
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize);
        ~AntiCacheDB();

        virtual void initialize();

        AntiCacheDBType inline getDBType() {
            return m_dbType;
        }

        /*
         * Write a block of serialized tuples out to the anti-cache database
         */
        virtual void writeBlock(const std::string tableName,
=======
        AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize);
        ~AntiCacheDB();

		void initializeNVM(); 
		
		void initializeBerkeleyDB(); 

        /**
         * Write a block of serialized tuples out to the anti-cache database
         */
        void writeBlock(const std::string tableName,
>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.
                        int16_t blockId,
                        const int tupleCount,
                        const char* data,
                        const long size);
<<<<<<< HEAD
        
        /**
         * Read a block and return its contents
         */
        virtual AntiCacheBlock readBlock(std::string tableName, int16_t blockId) = 0;
        
=======
        /**
         * Read a block and return its contents
         */
        AntiCacheBlock readBlock(std::string tableName, int16_t blockId);


>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.
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
<<<<<<< HEAD
        string m_dbDir;     // probably move
=======
        string m_dbDir;
>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.
        long m_blockSize;
        DbEnv* m_dbEnv;
        Db* m_db; 
        int16_t m_nextBlockId;
<<<<<<< HEAD
        int m_partitionId; 
=======
	int m_partitionId; 

        FILE* nvm_file;
        char* m_NVMBlocks; 
        int nvm_fd; 
>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.

        /**
         *  Maps a block id to a <index, size> pair
         */
<<<<<<< HEAD
        std::map<int16_t, pair<int, int32_t> > m_blockMap; 
        
        
        int m_totalBlocks; 
        int m_nextFreeBlock; 

        virtual void shutdownDB();      
        
        AntiCacheDBType m_dbType;
        
=======
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

        AntiCacheBlock readBlockBerkeleyDB(int16_t blockId);
        
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
>>>>>>> parent of 62de730... Made AntiCacheDB a generic class, split BerkeleyDB and NVM to separate subclasses. Did not change tests nor EE.
        
}; // CLASS

}
#endif
