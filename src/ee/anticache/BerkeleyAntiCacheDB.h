#ifndef BERKELEYANTICACHE_H
#define BERKELEYANTICACHE_H

#include <db_cxx.h>

#include "common/debuglog.h"
#include "common/DefaultTupleSerializer.h"

#include <map>
#include <vector>

#define ANTICACHE_DB_NAME "anticache.db"

using namespace std;

namespace voltdb {

class ExecutorContext;
class AntiCacheDB;
class BerkeleyAntiCacheDB;

class BerkeleyAntiCacheBlock: public AntiCacheBlock {
	friend class BerkeleyAntiCacheDB;
	public:
		~BerkeleyAntiCacheBlock();	
	private:
		BerkeleyAntiCacheBlock(int16_t blockId, Dbt value);
}; 

// Encapsulates a block that is flushed out to BerkeleyDB
// TODO: merge it with AntiCacheBloc

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

class BerkeleyAntiCacheDB: public AntiCacheDB {

	public:
		BerkeleyAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize); 
//			AntiCacheDB(ctx, db_dir, blockSize);
		~BerkeleyAntiCacheDB();
		void writeBlock(const std::string tableName,
						int16_t blockID,
						const int tupleCount,
						const char* data,
						const long size);
		
		void initialize();
		BerkeleyAntiCacheBlock readBlock(std::string tableName, int16_t blockId);

		// do we need a return flag? Probably success or failure, no?
		
		void flushBlocks(); 

	private:
		//string m_dbDir;
		Db* m_db;
		DbEnv* m_dbEnv;	
		void shutdownDB();	
		
		BerkeleyAntiCacheBlock readBlockBerkeleyDB(int16_t blockId);
		void writeBlockBerkeleyDB(	const std::string tableName, 
					   int16_t blockID, 
					   const int tupleCount, 
					   const char* data, 
					   const long size);

};
}
#endif	
