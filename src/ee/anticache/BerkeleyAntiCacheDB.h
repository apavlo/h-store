#ifndef BERKELEYANTICACHE_H
#define BERKELEYANTICACHE_H

#include <db_cxx.h>

#include "common/debuglog.h"
#include "common/DefaultTupleSerializer.h"

#include <map>
#include <vector>
#include <set>

using namespace std;

namespace voltdb {

class ExecutorContext;
class AntiCacheDB;

class BerkeleyAntiCacheBlock : public AntiCacheBlock {
    friend class BerkeleyAntiCacheDB;
    friend class AntiCacheDB;

    public:
        ~BerkeleyAntiCacheBlock();

    private:
        BerkeleyAntiCacheBlock(uint32_t blockId, Dbt value);
};

// Encapsulates a block that is flushed out to BerkeleyDB
// TODO: merge it with AntiCacheBlock
class BerkeleyDBBlock{
public:
    ~BerkeleyDBBlock();

    inline void initialize(long blockSize, std::vector<std::string> tableNames, int32_t blockId, int numTuplesEvicted){
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

class BerkeleyAntiCacheDB : public AntiCacheDB {
    public:
        BerkeleyAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize);
        ~BerkeleyAntiCacheDB(); 

        inline uint32_t nextBlockId() {
            return (++m_nextBlockId);
        }
        void initializeDB();

        AntiCacheBlock* readBlock(uint32_t blockId, bool isMigrate);

        void shutdownDB();

        void flushBlocks();

        void writeBlock(const std::string tableName,
                        uint32_t blockID,
                        const int tupleCount,
                        const char* data,
                        const long size,
                        const int evictedTupleCount);

        bool validateBlock(uint32_t blockID);

    private:
        DbEnv* m_dbEnv;
        Db* m_db;
        Dbt m_value;
        std::set <uint32_t> m_blockSet;
};

}

#endif
