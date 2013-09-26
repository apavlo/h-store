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

#include "anticache/AntiCacheDB.h"
#include "anticache/UnknownBlockAccessException.h"
#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace std;

namespace voltdb {

  AntiCacheBlock::AntiCacheBlock(int16_t blockId, Dbt value, char* nvmBlock, long size) :
        m_blockId(blockId),
        m_value(value),
        m_NVMBlock(nvmBlock),
        m_size(size) {
    // They see me rollin'
    // They hatin'
}

AntiCacheBlock::~AntiCacheBlock() {
    // we asked BDB to allocate memory for data dynamically, so we must delete
    if(m_blockId > 0)
        delete [] (char*)m_value.get_data(); 
}
    
AntiCacheDB::AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize) :
    m_executorContext(ctx),
    m_dbDir(db_dir),
    m_blockSize(blockSize),
    m_nextBlockId(0),
    m_totalBlocks(0) {
	
	#ifdef ANTICACHE_NVM
		initializeNVM(); 
	#else
		initializeBerkeleyDB(); 
	#endif
}

void AntiCacheDB::initializeBerkeleyDB()
{
        u_int32_t env_flags =
        DB_CREATE       | // Create the environment if it does not exist
//        DB_AUTO_COMMIT  | // Immediately commit every operation
        DB_INIT_MPOOL   | // Initialize the memory pool (in-memory cache)
//        DB_TXN_NOSYNC   | // Don't flush to disk every time, we will do that explicitly
//        DB_INIT_LOCK    | // concurrent data store
        DB_PRIVATE      |
        DB_THREAD       | // allow multiple threads
//        DB_INIT_TXN     |
        DB_DIRECT_DB;     // Use O_DIRECT

    try {
        // allocate and initialize Berkeley DB database env
        m_dbEnv = new DbEnv(0); 
        m_dbEnv->open(m_dbDir.c_str(), env_flags, 0); 

        // allocate and initialize new Berkeley DB instance
        m_db = new Db(m_dbEnv, 0); 
        m_db->open(NULL, ANTICACHE_DB_NAME, NULL, DB_HASH, DB_CREATE, 0); 

    } catch (DbException &e) {
        VOLT_ERROR("Anti-Cache initialization error: %s", e.what());
        VOLT_ERROR("Failed to initialize anti-cache database in directory %s", m_dbDir.c_str());
        throwFatalException("Failed to initialize anti-cache database in directory %s: %s",
                            m_dbDir.c_str(), e.what());
    }
}

void AntiCacheDB::initializeNVM()
{
	off_t NVM_FILE_SIZE = 1073741824; 
	
	int nvm_file = open(m_dbDir.c_str(), O_RDWR); 
	
	if(ftruncate(nvm_file, NVM_FILE_SIZE) < 0)
	{
		VOLT_ERROR("Anti-Cache initialization error."); 
		VOLT_ERROR("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
		throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
	}
	
	char* m_NVMBlock = new char[NVM_FILE_SIZE]; 
	mmap(NULL, NVM_FILE_SIZE, 0, 0, nvm_file, 0); 
	
	m_NVMBlocks = new char*[5000]; 
}

void AntiCacheDB::shutdownBerkeleyDB(){
	
	// NOTE: You have to close the database first before closing the environment
    try {
        m_db->close(0);
        delete m_db;
    } catch (DbException &e) {
        VOLT_ERROR("Anti-Cache database closing error: %s", e.what());
        throwFatalException("Failed to close anti-cache database: %s", e.what());
    }
    
    try {
        m_dbEnv->close(0);
        delete m_dbEnv;
    } catch (DbException &e) {
        VOLT_ERROR("Anti-Cache environment closing error: %s", e.what());
        throwFatalException("Failed to close anti-cache database environment: %s", e.what());
    }
}

void AntiCacheDB::shutdownNVM(){
	
}


AntiCacheDB::~AntiCacheDB() {
	#ifdef ANTICACHE_NVM
		shutdownNVM(); 
	#else
		shutdownBerkeleyDB(); 
	#endif
}

void AntiCacheDB::writeBlockBerkeleyDB(const std::string tableName,
                             int16_t blockId,
                             const int tupleCount,
                             const char* data,
                             const long size) {
    Dbt key; 
    key.set_data(&blockId);
    key.set_size(sizeof(int16_t));

    Dbt value;
    value.set_data(const_cast<char*>(data));
    value.set_size(static_cast<int32_t>(size)); 

    VOLT_DEBUG("Writing out a block #%d to anti-cache database [tuples=%d / size=%ld]",
               blockId, tupleCount, size);
    // TODO: Error checking
    m_db->put(NULL, &key, &value, 0);
}

AntiCacheBlock AntiCacheDB::readBlockBerkeleyDB(std::string tableName, int16_t blockId) {
    Dbt key;
    key.set_data(&blockId);
    key.set_size(sizeof(int16_t));

    Dbt value;
    value.set_flags(DB_DBT_MALLOC);
    
    VOLT_DEBUG("Reading evicted block with id %d", blockId);
    
    int ret_value = m_db->get(NULL, &key, &value, 0);
    if (ret_value != 0) {
        VOLT_ERROR("Invalid anti-cache blockId '%d' for table '%s'", blockId, tableName.c_str());
        throw UnknownBlockAccessException(tableName, blockId);
    }
    else {
//        m_db->del(NULL, &key, 0);  // if we have this the benchmark won't end
        assert(value.get_data() != NULL);
    }
    
    AntiCacheBlock block(blockId, value, NULL, value.get_size());
    return (block);
}

void AntiCacheDB::writeBlockNVM(const std::string tableName,
                             int16_t blockId,
                             const int tupleCount,
                             const char* data,
                             const long size) {

   m_NVMBlocks[m_totalBlocks] = new char[size]; 
   memcpy(m_NVMBlocks[m_totalBlocks], data, size); 
   //memcpy(m_NVMBlocks[m_totalBlocks], data, sizeof(char*)); 

   VOLT_DEBUG("Writing NVM Block: ID = %d, index = %d, size = %ld", blockId, m_totalBlocks, size); 
   m_blockMap.insert(std::pair<int16_t, std::pair<int, long> >(blockId, std::pair<int, long>(m_totalBlocks, size))); 
   m_totalBlocks++; 
}

AntiCacheBlock AntiCacheDB::readBlockNVM(std::string tableName, int16_t blockId) {

   Dbt empty;
   std::map<int16_t, std::pair<int, long> >::iterator itr; 
   itr = m_blockMap.find(blockId); 
  
   if (itr == m_blockMap.end()) {
     VOLT_ERROR("Invalid anti-cache blockId '%d' for table '%s'", blockId, tableName.c_str());
     throw UnknownBlockAccessException(tableName, blockId);
   }

   int blockIndex = itr->second.first; 
   VOLT_DEBUG("Reading NVM block: ID = %d, index = %d, size = %ld.", blockId, blockIndex, itr->second.second);
 
   AntiCacheBlock block(blockId, empty, m_NVMBlocks[blockIndex], itr->second.second);
   return (block);
}


void AntiCacheDB::writeBlock(const std::string tableName,
                             int16_t blockId,
                             const int tupleCount,
                             const char* data,
                             const long size) {
    #ifdef ANTICACHE_NVM
		return writeBlockNVM(tableName, blockId, tupleCount, data, size); 
	#else
		return writeBlockBerkeleyDB(tableName, blockId, tupleCount, data, size);
	#endif
}

AntiCacheBlock AntiCacheDB::readBlock(std::string tableName, int16_t blockId) {
    #ifdef ANTICACHE_NVM
		return readBlockNVM(tableName, blockId);
	#else
		return readBlockBerkeleyDB(tableName, blockId); 
	#endif
}
    
void AntiCacheDB::flushBlocks()
{
    m_db->sync(0); 
}
    
}

