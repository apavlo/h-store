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
#include "common/executorcontext.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>

using namespace std;

namespace voltdb {

AntiCacheBlock::AntiCacheBlock(int16_t blockId, Dbt value) {
    m_buf = (char *) value.get_data();

    int16_t id = *((int16_t *)m_buf);
    long bufLen_ = sizeof(int16_t);
    std::string tableName = m_buf + bufLen_;
    bufLen_ += tableName.size()+1;
    long size = *((long *)(m_buf+bufLen_));
    bufLen_+=sizeof(long);
    char * data = m_buf + bufLen_;
    bufLen_ += size;
//    for(int i=0;i<size;i++){
//       VOLT_INFO("%x", data[i]);
//    }

    payload p;
    p.blockId = id;
    p.tableName = tableName;
    p.data = data;
    p.size = size;
    m_size = size;
    m_payload = p;
    m_block = m_payload.data;
    m_blockId = blockId;
    
    VOLT_DEBUG("AntiCachBlock #%d [size=%ld / payload=%ld]",
              blockId, m_size, m_payload.size);
  }

AntiCacheBlock::AntiCacheBlock(int16_t blockId, char* block, long size) {
        m_block = block;
        m_blockId = blockId;
        m_size = size;
    }

AntiCacheBlock::~AntiCacheBlock() {
    // we asked BDB to allocate memory for data dynamically, so we must delete
    if(m_blockId > 0 && m_buf != NULL){
        delete m_buf;
    }
}

BerkeleyDBBlock::~BerkeleyDBBlock() {
    delete [] serialized_data;
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

void AntiCacheDB::initializeBerkeleyDB() {
    
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

void AntiCacheDB::initializeNVM() {
    
    char nvm_file_name[150];
    char partition_str[50];

    m_totalBlocks = 0; 

    #ifdef ANTICACHE_DRAM
        VOLT_INFO("Allocating anti-cache in DRAM."); 
        m_NVMBlocks = new char[aligned_file_size];
    return; 
    #endif

    // use executor context to figure out which partition we are at
    int partition_id = (int)m_executorContext->getPartitionId(); 
    sprintf(partition_str, "%d", partition_id); 

    strcpy(nvm_file_name, m_dbDir.c_str()); 
    // there will be one NVM anti-cache file per partition, saved in /mnt/pmfs/anticache-XX
    strcat(nvm_file_name, "/anticache-");
    strcat(nvm_file_name, partition_str);
    VOLT_INFO("Creating nvm file: %s", nvm_file_name); 
    nvm_file = fopen(nvm_file_name, "w"); 

    if(nvm_file == NULL) {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to open PMFS file %s: %s.", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    fclose(nvm_file); 
    nvm_file = fopen(nvm_file_name, "rw+"); 

    if(nvm_file == NULL)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to open PMFS file %s: %s.", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    nvm_fd = fileno(nvm_file); 
    if(nvm_fd < 0)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to allocate anti-cache PMFS file in directory %s.", m_dbDir.c_str());
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }
    
    if(ftruncate(nvm_fd, NVM_FILE_SIZE) < 0)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to ftruncate anti-cache PMFS file %s: %s", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    //off_t aligned_file_size = (((NVM_FILE_SIZE) + MMAP_PAGE_SIZE - 1) / MMAP_PAGE_SIZE * MMAP_PAGE_SIZE);  
    off_t aligned_file_size = NVM_FILE_SIZE; 

    m_NVMBlocks =  (char*)mmap(NULL, aligned_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, nvm_fd, 0);
 
    if(m_NVMBlocks == MAP_FAILED)
    {
        VOLT_ERROR("Anti-Cache initialization error."); 
        VOLT_ERROR("Failed to mmap PMFS file %s: %s", nvm_file_name, strerror(errno));
        throwFatalException("Failed to initialize anti-cache PMFS file in directory %s.", m_dbDir.c_str());
    }

    close(nvm_fd); // can safely close file now, mmap creates new reference

    /*
    // write out NULL characters to ensure entire file has been fetchted from memory
    for(int i = 0; i < NVM_FILE_SIZE; i++)
    {
        m_NVMBlocks[i] = '\0'; 
    }
    */
}

void AntiCacheDB::shutdownBerkeleyDB() {
    
    // NOTE: You have to close the database first before closing the environment
    try 
    {
        m_db->close(0);
        delete m_db;
    } catch (DbException &e) 
    {
        VOLT_ERROR("Anti-Cache database closing error: %s", e.what());
        throwFatalException("Failed to close anti-cache database: %s", e.what());
    }
    
    try 
    {
        m_dbEnv->close(0);
        delete m_dbEnv;
    } catch (DbException &e) 
    {
        VOLT_ERROR("Anti-Cache environment closing error: %s", e.what());
        throwFatalException("Failed to close anti-cache database environment: %s", e.what());
    }
}

void AntiCacheDB::shutdownNVM() {   
  fclose(nvm_file);

  #ifdef ANTICACHE_DRAM 
      delete [] m_NVMBlocks;
  #endif 
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
    key.set_size(sizeof(blockId));


    char * databuf_ = new char [size+tableName.size() + 1+sizeof(blockId)+sizeof(size)];
    memset(databuf_, 0, size+tableName.size() + 1+sizeof(blockId)+sizeof(size));
    // Now pack the data into a single contiguous memory location
    // for storage.
    long bufLen_ = 0;
    long dataLen = 0;
    dataLen = sizeof(blockId);
    memcpy(databuf_, &blockId, dataLen);
    bufLen_ += dataLen;
    dataLen = tableName.size() + 1;
    memcpy(databuf_ + bufLen_, tableName.c_str(), dataLen);
    bufLen_ += dataLen;
    dataLen = sizeof(size);
    memcpy(databuf_ + bufLen_, &size, dataLen);
    bufLen_ += dataLen;
    dataLen = size;
    memcpy(databuf_ + bufLen_, data, dataLen);
    bufLen_ += dataLen;

    Dbt value;
    value.set_data(databuf_);
    value.set_size(static_cast<int32_t>(bufLen_));


    VOLT_INFO("Writing out a block #%d to anti-cache database [tuples=%d / size=%ld]",
               blockId, tupleCount, size);
    // TODO: Error checking
    m_db->put(NULL, &key, &value, 0);

    delete [] databuf_;
}

AntiCacheBlock AntiCacheDB::readBlockBerkeleyDB(int16_t blockId) {
    

    Dbt key;
    key.set_data(&blockId);
    key.set_size(sizeof(blockId));

    Dbt value;
    value.set_flags(DB_DBT_MALLOC);
    
    VOLT_DEBUG("Reading evicted block with id %d", blockId);
    
    int ret_value = m_db->get(NULL, &key, &value, 0);

    if (ret_value != 0) 
    {
        VOLT_ERROR("Invalid anti-cache blockId '%d'", blockId);
        throw UnknownBlockAccessException(blockId);
    }
    else 
    {
//        m_db->del(NULL, &key, 0);  // if we have this the benchmark won't end
        assert(value.get_data() != NULL);
    }
    
    AntiCacheBlock block(blockId, value);
    return (block);
}

void AntiCacheDB::writeBlockNVM(const std::string tableName,
                            int16_t blockId,
                            const int tupleCount,
                            const char* data,
                            const long size)  {
   
  //int index = getFreeNVMBlockIndex();
  //char* block = getNVMBlock(index);
    char* block = getNVMBlock(m_totalBlocks); 
    memcpy(block, data, size);                      
   //m_NVMBlocks[m_totalBlocks] = new char[size]; 
   //memcpy(m_NVMBlocks[m_totalBlocks], data, size); 

    VOLT_INFO("Writing NVM Block: ID = %d, index = %d, size = %ld", blockId, m_totalBlocks, size); 
    m_blockMap.insert(std::pair<int16_t, std::pair<int, int32_t> >(blockId, std::pair<int, int32_t>(m_totalBlocks, static_cast<int32_t>(size))));
    m_totalBlocks++; 
}

AntiCacheBlock AntiCacheDB::readBlockNVM(std::string tableName, int16_t blockId) {
    
   std::map<int16_t, std::pair<int, int32_t> >::iterator itr; 
   itr = m_blockMap.find(blockId); 
  
   if (itr == m_blockMap.end()) {
     VOLT_INFO("Invalid anti-cache blockId '%d' for table '%s'", blockId, tableName.c_str());
     VOLT_ERROR("Invalid anti-cache blockId '%d' for table '%s'", blockId, tableName.c_str());
     throw UnknownBlockAccessException(tableName, blockId);
   }

   int blockIndex = itr->second.first; 
   VOLT_INFO("Reading NVM block: ID = %d, index = %d, size = %d", blockId, blockIndex, itr->second.second);
   
   char* block_ptr = getNVMBlock(blockIndex);
   char* block = new char[itr->second.second];
   memcpy(block, block_ptr, itr->second.second); 

   AntiCacheBlock anticache_block(blockId, block, itr->second.second);
   
   freeNVMBlock(blockId); 

   m_blockMap.erase(itr); 
   return (anticache_block);
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
        return readBlockBerkeleyDB(blockId);
    #endif
}
    
void AntiCacheDB::flushBlocks() {
    
    #ifdef ANTICACHE_NVM
        //msync(m_NVMBlocks, NVM_FILE_SIZE, MS_SYNC); 
    #else 
        m_db->sync(0);
    #endif
}

char* AntiCacheDB::getNVMBlock(int index) {
    
  //char* nvm_block = new char[NVM_BLOCK_SIZE];     
  //memcpy(nvm_block, m_NVMBlocks+(index*NVM_BLOCK_SIZE), NVM_BLOCK_SIZE); 
    
  //return nvm_block; 
  return (m_NVMBlocks+(index*NVM_BLOCK_SIZE));  
}

int AntiCacheDB::getFreeNVMBlockIndex() {
  
    int free_index = 0; 
    if(m_NVMBlockFreeList.size() > 0)
    {
        free_index = m_NVMBlockFreeList.back(); 
    m_NVMBlockFreeList.pop_back(); 
    }
    else 
    {
        free_index = m_nextFreeBlock;
        m_nextFreeBlock++;  
    }
  
    //int free_index = m_totalBlocks++; 
    return free_index; 
}

void AntiCacheDB::freeNVMBlock(int index) {
    m_NVMBlockFreeList.push_back(index); 
    //m_totalBlocks--; 
}
    
}

