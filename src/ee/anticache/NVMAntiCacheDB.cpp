
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
#include "common/executorcontext.hpp"
#include "anticache/AntiCacheDB.h"
#include "anticache/UnknownBlockAccessException.h"
#include "anticache/NVMAntiCacheDB.h"

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace std;

namespace voltdb {

NVMAntiCacheBlock::NVMAntiCacheBlock(int16_t blockId, char* block, long size) :
    AntiCacheBlock(blockId, block, size)
{
    m_block = block;
    m_blockId = blockId;
    m_size = size;
    m_blockType = ANTICACHEDB_NVM;
}

NVMAntiCacheDB::NVMAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize) :
    AntiCacheDB(ctx, db_dir, blockSize) 
{
    m_dbType = ANTICACHEDB_NVM;
    initialize();
}


void NVMAntiCacheDB::initialize() {
    
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

    if(nvm_file == NULL)
    {
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

void NVMAntiCacheDB::shutdownDB() 
{
    fclose(nvm_file);

    #ifdef ANTICACHE_DRAM
        delete [] m_NVMBlocks;
    #endif
}

NVMAntiCacheDB::~NVMAntiCacheDB() {
    shutdownDB();
}

void NVMAntiCacheDB::writeBlock(const std::string tableName,
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


AntiCacheBlock NVMAntiCacheDB::readBlock(std::string tableName, int16_t blockId) {
    
   std::map<int16_t, std::pair<int, int32_t> >::iterator itr; 
   itr = m_blockMap.find(blockId); 
  
   if (itr == m_blockMap.end()) 
   {
     VOLT_INFO("Invalid anti-cache blockId '%d' for table '%s'", blockId, tableName.c_str());
     VOLT_ERROR("Invalid anti-cache blockId '%d' for table '%s'", blockId, tableName.c_str());
     throw UnknownBlockAccessException(tableName, blockId);
   }

   int blockIndex = itr->second.first; 
   VOLT_INFO("Reading NVM block: ID = %d, index = %d, size = %d.", blockId, blockIndex, itr->second.second);
   
   char* block_ptr = getNVMBlock(blockIndex);
   char* block = new char[itr->second.second];
   memcpy(block, block_ptr, itr->second.second); 

   NVMAntiCacheBlock anticache_block(blockId, block, itr->second.second);
   
   freeNVMBlock(blockId); 

   m_blockMap.erase(itr); 
   return (static_cast<AntiCacheBlock>(anticache_block));
}

void NVMAntiCacheDB::flushBlocks() {
   // do we need some kind of sync?
}

////////////////////////////////////////
// private:
////////////////////////////////////////

char* NVMAntiCacheDB::getNVMBlock(int index) {
    
  //char* nvm_block = new char[NVM_BLOCK_SIZE];     
  //memcpy(nvm_block, m_NVMBlocks+(index*NVM_BLOCK_SIZE), NVM_BLOCK_SIZE); 
    
  //return nvm_block; 
  return (m_NVMBlocks+(index*NVM_BLOCK_SIZE));  
}

int NVMAntiCacheDB::getFreeNVMBlockIndex()
{
  
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

void NVMAntiCacheDB::freeNVMBlock(int index)
{
    m_NVMBlockFreeList.push_back(index); 
    //m_totalBlocks--; 
}

}//namespace
