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
#include "anticache/AntiCacheStats.h"
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

AntiCacheBlock::AntiCacheBlock(uint32_t blockId) {

//    for(int i=0;i<size;i++){
//       VOLT_INFO("%x", data[i]);
//    }
        m_blockId = blockId;

  }
   
AntiCacheDB::AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize) :
    m_executorContext(ctx),
    m_dbDir(db_dir),
    m_nextBlockId(0),
    m_blockSize(blockSize),
    m_totalBlocks(0),
    m_block_merge(1)
    { 
        // MJG: TODO: HACK: Come up with a better way to make a maxsize when one isn't given
        if (maxSize == -1) {
            m_maxDBSize = 5000*blockSize;
        } else {
            m_maxDBSize = maxSize;
        }
        m_bytesEvicted = 0;
        m_blocksEvicted = 0;
        m_bytesUnevicted = 0;
        m_blocksUnevicted = 0;

        m_stats = new AntiCacheStats(NULL, this);
        if (ctx != NULL)
            m_stats->configure("Anticache Memory Stats",
                ctx->m_hostId,
                ctx->m_hostname,
                ctx->m_siteId,
                ctx->m_partitionId,
                -1);
        else  
            m_stats->configure("Anticache Memory Stats", 0, "", 0, 0, -1);
}

AntiCacheDB::~AntiCacheDB() {
    delete m_stats;
    tupleInBlock.clear();
    evictedTupleInBlock.clear();
}

AntiCacheBlock* AntiCacheDB::getLRUBlock() {
    uint32_t lru_block_id;
    AntiCacheBlock* lru_block;

    VOLT_WARN("If you are using tuple merge, this is BROKEN BROKEN BROKEN");
    if (m_block_lru.empty()) {
        VOLT_ERROR("LRU Blocklist Empty!");
        throw UnknownBlockAccessException(0);
    } else {
        lru_block_id = m_block_lru.front();
        lru_block = readBlock(lru_block_id, 1);
        //m_totalBlocks--;
        return lru_block;
    }
}

void AntiCacheDB::removeBlockLRU(uint32_t blockId) {
    //VOLT_ERROR("Removing blockId %u into LRU", blockId);
    std::deque<uint32_t>::iterator it;
    bool found = false;
           
    
    for (it = m_block_lru.begin(); it != m_block_lru.end(); ++it) {
        if (*it == blockId) {
            VOLT_INFO("Found block id %d == blockId %d", *it, blockId);
            m_block_lru.erase(it);
            found = true;
            m_totalBlocks--;
            break;
        }
    }

    if (!found) {
        VOLT_ERROR("Found block but didn't find blockId %u in LRU!", blockId);
        //throw UnknownBlockAccessException(blockId);
    }
}

void AntiCacheDB::pushBlockLRU(uint32_t blockId) {
    //VOLT_ERROR("Pushing blockId %u into LRU, max size = %ld\n", blockId, m_block_lru.max_size());
    m_block_lru.push_back(blockId);
    m_totalBlocks++;
}

uint32_t AntiCacheDB::popBlockLRU() {
    uint32_t blockId = m_block_lru.front();
    m_block_lru.pop_front();
    return blockId;
}

void AntiCacheDB::setStatsSource() {
    //m_stats = new AntiCacheStats(NULL, this);
}

void AntiCacheDB::removeSingleTupleStats(uint32_t blockId, int32_t sign) {
    m_bytesUnevicted += static_cast<int32_t>( blockSize[blockId] / tupleInBlock[blockId]) * sign;
    evictedTupleInBlock[blockId] -= sign;
}

}

