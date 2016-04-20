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

#ifndef ALLOCATORNVMHSTOREANTICACHE_H
#define ALLOCATORNVMHSTOREANTICACHE_H

#include "common/types.h"
#include "common/debuglog.h"
#include "anticache/AntiCacheDB.h"

using namespace std;

namespace voltdb {

class ExecutorContext;
class AntiCacheDB;


class AllocatorNVMAntiCacheDB : public AntiCacheDB {
    public:
        AllocatorNVMAntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize, long maxSize);
        ~AllocatorNVMAntiCacheDB();

        //void initializeDB();

        inline uint32_t nextBlockId() {
            //return (int16_t)getFreeNVMBlockIndex(); 
            //return m_monoBlockID;
            return 0;
           
        }

        AntiCacheBlock* readBlock(uint32_t blockId, bool isMigrate);

        void shutdownDB();

        void flushBlocks();

        void writeBlock(const std::string tableName,
                        uint32_t blockId,
                        const int tupleCount,
                        const char* data,
                        const long size,
                        const int evictedTupleCount);

        bool validateBlock(uint32_t blockId);

};

}
#endif
