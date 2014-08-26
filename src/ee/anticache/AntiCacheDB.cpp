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

AntiCacheBlock::AntiCacheBlock(int16_t blockId) {

//	for(int i=0;i<size;i++){
//	   VOLT_INFO("%x", data[i]);
//	}
	    m_blockId = blockId;

  }
   
AntiCacheDB::AntiCacheDB(ExecutorContext *ctx, std::string db_dir, long blockSize) :
    m_executorContext(ctx),
    m_dbDir(db_dir),
    m_blockSize(blockSize),
    m_nextBlockId(0),
    m_totalBlocks(0) {
        
}

AntiCacheDB::~AntiCacheDB() {
}

}

