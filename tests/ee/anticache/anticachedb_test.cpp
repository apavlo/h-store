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

#include <string>
#include "harness.h"
#include "anticache/AntiCacheDB.h"
#include "anticache/BerkeleyAntiCacheDB.h"
#include "anticache/NVMAntiCacheDB.h"

using namespace std;
using namespace voltdb;
using stupidunit::ChTempDir;

#define BLOCK_SIZE 524288

/**
 * AntiCacheDB Tests
 */
class AntiCacheDBTest : public Test {
public:
    AntiCacheDBTest() {
        
    };
};


TEST_F(AntiCacheDBTest, BerkeleyNextBlockId) {
    ChTempDir tempdir;
    AntiCacheDB* anticache = new BerkeleyAntiCacheDB(NULL, ".", BLOCK_SIZE);
    
    uint16_t lastBlockId;
    for (int i = 0; i < 1000; i++) {
        uint16_t blockId = anticache->nextBlockId();
        if (i > 0) ASSERT_NE(lastBlockId, blockId);
        lastBlockId = blockId;
    } // FOR
    delete anticache;
}

TEST_F(AntiCacheDBTest, NVMNextBlockId) {
    ChTempDir tempdir;
    AntiCacheDB* anticache = new NVMAntiCacheDB(NULL, ".", BLOCK_SIZE);
    
    uint16_t lastBlockId;
    for (int i = 0; i < 1000; i++) {
        uint16_t blockId = anticache->nextBlockId();
        if (i > 0) ASSERT_NE(lastBlockId, blockId);
        lastBlockId = blockId;
    } // FOR
    delete anticache;
}


// This is based off of the code from Yi Wang
// http://cxwangyi.wordpress.com/2010/10/10/how-to-use-berkeley-db/
TEST_F(AntiCacheDBTest, BerkeleyWriteBlock) {
    // This will create a tempdir that will automatically be cleaned up
    ChTempDir tempdir;
    AntiCacheDB* anticache = new BerkeleyAntiCacheDB(NULL, ".", BLOCK_SIZE);

    string tableName("FAKE");
    string payload("Squirrels and Girls!");
    uint16_t blockId = anticache->nextBlockId();

    try {
        anticache->writeBlock(tableName,
                             blockId,
                             1,
                             const_cast<char*>(payload.data()),
                             static_cast<int>(payload.size())+1);
    } catch (...) {
        delete anticache;
        ASSERT_TRUE(false);
    }
    delete anticache;
}

TEST_F(AntiCacheDBTest, NVMWriteBlock) {
    // This will create a tempdir that will automatically be cleaned up
    ChTempDir tempdir;
    AntiCacheDB* anticache = new NVMAntiCacheDB(NULL, ".", BLOCK_SIZE);

    string tableName("FAKE");
    string payload("Squirrels and Girls!");
    uint16_t blockId = anticache->nextBlockId();

    try {
        anticache->writeBlock(tableName,
                             blockId,
                             1,
                             const_cast<char*>(payload.data()),
                             static_cast<int>(payload.size())+1);
    } catch (...) {
        delete anticache;
        ASSERT_TRUE(false);
    }
    delete anticache;
}


TEST_F(AntiCacheDBTest, BerkeleyReadBlock) {
    // This will create a tempdir that will automatically be cleaned up
    ChTempDir tempdir;

    AntiCacheDB* anticache = new BerkeleyAntiCacheDB(NULL, ".", BLOCK_SIZE);

    string tableName("FAKE");
    string payload("Test Read");
    uint16_t blockId = anticache->nextBlockId();
	anticache->writeBlock(tableName,
						 blockId,
						 1,
						 const_cast<char*>(payload.data()),
						 static_cast<int>(payload.size())+1);

	AntiCacheBlock* block = anticache->readBlock(blockId);

	//ASSERT_EQ(block->getTableName(), tableName);
	ASSERT_EQ(block->getBlockId(), blockId);
	ASSERT_EQ(0, payload.compare(block->getData()));
	long expected_size = payload.size()+1;
	ASSERT_EQ(block->getSize(), expected_size);
    delete block;
    delete anticache;
}

// This test needs a functioning executorContext in order to obtain a partitionID
// to write out the file. Not havign a valid one causes a seg fault. The solution i
// s probably to not require the use of a partitionID for the filename

TEST_F(AntiCacheDBTest, NVMReadBlock) {
    // This will create a tempdir that will automatically be cleaned up
    ChTempDir tempdir;

    AntiCacheDB* anticache = new NVMAntiCacheDB(NULL, ".", BLOCK_SIZE);

    string tableName("FAKE");
    string payload("Test Read");
    uint16_t blockId = anticache->nextBlockId();
	anticache->writeBlock(tableName,
						 blockId,
						 1,
						 const_cast<char*>(payload.data()),
						 static_cast<int>(payload.size())+1);

	AntiCacheBlock* block = anticache->readBlock(blockId);

	//ASSERT_EQ(block->getTableName(), tableName);
	ASSERT_EQ(block->getBlockId(), blockId);
	ASSERT_EQ(0, payload.compare(block->getData()));
	long expected_size = payload.size()+1;
	ASSERT_EQ(block->getSize(), expected_size);
    delete block;
    delete anticache;
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
