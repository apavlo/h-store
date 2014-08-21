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

#include "harness.h"
#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/CopyOnWriteIterator.h"
#include "common/DefaultTupleSerializer.h"
#include <vector>
#include <string>
#include <stdint.h>
#include <set>
#include <stdlib.h>
#include <time.h>
#include "boost/scoped_ptr.hpp"

using namespace std;
using namespace voltdb;
using stupidunit::ChTempDir;

/**
 * Tracker Allocator Tests
 */
class IndexTrackerAllocatorTest: public Test {
public:
    IndexTrackerAllocatorTest() {
        m_tuplesInserted = 0;
        m_tuplesUpdated = 0;
        m_tuplesDeleted = 0;

        m_engine = new voltdb::VoltDBEngine();
        m_engine->initialize(1,1, 0, 0, "");
        
        m_columnNames.push_back("1");
        m_columnNames.push_back("2");
        
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_primaryKeyIndexSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
        
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
        m_primaryKeyIndexSchemaColumnSizes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
        m_primaryKeyIndexSchemaColumnSizes.push_back(voltdb::VALUE_TYPE_INTEGER);
        
        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        
        m_primaryKeyIndexColumns.push_back(0);
        
    };
    
    ~IndexTrackerAllocatorTest() {
        delete m_engine;
        //delete m_table;
        voltdb::TupleSchema::freeTupleSchema(m_primaryKeyIndexSchema);
    }
    
    
    void initTable(bool allowInlineStrings, TableIndexType type, bool unique) {
        m_tableSchema = voltdb::TupleSchema::createTupleSchema(m_tableSchemaTypes,
                                                               m_tableSchemaColumnSizes,
                                                               m_tableSchemaAllowNull,
                                                               allowInlineStrings);
        
        m_primaryKeyIndexSchema = voltdb::TupleSchema::createTupleSchema(m_primaryKeyIndexSchemaTypes,
                                                                         m_primaryKeyIndexSchemaColumnSizes,
                                                                         m_primaryKeyIndexSchemaAllowNull,
                                                                         allowInlineStrings);
        voltdb::TableIndexScheme indexScheme = voltdb::TableIndexScheme("primaryKeyIndex",
                                                                         type,
                                                                         m_primaryKeyIndexColumns,
                                                                         m_primaryKeyIndexSchemaTypes,
                                                                         unique, true, m_tableSchema);

//           voltdb::TableIndexScheme indexScheme = voltdb::TableIndexScheme("primaryKeyIndex",
//                                                                        voltdb::HASH_TABLE_INDEX,
//                                                                        m_primaryKeyIndexColumns,
//                                                                        m_primaryKeyIndexSchemaTypes,
//                                                                        true, false, m_tableSchema);
        indexScheme.keySchema = m_primaryKeyIndexSchema;
        std::vector<voltdb::TableIndexScheme> indexes;
        
        m_table = dynamic_cast<voltdb::PersistentTable*>(voltdb::TableFactory::getPersistentTable
                                                         (0, m_engine->getExecutorContext(), "Foo",
                                                          m_tableSchema, &m_columnNames[0], indexScheme, indexes, 0,
                                                          false, false));
    }
    
    void cleanupTable()
    {
        //printf("delete from cleanupTable(): %p\n", m_table->getEvictedTable());
        delete m_table;
    
    }
    
    voltdb::VoltDBEngine *m_engine;
    voltdb::TupleSchema *m_tableSchema;
    voltdb::TupleSchema *m_primaryKeyIndexSchema;
    voltdb::PersistentTable *m_table;
    std::vector<std::string> m_columnNames;
    std::vector<voltdb::ValueType> m_tableSchemaTypes;
    std::vector<int32_t> m_tableSchemaColumnSizes;
    std::vector<bool> m_tableSchemaAllowNull;
    std::vector<voltdb::ValueType> m_primaryKeyIndexSchemaTypes;
    std::vector<int32_t> m_primaryKeyIndexSchemaColumnSizes;
    std::vector<bool> m_primaryKeyIndexSchemaAllowNull;
    std::vector<int> m_primaryKeyIndexColumns;
    
    int32_t m_tuplesInserted;
    int32_t m_tuplesUpdated;
    int32_t m_tuplesDeleted;
    
};

TEST_F(IndexTrackerAllocatorTest, BinaryTreeUniqueIndexMemoryEstimate)
{
    initTable(true, voltdb::BALANCED_TREE_INDEX, true); 
    
    TableTuple tuple = m_table->tempTuple();
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
        
    for(int i = 0; i < 1024; i++) // insert 1024 tuples
    {
        tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
        tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
        m_table->insertTuple(tuple);
    }

    // get the tuple that was just inserted
    tuple = m_table->lookupTuple(tuple); 
    
    VOLT_INFO("%s memory estimate: %ld\n", m_table->index("primaryKeyIndex")->getTypeName().c_str(), m_table->index("primaryKeyIndex")->getMemoryEstimate());

    ASSERT_NE(m_table->index("primaryKeyIndex")->getMemoryEstimate(), 0);
    
    cleanupTable(); 
}

TEST_F(IndexTrackerAllocatorTest, BinaryTreeMultiMapIndexMemoryEstimate)
{
    initTable(true, voltdb::BALANCED_TREE_INDEX, false); 
    
    TableTuple tuple = m_table->tempTuple();
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
        
    for(int i = 0; i < 1024; i++) // insert 1024 tuples
    {
        tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
        tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
        m_table->insertTuple(tuple);
    }

    // get the tuple that was just inserted
    tuple = m_table->lookupTuple(tuple); 
    
    VOLT_INFO("%s memory estimate: %ld\n", m_table->index("primaryKeyIndex")->getTypeName().c_str(), m_table->index("primaryKeyIndex")->getMemoryEstimate());

    ASSERT_NE(m_table->index("primaryKeyIndex")->getMemoryEstimate(), 0);
    
    cleanupTable(); 
}

TEST_F(IndexTrackerAllocatorTest, HashTableMultiMapIndexMemoryEstimate)
{
    initTable(true, voltdb::HASH_TABLE_INDEX, false); 
    
    TableTuple tuple = m_table->tempTuple();
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
        
    for(int i = 0; i < 1024; i++) // insert 1024 tuples
    {
        tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
        tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
        m_table->insertTuple(tuple);
    }

    // get the tuple that was just inserted
    tuple = m_table->lookupTuple(tuple); 
    
    VOLT_INFO("%s memory estimate: %ld\n", m_table->index("primaryKeyIndex")->getTypeName().c_str(), m_table->index("primaryKeyIndex")->getMemoryEstimate());

    ASSERT_NE(m_table->index("primaryKeyIndex")->getMemoryEstimate(), 0);
    
    cleanupTable(); 
}

TEST_F(IndexTrackerAllocatorTest, HashTableUniqueMapIndexMemoryEstimate)
{
    initTable(true, voltdb::HASH_TABLE_INDEX, true); 
    
    TableTuple tuple = m_table->tempTuple();
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
        
    for(int i = 0; i < 1024; i++) // insert 1024 tuples
    {
        tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
        tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
        m_table->insertTuple(tuple);
    }

    // get the tuple that was just inserted
    tuple = m_table->lookupTuple(tuple); 
    
    VOLT_INFO("%s memory estimate: %ld\n", m_table->index("primaryKeyIndex")->getTypeName().c_str(), m_table->index("primaryKeyIndex")->getMemoryEstimate());

    ASSERT_NE(m_table->index("primaryKeyIndex")->getMemoryEstimate(), 0);
    
    cleanupTable(); 
}

TEST_F(IndexTrackerAllocatorTest, ArrayUniqueIndexMemoryEstimate)
{
    initTable(true, voltdb::ARRAY_INDEX, true); 
    
    TableTuple tuple = m_table->tempTuple();
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
    
    tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
    tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
    m_table->insertTuple(tuple);
        
    for(int i = 0; i < 1024; i++) // insert 1024 tuples
    {
        tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
        tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
        m_table->insertTuple(tuple);
    }

    // get the tuple that was just inserted
    tuple = m_table->lookupTuple(tuple); 
    
    VOLT_INFO("%s memory estimate: %ld\n", m_table->index("primaryKeyIndex")->getTypeName().c_str(), m_table->index("primaryKeyIndex")->getMemoryEstimate());

    ASSERT_NE(m_table->index("primaryKeyIndex")->getMemoryEstimate(), 0);
    
    cleanupTable(); 
}

/*
TEST_F(AntiCacheEvictionManagerTest, TestEvictionOrder)
{
    int num_tuples = 100; 

    initTable(true); 
      
    TableTuple tuple = m_table->tempTuple();
    int tuple_size = m_tableSchema->tupleLength() + TUPLE_HEADER_SIZE;

        
    for(int i = 0; i < num_tuples; i++) // insert 10 tuples
    {
        tuple.setNValue(0, ValueFactory::getIntegerValue(m_tuplesInserted++));
        tuple.setNValue(1, ValueFactory::getIntegerValue(rand()));
        m_table->insertTuple(tuple);
    }
    
    EvictionIterator itr(m_table); 

    itr.reserve(20 * tuple_size);

    ASSERT_TRUE(itr.hasNext());

    uint32_t oldTimeStamp = 0;

    while(itr.hasNext()) {
        itr.next(tuple); 
        
        uint32_t newTimeStamp = tuple.getTimeStamp();
        ASSERT_LE(oldTimeStamp, newTimeStamp);
        oldTimeStamp = newTimeStamp;
    }

    cleanupTable();
}*/

int main() {
    return TestSuite::globalInstance()->runAll();
}

