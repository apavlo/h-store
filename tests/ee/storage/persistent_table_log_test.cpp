/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
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
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include <vector>
#include <string>
#include <set>
#include <stdint.h>

using namespace voltdb;

class PersistentTableLogTest : public Test {
public:
    PersistentTableLogTest() {
        m_engine = new voltdb::VoltDBEngine();
        m_engine->initialize(1,1, 0, 0, "");

        m_columnNames.push_back("1");
        m_columnNames.push_back("2");
        m_columnNames.push_back("3");
        m_columnNames.push_back("4");
        m_columnNames.push_back("5");
        m_columnNames.push_back("6");
        m_columnNames.push_back("7");
        m_columnNames.push_back("8");
        m_columnNames.push_back("9");
        m_columnNames.push_back("10");

        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_primaryKeyIndexSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_TINYINT);
        m_primaryKeyIndexSchemaTypes.push_back(voltdb::VALUE_TYPE_TINYINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_INTEGER);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_SMALLINT);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_DOUBLE);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_primaryKeyIndexSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_primaryKeyIndexSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);
        m_tableSchemaTypes.push_back(voltdb::VALUE_TYPE_VARCHAR);

        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_primaryKeyIndexSchemaColumnSizes.push_back(voltdb::VALUE_TYPE_BIGINT);
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_TINYINT));
        m_primaryKeyIndexSchemaColumnSizes.push_back(voltdb::VALUE_TYPE_TINYINT);
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_SMALLINT));
        m_tableSchemaColumnSizes.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_DOUBLE));
        m_tableSchemaColumnSizes.push_back(300);
        m_primaryKeyIndexSchemaColumnSizes.push_back(300);
        m_tableSchemaColumnSizes.push_back(16);
        m_primaryKeyIndexSchemaColumnSizes.push_back(16);
        m_tableSchemaColumnSizes.push_back(500);
        m_tableSchemaColumnSizes.push_back(32);

        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(false);
        m_primaryKeyIndexSchemaAllowNull.push_back(false);
        m_tableSchemaAllowNull.push_back(true);
        m_tableSchemaAllowNull.push_back(true);

        m_primaryKeyIndexColumns.push_back(0);
        m_primaryKeyIndexColumns.push_back(1);
        m_primaryKeyIndexColumns.push_back(6);
        m_primaryKeyIndexColumns.push_back(7);

        m_engine->setUndoToken(INT64_MIN + 1);
    }

    ~PersistentTableLogTest() {
        delete m_engine;
        delete m_table;
        voltdb::TupleSchema::freeTupleSchema(m_primaryKeyIndexSchema);
    }

    void initTable(bool allowInlineStrings) {
        m_tableSchema = voltdb::TupleSchema::createTupleSchema(m_tableSchemaTypes,
                                                               m_tableSchemaColumnSizes,
                                                               m_tableSchemaAllowNull,
                                                               allowInlineStrings);

        m_primaryKeyIndexSchema = voltdb::TupleSchema::createTupleSchema(m_primaryKeyIndexSchemaTypes,
                                                                         m_primaryKeyIndexSchemaColumnSizes,
                                                                         m_primaryKeyIndexSchemaAllowNull,
                                                                         allowInlineStrings);

        voltdb::TableIndexScheme indexScheme = voltdb::TableIndexScheme("primaryKeyIndex",
                                                                        voltdb::BALANCED_TREE_INDEX,
                                                                        m_primaryKeyIndexColumns,
                                                                        m_primaryKeyIndexSchemaTypes,
                                                                        true, false, m_tableSchema);
        indexScheme.keySchema = m_primaryKeyIndexSchema;

        std::vector<voltdb::TableIndexScheme> indexes;

        m_table = dynamic_cast<voltdb::PersistentTable*>(voltdb::TableFactory::getPersistentTable
                                                         (0, m_engine->getExecutorContext(), "Foo",
                                                          m_tableSchema, &m_columnNames[0], indexScheme, indexes, 0,
                                                          false, false));
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
};
/*
TEST_F(PersistentTableLogTest, InsertDeleteThenUndoOneTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 1000);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);

    ASSERT_FALSE( m_table->lookupTuple(tuple).isNullTuple());

    voltdb::TableTuple tupleBackup(m_tableSchema);
    tupleBackup.move(new char[tupleBackup.tupleLength()]);
    tupleBackup.copyForPersistentInsert(tuple);

    m_engine->setUndoToken(INT64_MIN + 2);
    m_table->deleteTuple(tuple, true);

    ASSERT_TRUE( m_table->lookupTuple(tupleBackup).isNullTuple());

    m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_FALSE(m_table->lookupTuple(tuple).isNullTuple());

    for (int ii = 0; ii < m_tableSchema->getUninlinedStringColumnCount(); ii++) {
        tupleBackup.getNValue(m_tableSchema->getUninlinedStringColumnInfoIndex(ii)).free();
    }
    delete [] tupleBackup.address();
}*/

TEST_F(PersistentTableLogTest, TupleIds) {
    initTable(true);
    voltdb::TableTuple tuple(m_tableSchema);

    // Insert a bunch of tuples and make sure that they all have valid tuple ids
    voltdb::TableIterator iterator = m_table->tableIterator();
    while (iterator.next(tuple)) {
        //printf("%s\n", tuple->debug(this->table).c_str());
        // Make sure it is not deleted
        EXPECT_EQ(true, tuple.isActive());
    }

    // Make sure that if we insert one tuple, we only get one tuple
    int num_tuples = 10;
    tableutil::addRandomTuples(m_table, num_tuples);
//     printf("# of Tuples -> %ld\n", m_table->usedTupleCount());
    ASSERT_EQ(num_tuples, m_table->usedTupleCount());

    // Then check to make sure that it has the same value and type
    iterator = m_table->tableIterator();
    std::set<uint32_t> tupleIds;
    while (iterator.next(tuple)) {
        uint32_t id = m_table->getTupleID(tuple.address());
//         printf("[%d] -- %s\n\n", id, tuple.debug(m_table->name()).c_str());
        ASSERT_EQ(0, tupleIds.count(id));
        tupleIds.insert(id);
    } // WHILE
    ASSERT_EQ(num_tuples, tupleIds.size());
}

TEST_F(PersistentTableLogTest, InsertUpdateThenUndoOneTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 1);
    voltdb::TableTuple tuple(m_tableSchema);

    tableutil::getRandomTuple(m_table, tuple);
    //std::cout << "Retrieved random tuple " << std::endl << tuple.debugNoHeader() << std::endl;

    ASSERT_FALSE( m_table->lookupTuple(tuple).isNullTuple());

    /*
     * A backup copy of what the tuple looked like before updates
     */
    voltdb::TableTuple tupleBackup(m_tableSchema);
    tupleBackup.move(new char[tupleBackup.tupleLength()]);
    tupleBackup.copyForPersistentInsert(tuple);

    /*
     * A copy of the tuple to modify and use as a source tuple when updating the new tuple.
     */
    voltdb::TableTuple tupleCopy(m_tableSchema);
    tupleCopy.move(new char[tupleCopy.tupleLength()]);
    tupleCopy.copyForPersistentInsert(tuple);

    m_engine->setUndoToken(INT64_MIN + 2);

    // this next line is a testing hack until engine data is
    // de-duplicated with executorcontext data
    m_engine->getExecutorContext();

    /*
     * Update a few columns
     */
    tupleCopy.setNValue(0, ValueFactory::getBigIntValue(5));
    NValue newStringValue = ValueFactory::getStringValue("foo");
    tupleCopy.setNValue(7, newStringValue);

    //m_table->updateTuple(tupleCopy, tuple, true);

    //ASSERT_TRUE( m_table->lookupTuple(tupleBackup).isNullTuple());
    //ASSERT_FALSE( m_table->lookupTuple(tupleCopy).isNullTuple());
    //m_engine->undoUndoToken(INT64_MIN + 2);

    ASSERT_FALSE(m_table->lookupTuple(tuple).isNullTuple());
    ASSERT_TRUE( m_table->lookupTuple(tupleCopy).isNullTuple());
    tupleBackup.freeObjectColumns();
    tupleCopy.freeObjectColumns();
    delete [] tupleBackup.address();
    delete [] tupleCopy.address();
    newStringValue.free();
}

TEST_F(PersistentTableLogTest, InsertThenUndoInsertsOneTest) {
    initTable(true);
    tableutil::addRandomTuples(m_table, 10);
    ASSERT_EQ( m_table->activeTupleCount(), 10);
    m_engine->undoUndoToken(INT64_MIN + 1);
    ASSERT_EQ( m_table->activeTupleCount(), 0);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
