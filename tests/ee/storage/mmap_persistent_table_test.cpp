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
#include "storage/mmap_persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include <vector>
#include <string>
#include <time.h>
#include <stdint.h>

using namespace voltdb;

string genRandomString(const int len);

class MMAP_PersistentTableTest : public Test {
public:
  MMAP_PersistentTableTest() {
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

  ~MMAP_PersistentTableTest() {
    voltdb::TupleSchema::freeTupleSchema(m_primaryKeyIndexSchema);
    delete m_table;
    delete m_engine;
  }

  void initTable(bool allowInlineStrings) {
    struct timeval start, end;
    long  seconds, useconds;
    double mtime;

    // Randomize seed for rand
    srand(static_cast<unsigned int>(time(NULL)));

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

    string tableName;

    gettimeofday(&start, NULL);

    tableName = genRandomString(32);

    std::string dbDir = "/tmp";
    

#ifdef STORAGE_MMAP
    /** Enable MMAP_PersistentTable **/
    ExecutorContext* ec = m_engine->getExecutorContext();
    ec->enableMMAP(dbDir, true, 1024);
#endif
    
    // MEASURE TIME TAKEN FOR CREATION
    m_table = dynamic_cast<voltdb::PersistentTable*>(voltdb::TableFactory::getPersistentTable
    (0, m_engine->getExecutorContext(), tableName,
     m_tableSchema, &m_columnNames[0], indexScheme, indexes, 0,
     false, false));
    
    gettimeofday(&end, NULL);

    VOLT_INFO("created table : %s pointer : %p", tableName.c_str(), (const void*)m_table);
    
    seconds  = end.tv_sec  - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
    mtime = (double)((seconds) * 1000 + useconds/1000);

    VOLT_INFO("total time for table allocation :: %f milliseconds", mtime);

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

// Generates alphanumeric strings
string genRandomString(const int len) {
  static const char alphanum[] =
  "0123456789"
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz";
  string randStr;

  for (int i = 0; i < len; ++i) {
    randStr += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return randStr;
}

TEST_F(MMAP_PersistentTableTest, TableAllocationTest) {
  initTable(true);
  tableutil::addRandomTuples(m_table, 10);
  ASSERT_EQ( m_table->activeTupleCount(), 10);
  //m_engine->undoUndoToken(INT64_MIN + 1);
  //ASSERT_EQ( m_table->activeTupleCount(), 0);
}

int main() {
  return TestSuite::globalInstance()->runAll();
}
