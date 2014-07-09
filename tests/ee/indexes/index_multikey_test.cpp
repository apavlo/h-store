/* Copyright (C) 2014 by H-Store Project
 * Brown University
 * Carnegie Mellon University
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
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "execution/VoltDBEngine.h"


using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_TUPLES 1000

class IndexMultikeyTest : public Test {
public:
    IndexMultikeyTest() : table(NULL) {}
    ~IndexMultikeyTest() {
        delete table;
        delete[] m_exceptionBuffer;
        delete m_engine;
    }

    
    void init(TableIndexScheme index) {
        CatalogId database_id = 1000;
        vector<boost::shared_ptr<const TableColumn> > columns;

        // TODO: Change the column types to be mixed (instead of all BIGINTs)
        string *columnNames = new string[NUM_OF_COLUMNS];
        vector<ValueType> columnTypes(NUM_OF_COLUMNS, VALUE_TYPE_BIGINT);
        vector<int32_t> columnLengths(NUM_OF_COLUMNS, NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
        vector<bool> columnAllowNull(NUM_OF_COLUMNS, false);
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
            char buffer[32];
            snprintf(buffer, 32, "column%02d", ctr);
            columnNames[ctr] = buffer;
        }
        TupleSchema* schema = TupleSchema::createTupleSchema(columnTypes,
                                           columnLengths,
                                           columnAllowNull,
                                           true);
        index.tupleSchema = schema;
        
        // TODO: This will create the primary key index for the table.
        //       Note that we also add the index that was passed into this function
        vector<int> pkey_column_indices;
        vector<ValueType> pkey_column_types;
        pkey_column_indices.push_back(0);
        pkey_column_indices.push_back(1);
        pkey_column_types.push_back(VALUE_TYPE_BIGINT);
        pkey_column_types.push_back(VALUE_TYPE_BIGINT);
        TableIndexScheme pkey("idx_pkey",
                              BALANCED_TREE_INDEX,
                              pkey_column_indices,
                              pkey_column_types,
                              true, true, schema);

        // This initializes a VoltDBEngine that will only contain 
        // a single table and the index that we just created
        vector<TableIndexScheme> indexes;
        indexes.push_back(index);
        m_engine = new VoltDBEngine();
        m_exceptionBuffer = new char[4096];
        m_engine->setBuffers( NULL, 0, NULL, 0, m_exceptionBuffer, 4096);
        m_engine->initialize(0, 0, 0, 0, "");
        table =
            dynamic_cast<PersistentTable*>
          (TableFactory::getPersistentTable(database_id, m_engine->getExecutorContext(),
                                            "test_table", schema,
                                            columnNames, pkey, indexes, -1, false, false));
        delete[] columnNames;

        
        // TODO: Now populate the table with a bunch of tuples.
        //       This will automatically populate the index too.
        for (int64_t i = 0; i < NUM_OF_TUPLES; ++i) {
            // Populate the tuple with random data
            // FIXME TableTuple &tuple = table->tempTuple();
            for (int32_t col = 0; col < NUM_OF_COLUMNS; ++col) {
                // FIXME tuple.setNValue(0, ValueFactory::getBigIntValue(i));
            }
            // FIXME assert(true == table->insertTuple(tuple));
        }
    }
   

protected:
    PersistentTable* table;
    char* m_exceptionBuffer;
    VoltDBEngine* m_engine;
};

TEST_F(IndexMultikeyTest, IntUnique) {
    // Create the index that we want to test against
    vector<int> iu_column_indices;
    vector<ValueType> iu_column_types;
    iu_column_indices.push_back(3);
    iu_column_indices.push_back(4);
    iu_column_types.push_back(VALUE_TYPE_BIGINT);
    iu_column_types.push_back(VALUE_TYPE_BIGINT);
    TableIndexScheme indexScheme("iu", BALANCED_TREE_INDEX,
                                 iu_column_indices,
                                 iu_column_types,
                                 true, true, NULL);
    this->init(indexScheme);
    
    // TODO: Now grab the index that we just created and do a bunch of tests with it.
    TableIndex* index = table->index("iu");
    EXPECT_EQ(true, index != NULL);
}




int main()
{
    return TestSuite::globalInstance()->runAll();
}
