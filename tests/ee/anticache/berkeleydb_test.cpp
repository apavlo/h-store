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

#include <stdio.h>
#include <string>
#include <iostream>
#include <iomanip>
#include "harness.h"

#include <db_cxx.h>

using namespace std;
using stupidunit::ChTempDir;

const char* kDatabaseName = "access.db";

/**
 * This is just a simple test to prove that we can successfully
 * link in BerkeleyDB and include all of the headers that we need
 */
class BerkeleyDBTest : public Test {
public:
    BerkeleyDBTest() {};
};

// This is based off of the code from Yi Wang
// http://cxwangyi.wordpress.com/2010/10/10/how-to-use-berkeley-db/
TEST_F(BerkeleyDBTest, BasicTest) {
    // This will create a tempdir that will automatically be cleaned up
    ChTempDir tempdir;
    
    string fruit("fruit");
    string apple("apple");
    string orange("orange");

    try {
        DbEnv env(0);
        env.set_error_stream(&cerr);
        env.open(".", DB_CREATE | DB_INIT_MPOOL, 0);

        Db* pdb = new Db(&env, 0);
        
        // If the database does not exist, create it.  If it exists, clear
        // its content after openning.
        pdb->open(NULL, kDatabaseName, NULL, DB_BTREE, DB_CREATE | DB_TRUNCATE, 0);

        Dbt key(const_cast<char*>(fruit.data()), static_cast<int>(fruit.size()));
        Dbt value(const_cast<char*>(apple.data()), static_cast<int>(apple.size())+1);
        pdb->put(NULL, &key, &value, 0);

        Dbt value_orange(const_cast<char*>(orange.data()), static_cast<int>(orange.size())+1);
        pdb->put(NULL, &key, &value_orange, 0);

        // You need to set ulen and flags=DB_DBT_USERMEM to prevent Dbt
        // from allocate its own memory but use the memory provided by you.
        char buffer[1024];
        Dbt data;
        data.set_data(buffer);
        data.set_ulen(1024);
        data.set_flags(DB_DBT_USERMEM);
        if (pdb->get(NULL, &key, &data, 0) == DB_NOTFOUND) {
            cerr << "Not found" << endl;
        }
//         ASSERT_EQ(orange.data(), buffer);
        
        if (pdb != NULL) {
            pdb->close(0);
            delete pdb;
            // You have to close and delete an exisiting handle, then create
            // a new one before you can use it to remove a database (file).
            pdb = new Db(NULL, 0);
            pdb->remove(kDatabaseName, NULL, 0);
            delete pdb;
        }
        env.close(0);
    } catch (...) {
        ASSERT_TRUE(false);
    }
}
int main() {
    return TestSuite::globalInstance()->runAll();
}
