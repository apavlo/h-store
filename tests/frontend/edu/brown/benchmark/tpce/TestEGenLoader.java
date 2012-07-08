/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.tpce;

import java.io.File;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestEGenLoader extends BaseTestCase {

    // HACK
    private static final String EGENLOADER_HOME = System.getenv("TPCE_LOADER_FILES");
    protected static EGenLoader loader;
    protected static Catalog catalog;
    protected static Database catalog_db;

    protected static final int NUM_CUSTOMERS = 1000;
    protected static final int SCALE_FACTOR = 1000;
    protected static final int INITIAL_DAYS = 1;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCE, true, false);
        if (loader == null) {
            loader = new EGenLoader(EGENLOADER_HOME, NUM_CUSTOMERS, SCALE_FACTOR, INITIAL_DAYS);
        }
        assertTrue(loader.loader_bin.exists());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        loader.clearTables();
    }

    /**
     * testGenerateFixedTables
     */
    public void testGenerateFixedTables() throws Exception {
        loader.generateFixedTables();
        for (String table_name : TPCEConstants.FIXED_TABLES) {
            File table_file = loader.getTablePath(table_name);
            // System.out.println("Looking for " + table_file);
            assertTrue(table_file.exists());
        } // FOR
    }

    /**
     * testGetTable
     */
    public void testGetTable() throws Exception {
        loader.generateFixedTables();
        Table catalog_tbl = this.getTable(TPCEConstants.TABLENAME_COMMISSION_RATE);
        assertNotNull(catalog_tbl);

        Iterable<Object[]> it = loader.getTable(catalog_tbl);
        assertNotNull(it);
        for (Object tuple[] : it) {
            assert (tuple != null);
            assertEquals(catalog_tbl.getColumns().size(), tuple.length);

            String add = "";
            System.out.print("[");
            for (int i = 0; i < tuple.length; i++) {
                assert (tuple[i] != null) : "Tuple at " + i + " is null";
                System.out.print(add + tuple[i] + " (" + tuple[i].getClass().getSimpleName() + ")");
                add = ", ";
            }
            System.out.println("]");
            break;
        } // FOR
    }

    /**
     * testGenerateScalingTables
     */
    public void testGenerateScalingTables() throws Exception {
        loader.generateScalingTables(0);
        for (String table_name : TPCEConstants.SCALING_TABLES) {
            File table_file = loader.getTablePath(table_name);
            // System.out.println("Looking for " + table_file);
            assertTrue(table_file.exists());
        } // FOR
    }

    /**
     * testGenerateGrowingTables
     */
    public void testGenerateGrowingTables() throws Exception {
        loader.generateGrowingTables(0);
        for (String table_name : TPCEConstants.GROWING_TABLES) {
            File table_file = loader.getTablePath(table_name);
            // System.out.println("Looking for " + table_file);
            assertTrue(table_file.exists());
        } // FOR
    }
}