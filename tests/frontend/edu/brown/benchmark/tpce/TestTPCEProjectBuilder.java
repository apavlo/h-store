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

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.types.ConstraintType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ProjectType;

public class TestTPCEProjectBuilder extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCE, true, false);
    }

    /**
     * testVerifyCatalog
     */
    // Disabled May 03, 2010 because the compiler takes too long to compile the
    // TPC-E procedures
    // public void testVerifyCatalog() throws Exception {
    // assertNotNull(catalog_db);
    // System.err.println("Procedures: " +
    // CatalogUtil.debug(catalog_db.getProcedures()));
    // for (Class<?> proc_class : TPCEProjectBuilder.PROCEDURES) {
    // String proc_name = proc_class.getSimpleName();
    // assertNotNull("Missing procedure '" + proc_name + "'",
    // catalog_db.getProcedures().get(proc_name));
    // } // FOR
    // }

    /**
     * testCreateSchemaCatalog
     */
    public void testCreateSchemaCatalog() throws Exception {
        Catalog s_catalog = new TPCEProjectBuilder().getSchemaCatalog(false);
        assertNotNull(s_catalog);
        Database s_catalog_db = CatalogUtil.getDatabase(s_catalog);
        assertNotNull(catalog_db);

        // ADDRESS should point to ZIP_CODE
        Table address = s_catalog_db.getTables().get(TPCEConstants.TABLENAME_ADDRESS);
        assertNotNull(address);
        Table zipcode = s_catalog_db.getTables().get(TPCEConstants.TABLENAME_ZIP_CODE);
        assertNotNull(zipcode);

        for (Constraint catalog_const : address.getConstraints()) {
            if (catalog_const.getType() == ConstraintType.FOREIGN_KEY.getValue()) {
                assertEquals(zipcode, catalog_const.getForeignkeytable());
                assertEquals(1, catalog_const.getForeignkeycols().size());
                break;
            }
        } // FOR
    }
}
