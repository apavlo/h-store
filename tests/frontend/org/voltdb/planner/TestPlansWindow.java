/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.planner;

import junit.framework.TestCase;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;

// test window behavior
public class TestPlansWindow extends TestCase {

    private PlannerTestAideDeCamp aide;


    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestPlansWindow.class.getResource("testplans-window-ddl.sql"), "testplanswindow");

        // Set all tables to non-replicated.
        Cluster cluster = aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            t.setIsreplicated(false);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }
    
    public void testSelectFromWindow() {
        AbstractPlanNode pn = null;
        
        // traditional table
        pn = compile("SELECT * FROM TABLEA WHERE cash = 100", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());

        pn = compile("SELECT TABLEA.phone_number, TABLEB.cash FROM TABLEA, TABLEB WHERE TABLEA.phone_number = TABLEB.phone_number", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());

        // window
        pn = compile("SELECT * FROM W_ROWS WHERE cash = 100", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());

        pn = compile("SELECT * FROM W_TIME WHERE cash = 100", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());
        
        pn = compile("SELECT W_ROWS.phone_number, W_TIME.cash FROM W_TIME, W_ROWS WHERE W_TIME.phone_number = W_ROWS.phone_number", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }
    
    public void testInsertIntoWindow() {
        AbstractPlanNode pn = null;
        pn = compile("INSERT INTO TABLEA (phone_number, cash) VALUES (1, 100)",0);
        if (pn != null)
            System.out.println(pn.toJSONString());

        pn = compile("INSERT INTO W_ROWS (phone_number, cash) VALUES (1, 100)",0);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }
    
    private AbstractPlanNode compile(String sql, int paramCount) {
        AbstractPlanNode pn = null;
        try {
            pn =  aide.compile(sql, paramCount);
        }
        catch (NullPointerException ex) {
            // aide may throw NPE if no plangraph was created
            ex.printStackTrace();
            fail();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        return pn;
    }
}
