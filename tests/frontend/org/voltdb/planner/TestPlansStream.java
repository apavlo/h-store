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
import org.junit.Ignore;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.TestDDLCompiler;
import org.voltdb.plannodes.AbstractPlanNode;

public class TestPlansStream extends TestCase {

    private PlannerTestAideDeCamp aide;


    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestPlansStream.class.getResource("testplans-stream-ddl.sql"), "testplansstream");

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
    
    public void testInsertSelect() {
        AbstractPlanNode pn = null;
        pn = compile("INSERT INTO votes_by_phone_number (phone_number, num_votes) SELECT B_ID, NUMROWS + 1 FROM TABLEB ", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }

    public void testInsert() {
        AbstractPlanNode pn = null;
        pn = compile("INSERT INTO votes_by_phone_number (phone_number, num_votes) VALUES (1,2)", 0, false);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }

    public void testUpsert() {
        AbstractPlanNode pn = null;
        pn = compile("INSERT INTO votes_by_phone_number (phone_number, num_votes) VALUES (1,2)", 0, true);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }
    
    public void testSelectFromStream() {
        AbstractPlanNode pn = null;
        //pn = compile("SELECT t_id from ticker", 0);
        pn = compile("SELECT TOP 1 * FROM TABLEA", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }
 
    @Ignore   
    public void testDeleteTopFromStream() {
        AbstractPlanNode pn = null;
        //pn = compile("SELECT t_id from ticker", 0);
        //pn = compile("DELETE FROM TABLEA WHERE A_ID = (SELECT MAX(A_ID) FROM TABLEA)", 0);
        pn = compile("DELETE FROM TABLEA", 0);
        if (pn != null)
            System.out.println(pn.toJSONString());
    }
    
    private AbstractPlanNode compile(String sql, int paramCount) {
        return compile(sql, paramCount, false);
    }

    private AbstractPlanNode compile(String sql, int paramCount, boolean Upsertable) {
        AbstractPlanNode pn = null;
        try {
            pn =  aide.compile(sql, paramCount, Upsertable);
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
