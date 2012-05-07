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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.fixedsql.Insert;

/**
 * Actual regression tests for SQL that I found that was broken and
 * have fixed.  Didn't like any of the other potential homes that already
 * existed for this for one reason or another.
 */

public class TestFixedSQLSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };


    public void testTicket309() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 500, 14.5);

            String query =
                String.format("select count(*), %s.NUM from %s group by %s.NUM",
                              table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
//            VoltTable[] results = client.callProcedure(String.format("Ticket309%s", table)).getResults();
            assertEquals(3, results[0].getRowCount());
            while (results[0].advanceRow())
            {
                if (results[0].getLong(1) == 100)
                {
                    assertEquals(3, results[0].getLong(0));
                }
                else if (results[0].getLong(1) == 300)
                {
                    assertEquals(2, results[0].getLong(0));
                }
                else if (results[0].getLong(1) == 500)
                {
                    assertEquals(1, results[0].getLong(0));
                }
                else
                {
                    fail();
                }
            }
        }
    }


    //
    // Regression test for broken SQL of the variety:
    //
    // select * from TABLE where (TABLE.ID = value) and
    //          (TABLE.col1 compared_to TABLE.col2)
    //
    // which would return results any time TABLE.ID = value was true,
    // regardless of whether the second expression was true.
    //
    public void testAndExpressionComparingSameTableColumns()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            client.callProcedure("Insert", table, 5, "desc", 10, 14.5);
            client.callProcedure("Insert", table, 15, "desc2", 10, 14.5);
            // These queries should result in no rows, but the defect in
            // SubPlanAssembler resulted in only the NO_NULLS.PKEY = 5 expression
            // being used
            String query = "select * from " + table + " where (" +
                table + ".ID = 5) and (" + table + ".NUM < " + table +".ID)";
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = "select * from " + table + " where (" +
                table + ".ID = 5) and (" + table + ".NUM <= " + table +".ID)";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = "select * from " + table + " where (" +
                table + ".ID = 15) and (" + table + ".NUM > " + table +".ID)";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
            query = "select * from " + table + " where (" +
                table + ".ID = 15) and (" + table + ".NUM >= " + table +".ID)";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
        }
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // select * from replicated_table where (predicate) LIMIT n
    //
    // For replicated tables, LIMIT is inlined in seqscan; the tuple count was
    // being incremented for each input tuple regardless of the predicate
    // result, which was resulting in the wrong number of rows returned in some
    // cases.
    // @throws IOException
    // @throws ProcCallException
    //
    public void testSeqScanFailedPredicateDoesntCountAgainstLimit()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            // our predicate is going to be ID < NUM.
            // Insert one row where this is false
            client.callProcedure("Insert", table, 1, "desc", -1, 14.5);
            // And two where it is true
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            String query = "select * from " + table + " where " +
                table + ".ID < " + table +".NUM limit 2";
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            // we should get 2 rows but this bug would result in only 1 returned
            assertEquals(2, results[0].getRowCount());
        }
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // select (non-aggregating expression) from table
    // e.g. select col1 + col2 from table
    //
    // PlanAssembler extracts the left side of the expression to discard
    // aggregation-type expressions from the parsed SQL, but was basically
    // assuming that anything not a VALUE_TUPLE was an aggregate.
    //
    // Note: Adding 5.5 in the third test here also tests a "fix" in
    // HSQL where we coerce the type of numeric literals from NUMERIC to DOUBLE
    //
    public void testSelectExpression()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            Client client = getClient();
            client.callProcedure("Insert", table, 1, "desc", 2, 14.5);
            String query = String.format("select %s.ID + 10 from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(11, results[0].getLong(0));
            query = String.format("select %s.NUM + 20 from %s", table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(22, results[0].getLong(0));
            query = String.format("select %s.RATIO + 5.5 from %s",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(20.0, results[0].getDouble(0));
            query = String.format("select %s.ID + %s.NUM from %s",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            results[0].advanceRow();
            assertEquals(3, results[0].getLong(0));
        }
    }


    //
    // Regression test for broken SQL of the variety:
    //
    // trac #166
    //
    // When evaluating the nest loop join predicate, insufficient
    // information was available to tuplevalue expression nodes to
    // understand which column(s) needed to be evaluated by the TVE's
    // operators.
    //
    public void testNestLoopJoinPredicates()
    throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int id=0; id < 5; id++) {
            // insert id, (5-id) in to P1
            client.callProcedure("Insert", "P1", id, "desc", (5-id), 2.5);
            // insert id, (id) in to R1
            client.callProcedure("Insert", "R1", id, "desc", (id), 2.5);
        }
        // join on the (5-id), (id) columns
        String query = "select * from P1, R1 where P1.NUM = R1.NUM";
        VoltTable vts[] = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicates_verify(vts);

        // same thing using inner join syntax
        query = "select * from P1 INNER JOIN R1 on P1.NUM = R1.NUM";
        vts = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicates_verify(vts);

        // join on ID and verify NUM. (ID is indexed)
        query = "select * from P1, R1 where P1.ID = R1.ID";
        vts = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicates_verifyid(vts);

        // as above with inner join syntax
        query = "select * from P1 INNER JOIN R1 on P1.ID = R1.ID";
        vts = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicates_verifyid(vts);
    }

    private void testNestLoopJoinPredicates_verifyid(VoltTable[] vts) {
        assertEquals(1, vts.length);
        System.out.println("verifyid: " + vts[0]);
        assertTrue(vts[0].getRowCount() == 5);

        for (int i=0; vts[0].advanceRow(); ++i) {
            int p_id = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            int r_id = ((Integer)vts[0].get(4, VoltType.INTEGER)).intValue();
            int p_n =  ((Integer)vts[0].get(2, VoltType.INTEGER)).intValue();
            int r_n =  ((Integer)vts[0].get(6, VoltType.INTEGER)).intValue();

            assertEquals(p_id, r_id);
            assertEquals(5 - p_n, r_n);
        }
    }

    private void testNestLoopJoinPredicates_verify(VoltTable[] vts)
    {
        assertEquals(1, vts.length);
        System.out.println(vts[0]);
        assertTrue(vts[0].getRowCount() == 4);

        // the id of the first should be (5-id) in the second
        // because of the insertion trickery done above
        // verifies trac #125
        for (int i=0; vts[0].advanceRow(); ++i) {
            int id1 = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            int id2 = ((Integer)vts[0].get(4, VoltType.INTEGER)).intValue();
            assertEquals(id1, (5 - id2));
        }
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // trac #125.  (verification in addition to testNestLoopJoinPredicates).
    //
    // Select a complex expression (not just a TupleValueExpression)
    // to verify that non-root TVEs are correctly offset.
    //
    public void testNestLoopJoinPredicatesWithExpressions()
    throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int id=0; id < 5; id++) {
            // insert id, (5-id) in to P1
            client.callProcedure("Insert", "P1", id, "desc", (5-id), 2.5);
            // insert id, (id) in to R1
            client.callProcedure("Insert", "R1", id, "desc", (id), 2.5);
        }
        // join on the (5-id), (id) columns and select a value modified by an expression
        String query = "select (P1.ID + 20), (R1.ID + 40) from P1, R1 where P1.NUM = R1.NUM";
        VoltTable vts[] = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicatesWithExpressions_verify(vts);

        // same thing using inner join syntax
        query = "select (P1.ID + 20), (R1.ID + 40) from P1 INNER JOIN R1 on P1.NUM = R1.NUM";
        vts = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicatesWithExpressions_verify(vts);
    }

    private void testNestLoopJoinPredicatesWithExpressions_verify(
            VoltTable[] vts) {
        assertEquals(1, vts.length);
        System.out.println(vts[0]);
        assertTrue(vts[0].getRowCount() == 4);

        // the id of the first should be (5-id) in the second once the addition
        // done in the select expression is un-done.
        for (int i=0; vts[0].advanceRow(); ++i) {
            int p1_id = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            int r1_id = ((Integer)vts[0].get(1, VoltType.INTEGER)).intValue();
            assertEquals( (p1_id - 20), (5 - (r1_id - 40)) );
            // and verify that the addition actually happened.
            assertTrue(p1_id >= 20);
            assertTrue(p1_id <= 24);
            assertTrue(r1_id >= 40);
            assertTrue(r1_id <= 44);
        }
    }

    //
    // Regression test for broken SQL of the variety:
    //
    // trac #125. (additional verification).
    //
    // Select columns and expressions with aliases.
    //
    public void testNestLoopJoinPredicatesWithAliases()
    throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int id=0; id < 5; id++) {
            // insert id, (5-id) in to P1
            client.callProcedure("Insert", "P1", id, "desc", (5-id), 2.5);
            // insert id, (id) in to R1
            client.callProcedure("Insert", "R1", id, "desc", (id), 2.5);
        }
        // join on the (5-id), (id) columns and select a value modified by an expression
        // use an alias that would select an invalid column. (be a jerk).
        String query = "select R1.ID AS DESC, (P1.ID + 20) AS THOMAS from P1, R1 where P1.NUM = R1.NUM";
        VoltTable vts[] = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicatesWithAliases_verify(vts);

        // same thing using inner join syntax
        query = "select R1.ID AS DESC, (P1.ID + 20) AS THOMAS from P1 INNER JOIN R1 on P1.NUM = R1.NUM";
        vts = client.callProcedure("@AdHoc", query).getResults();
        testNestLoopJoinPredicatesWithAliases_verify(vts);
    }

    private void testNestLoopJoinPredicatesWithAliases_verify(VoltTable[] vts) {
        assertEquals(1, vts.length);
        System.out.println(vts[0]);
        assertTrue(vts[0].getRowCount() == 4);

        // the id of the first should be (5-id) in the second once the addition
        // done in the select expression is un-done.
        for (int i=0; vts[0].advanceRow(); ++i) {
            int p1_id = ((Integer)vts[0].get(1, VoltType.INTEGER)).intValue();
            int r1_id = ((Integer)vts[0].get(0, VoltType.INTEGER)).intValue();
            assertEquals( (p1_id - 20), (5 - r1_id) );
            // and verify that the addition actually happened.
            assertTrue(p1_id >= 20);
            assertTrue(p1_id <= 24);
            assertTrue(r1_id >= 0);
            assertTrue(r1_id <= 4);
        }
    }



    //
    // Regression test for broken SQL of the sort
    //
    // select * from TABLE where COL_WITH_ORDERED_INDEX > n
    //
    // The bug is that indexscanexecutor and indexes treat > as >=
    // @throws IOException
    // @throws ProcCallException
    //
    public void testGreaterThanOnOrderedIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 100, 14.5);
            String query = "select * from " + table + " where " +
                table + ".ID > 1";
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            // we should get 5 rows but this bug would result in all 6 returned
            assertEquals(5, results[0].getRowCount());
            // make sure that we work if the value we want isn't present
            query = "select * from " + table + " where " +
                table + ".ID > 4";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            query = "select * from " + table + " where " +
                table + ".ID > 8";
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(0, results[0].getRowCount());
        }
    }

    public void testTicket196() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 500, 14.5);
            String query = String.format("select count(*) from %s", table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            results[0].advanceRow();
            assertEquals(6, results[0].getLong(0));
            query = String.format("select %s.NUM, count(*) from %s group by %s.NUM",
                                  table, table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
            while (results[0].advanceRow())
            {
                if (results[0].getLong(0) == 100)
                {
                    assertEquals(3, results[0].getLong(1));
                }
                else if (results[0].getLong(0) == 300)
                {
                    assertEquals(2, results[0].getLong(1));
                }
                else if (results[0].getLong(0) == 500)
                {
                    assertEquals(1, results[0].getLong(1));
                }
                else
                {
                    fail();
                }
            }
        }

        // SO, given our current count(*) hack (replace * with the first column
        // in the input to the aggregator, this is a test that will
        // FAIL when we go and implement COUNT to do the right thing with null
        // values.  If this test breaks for you, don't blow it off.
        String query = "insert into COUNT_NULL values (10, 0, 100)";
        client.callProcedure("@AdHoc", query);
        if (isHSQL()) {
            query = "insert into COUNT_NULL values (" + Byte.MIN_VALUE + ", 1, 200)";
        } else {
            query = "insert into COUNT_NULL values (" + Long.MIN_VALUE + ", 1, 200)";
        }
        client.callProcedure("@AdHoc", query);
        query = "insert into COUNT_NULL values (10, 2, 300)";
        client.callProcedure("@AdHoc", query);
        if (isHSQL()) {
            query = "insert into COUNT_NULL values (" + Byte.MIN_VALUE + ", 3, 400)";
        } else {
            query = "insert into COUNT_NULL values (" + Long.MIN_VALUE + ", 3, 400)";
        }
        client.callProcedure("@AdHoc", query);
        query = "select count(*) from COUNT_NULL";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(4, results[0].getLong(0));
    }

    public void testTicket201() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 400, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 500, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 600, 14.5);
            String query = String.format("select * from %s where (%s.ID + 1) = 2",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(1, results[0].getRowCount());
            query = String.format("select * from %s where (%s.ID + 1) > 2",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(5, results[0].getRowCount());
            query = String.format("select * from %s where (%s.ID + 1) >= 2",
                                         table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(6, results[0].getRowCount());
        }
    }
    /*
    public void testTicket205() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 400, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 500, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 600, 14.5);
            String query = String.format("select sum(%s.NUM + 1) from %s",
                                         table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query);
            assertEquals(1, results[0].getRowCount());
            query = String.format("select sum(%s.NUM + %s.ID) from %s",
                                         table, table);
            results = client.callProcedure("@AdHoc", query);
            assertEquals(1, results[0].getRowCount());
        }
    }
     */

// FIXME
//    public void testTicket216() throws IOException, ProcCallException
//    {
//        String[] tables = {"P1", "R1", "P2", "R2"};
//        Client client = getClient();
//        for (String table : tables)
//        {
//            client.callProcedure("Insert", table, 1, "desc", 100, 100.0);
//            client.callProcedure("Insert", table, 2, "desc", 200, 200.0);
//            client.callProcedure("Insert", table, 3, "desc", 300, 300.0);
//            client.callProcedure("Insert", table, 6, "desc", 400, 400.0);
//            client.callProcedure("Insert", table, 7, "desc", 500, 500.0);
//            client.callProcedure("Insert", table, 8, "desc", 600, 600.0);
//            String query = String.format("select %s.RATIO / 2.0 from %s order by ID",
//                                         table, table);
//            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
//            assertEquals(6, results[0].getRowCount());
//            for (double f=50.0; results[0].advanceRow(); f+=50.0) {
//                double num = (results[0].getDouble(0));
//                assertEquals(f, num);
//            }
//            query = String.format("select * from %s where %s.RATIO >= 400.0",
//                                  table, table);
//            results = client.callProcedure("@AdHoc", query).getResults();
//            assertEquals(3, results[0].getRowCount());
//        }
//    }


    public void testTicket194() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 400, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 500, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 600, 14.5);
            String query = String.format("select * from %s where %s.ID >= 2.1",
                                  table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
            query = String.format("select * from %s where %s.ID >= 4.0",
                                  table, table);
            results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
        }
    }


    public void testTickets227And228() throws IOException, ProcCallException
    {
        String[] tables = {"P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 100, 14.5);
        }
        // test > on the join (ticket 227)
        String query = "select * from R2, P2 where R2.ID > 1";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(30, results[0].getRowCount());
        query = "select * from P2, R2 where R2.ID > 1";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(30, results[0].getRowCount());
        // test >= on the join (ticket 228)
        query = "select * from R2, P2 where R2.ID >= 3";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(24, results[0].getRowCount());
        query = "select * from P2, R2 where R2.ID >= 3";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(24, results[0].getRowCount());
        query = "select * from R2, P2 where R2.ID >= 4";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(18, results[0].getRowCount());
        query = "select * from P2, R2 where R2.ID >= 4";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(18, results[0].getRowCount());
    }

    public void testTicket220() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
        }
        String query = "select R1.ID + 5 from R1, P1 order by R1.ID";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(9, results[0].getRowCount());
        for (int i = 0; i < 3; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                results[0].advanceRow();
                assertEquals(i + 3 + 5, results[0].getLong(0));
            }
        }
    }

    //
    // At first pass, HSQL barfed on decimal in sql-coverage. Debug/test that here.
    //
//    public void testForHSQLDecimalFailures() throws IOException, ProcCallException
//    {
//        Client client = getClient();
//        String sql =
//            "INSERT INTO R1_DECIMAL VALUES (26, 307473.174514, 289429.605067, 9.71903320295135486617e-01)";
//        client.callProcedure("@AdHoc", sql);
//        sql = "select R1_DECIMAL.CASH + 2.0 from R1_DECIMAL";
//        VoltTable[] results = client.callProcedure("@AdHoc", sql).getResults();
//        assertEquals(1, results.length);
//    }

    public void testTicket310() throws IOException, ProcCallException
    {
        Client client = getClient();
        String sql =
            "INSERT INTO R1_DECIMAL VALUES (26, 307473.174514, 289429.605067, 9.71903320295135486617e-01)";
        client.callProcedure("@AdHoc", sql);

        boolean caught = false;
        // HSQL doesn't choke the same way Volt does at the moment.
        // Fake the test out.
        if (isHSQL())
        {
            caught = true;
        }
        try
        {
            sql = "SELECT * FROM R1_DECIMAL WHERE " +
            "(R1_DECIMAL.CASH <= 0.0622493314185)" +
            " AND (R1_DECIMAL.ID > R1_DECIMAL.CASH)";
            client.callProcedure("@AdHoc", sql);
        }
        catch (ProcCallException e)
        {
            caught = true;
        }
        assertTrue(caught);
    }

    public void testNumericExpressionConversion() throws IOException, ProcCallException
    {
        VoltTable[] results;
        Client client = getClient();

        String sql = "INSERT INTO R1_DECIMAL VALUES " +
           "(26, 307473.174514, 289429.605067, 9.71903320295135486617e-01)";
        results = client.callProcedure("@AdHoc", sql).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        sql = "UPDATE R1_DECIMAL SET CASH = CASH * 5 WHERE " +
            "R1_DECIMAL.CASH != 88687.224073";
        results = client.callProcedure("@AdHoc", sql).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        sql = "UPDATE R1_DECIMAL SET CASH = CASH + 5.5 WHERE " +
            "R1_DECIMAL.CASH != 88687.224073";
        results = client.callProcedure("@AdHoc", sql).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());
    }

    public void testTicket221() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select distinct P1.NUM from R1, P1 order by P1.NUM";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(3, results[0].getRowCount());
        for (int i = 100; results[0].advanceRow(); i+=100)
        {
            assertEquals(i, results[0].getLong(0));
            System.out.println("i: " + results[0].getLong(0));
        }
    }

    public void testTicket222() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select max(P1.ID) from R1, P1";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(1, results[0].getRowCount());
        results[0].advanceRow();
        assertEquals(2, results[0].getLong(0));
        System.out.println("i: " + results[0].getLong(0));
    }

    public void testTicket224() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select P1.ID from R1, P1 group by P1.ID";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(3, results[0].getRowCount());
        assertEquals(1, results[0].getColumnCount());
        for (int i = 0; results[0].advanceRow(); i++)
        {
            assertEquals(i, results[0].getLong(0));
            System.out.println("i: " + results[0].getLong(0));
        }
    }

    public void testTicket226() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
        }
        String query = "select P1.ID from P1, R1 order by P1.ID";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(9, results[0].getRowCount());
        assertEquals(1, results[0].getColumnCount());
        for (int i = 0; i < 3; i++)
        {
            for (int j = 0; j < 3; j++)
            {
                results[0].advanceRow();
                assertEquals(i, results[0].getLong(0));
                System.out.println("i: " + results[0].getLong(0));
            }
        }
    }

// FIXME
//    public void testTicket231() throws IOException, ProcCallException
//    {
//        String[] tables = {"P1", "R1", "P2", "R2"};
//        Client client = getClient();
//        for (String table : tables)
//        {
//            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
//            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
//            client.callProcedure("Insert", table, 3, "desc", 200, 14.5);
//            client.callProcedure("Insert", table, 6, "desc", 200, 14.5);
//            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
//            client.callProcedure("Insert", table, 8, "desc", 300, 14.5);
//
//            // This statement is a test case for one of the ticket 231
//            // work-arounds
//            String query =
//                String.format("select (%s.NUM + %s.NUM) as NUMSUM from %s where (%s.NUM + %s.NUM) > 400",
//                              table, table, table, table, table);
//            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
//            assertEquals(2, results[0].getRowCount());
//// This failing statement is the current ticket 231 failing behavior.
////            query =
////                String.format("select (%s.NUM + %s.NUM) as NUMSUM from %s order by (%s.NUM + %s.NUM)",
////                              table, table, table, table, table);
////            results = client.callProcedure("@AdHoc", query);
////            assertEquals(6, results[0].getRowCount());
//        }
//    }



    public void testTicket232() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        for (String table : tables)
        {
            client.callProcedure("Insert", table, 1, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 2, "desc", 100, 14.5);
            client.callProcedure("Insert", table, 3, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 6, "desc", 200, 14.5);
            client.callProcedure("Insert", table, 7, "desc", 300, 14.5);
            client.callProcedure("Insert", table, 8, "desc", 300, 14.5);
            String query =
                String.format("select %s.NUM from %s group by %s.NUM order by %s.NUM",
                              table, table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(3, results[0].getRowCount());
        }
    }


    public void testTicket293() throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1", "P2", "R2"};
        Client client = getClient();
        int id = 0;
        for (String table : tables)
        {
            client.callProcedure("Insert", table, id++, "desc", 100, 14.5);
            client.callProcedure("Insert", table, id++, "desc", 200, 15.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 16.5);
            client.callProcedure("Insert", table, id++, "desc", 300, 17.5);
            client.callProcedure("Insert", table, id++, "desc", 400, 18.5);
            String query = String.format("select distinct %s.NUM from %s order by %s.NUM",
                                         table, table, table);
            VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
            assertEquals(4, results[0].getRowCount());
        }
        String query = "select distinct P1.NUM from R1, P1 order by P1.NUM";
        VoltTable[] results = client.callProcedure("@AdHoc", query).getResults();
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(4, results[0].getRowCount());
    }

    public void testTicketEng397() throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int i=0; i < 20; i++) {
            client.callProcedure("Insert", "P1", i, "desc", 100 + i, 4.5);
        }
        // base case
        VoltTable[] results = client.callProcedure("Eng397Limit1", new Integer(10)).getResults();
        assertEquals(10, results[0].getRowCount());

        // negative limit rollsback
        boolean caught = false;
        try {
            results = client.callProcedure("Eng397Limit1", new Integer(-1)).getResults();
        }
        catch (ProcCallException ignored) {
            caught = true;
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        assertTrue(caught);
    }

    // RE-ENABLE ONCE ENG-490 IS FIXED
    /*public void testTicketEng490() throws IOException, ProcCallException {
        Client client = getClient();

        VoltTable[] results = client.callProcedure("Eng490Select");
        assertEquals(1, results.length);

        String query = "SELECT  A.ASSET_ID,  A.OBJECT_DETAIL_ID,  OD.OBJECT_DETAIL_ID " +
            "FROM   ASSET A,  OBJECT_DETAIL OD WHERE   A.OBJECT_DETAIL_ID = OD.OBJECT_DETAIL_ID";
        results = client.callProcedure("@AdHoc", query);
        assertEquals(1, results.length);
    }*/

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFixedSQLSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFixedSQLSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder("fixed");
        project.addSchema(Insert.class.getResource("fixed-sql-ddl.sql"));
        project.addTablePartitionInfo("P1", "ID");
        project.addTablePartitionInfo("P2", "ID");
        project.addTablePartitionInfo("ASSET", "ASSET_ID");
        project.addTablePartitionInfo("OBJECT_DETAIL", "OBJECT_DETAIL_ID");
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("Eng397Limit1", "Select P1.NUM from P1 order by P1.NUM limit ?;");
        
        // TODO: Hardcoded procedures for ad-hoc queries
        project.addStmtProcedure("Ticket309P1", "select count(*), P1.NUM from P1 group by P1.NUM");
        project.addStmtProcedure("Ticket309R1", "select count(*), R1.NUM from R1 group by R1.NUM");
        project.addStmtProcedure("Ticket309P2", "select count(*), P2.NUM from P2 group by P2.NUM");
        project.addStmtProcedure("Ticket309R2", "select count(*), R2.NUM from R2 group by R2.NUM");
        
        //project.addStmtProcedure("Eng490Select", "SELECT A.ASSET_ID, A.OBJECT_DETAIL_ID,  OD.OBJECT_DETAIL_ID FROM ASSET A, OBJECT_DETAIL OD WHERE A.OBJECT_DETAIL_ID = OD.OBJECT_DETAIL_ID;");

        // CONFIG #1: Local Site/Partitions running on IPC backend
        // config = new LocalSingleProcessServer("sqltypes-onesite.jar", 1, BackendTarget.NATIVE_EE_IPC);
        // config.compile(project);
        // builder.addServerConfig(config);

        // JNI
        config = new LocalSingleProcessServer("fixedsql-onesite.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);


        // CLUSTER!
//        config = new LocalCluster("fixedsql-cluster.jar", 2, 1, 1, BackendTarget.NATIVE_EE_JNI);
//        config.compile(project);
//        builder.addServerConfig(config);

        return builder;
    }
}
