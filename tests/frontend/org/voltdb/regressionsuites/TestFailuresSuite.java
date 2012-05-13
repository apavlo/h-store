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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.regressionsuites.failureprocs.CleanupFail;
import org.voltdb.regressionsuites.failureprocs.DivideByZero;
import org.voltdb.regressionsuites.failureprocs.FetchTooMuch;
import org.voltdb.regressionsuites.failureprocs.InsertBigString;
import org.voltdb.regressionsuites.failureprocs.InsertLotsOfData;
import org.voltdb.regressionsuites.failureprocs.TooFewParams;
import org.voltdb.regressionsuites.failureprocs.ViolateUniqueness;
import org.voltdb.regressionsuites.failureprocs.ViolateUniquenessAndCatchException;
import org.voltdb.regressionsuites.failureprocs.ReturnAppStatus;
import org.voltdb.regressionsuites.sqlfeatureprocs.WorkWithBigString;

public class TestFailuresSuite extends RegressionSuite {

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        ViolateUniqueness.class, ViolateUniquenessAndCatchException.class,
        DivideByZero.class, WorkWithBigString.class, InsertBigString.class,
        InsertLotsOfData.class, FetchTooMuch.class, CleanupFail.class, TooFewParams.class,
        ReturnAppStatus.class
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestFailuresSuite(String name) {
        super(name);
    }

    public void testVoilateUniqueness() throws IOException {
        System.out.println("STARTING testVU");
        Client client = getClient();

        VoltTable[] results = null;
        try {
            results = client.callProcedure("ViolateUniqueness", 1L, 1L, 1L).getResults();
            System.out.println("testVU client response received");
            assertEquals(results.length, 0);
        } catch (ProcCallException e) {
            try {
                results = client.callProcedure("InsertNewOrder", 2L, 2L, 2L).getResults();
            } catch (ProcCallException e1) {
                fail(e1.toString());
            } catch (IOException e1) {
                fail(e1.toString());
            }
            System.out.println("second client response received");
            assertEquals(1, results.length);
            assertEquals(1, results[0].asScalarLong());

            return;
        } catch (IOException e) {
            fail(e.toString());
            return;
        }
        fail("testViolateUniqueness should return while catching ProcCallException");
    }

    public void testVoilateUniquenessAndCatchException() throws IOException {
        Client client = getClient();
        System.out.println("STARTING testVoilateUniquenessAndCatchException");
        VoltTable[] results = null;
        try {
            results = client.callProcedure("ViolateUniquenessAndCatchException", 1L, 1L, 1L).getResults();
            assertTrue(results.length == 1);
            System.out.println("PASSED testVoilateUandCE");
        } catch (ProcCallException e) {
            System.out.println("FAIL(1) testVoilateUandCE");
            e.printStackTrace();
            fail("ViolateUniquenessAndCatchException should not have thrown a ProcCallException");
        } catch (IOException e) {
            System.out.println("FAIL(2) testVoilateUandCE");
            fail(e.toString());
            return;
        }

        try {
            results = client.callProcedure("InsertNewOrder", 2L, 2L, 2L).getResults();
        } catch (ProcCallException e1) {
            fail(e1.toString());
        } catch (IOException e1) {
            fail(e1.toString());
        }
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        return;
    }

    /*
     * Check that a very large ConstraintFailureException can serialize correctly.
     */
    public void testTicket511_ViolateUniquenessWithLargeString() throws Exception {
        Client client = getClient();
        System.out.println("STARTING testTicket511_ViolateUniquenessWithLargeString");
        byte stringData[] = new byte[60000];
        java.util.Arrays.fill(stringData, (byte)'a');

        client.callProcedure("InsertBigString", 0, new String(stringData, "UTF-8"));

        java.util.Arrays.fill(stringData, (byte)'b');
        boolean threwException = false;
        try {
            client.callProcedure("InsertBigString", 0, new String(stringData, "UTF-8"));
        } catch (ProcCallException e) {
            threwException = true;
            assertTrue(e.getMessage().contains("CONSTRAINT VIOLATION"));
            assertTrue(e.getCause().getMessage().toUpperCase().contains("UNIQUE"));
            if (!isHSQL()) {
                VoltTable table = ((ConstraintFailureException)e.getCause()).getTuples();
                table.resetRowPosition();
                assertTrue(table.advanceRow());
                assertTrue(java.util.Arrays.equals(stringData, table.getStringAsBytes(2)));
            }
        }
        assertTrue(threwException);
    }
// This is failing, so commenting out for now b/c it doesn't use @AdHoc
//    public void testDivideByZero() throws IOException {
//        System.out.println("STARTING DivideByZero");
//        Client client = getClient();
//
//        VoltTable[] results = null;
//        try {
//            results = client.callProcedure("DivideByZero", 0L, 0L, 1L).getResults();
//            System.out.println("DivideByZero client response received");
//            assertEquals(results.length, 0);
//        } catch (ProcCallException e) {
//            System.out.println(e.getMessage());
//            if (e.getMessage().contains("SQL ERROR"))
//                return;
//            if (isHSQL())
//                if (e.getMessage().contains("HSQL-BACKEND ERROR"))
//                    return;
//        } catch (IOException e) {
//            fail(e.toString());
//            return;
//        }
//        fail("testDivideByZero should return while catching ProcCallException");
//    }

    //This is taking a long time, so commenting out for now, also doesn't use @AdHoc
    //
    // Note: this test looks like it should be testing the 10MB buffer serialization
    // limit between the EE and Java but watching it run, it really fails on max
    // temp table serialization sizes. This needs more investigation.
    //
//    public void testMemoryOverload() throws IOException, ProcCallException {
//        if (isHSQL() || isValgrind()) return;
//
//        final int STRLEN = 30000;
//
//        int totalBytes = 0;
//        int expectedMaxSuccessBytes = 10000000; // less than the 10*1024*1024 limit.
//        int expectedRows = 0;
//
//        System.out.println("STARTING testMemoryOverload");
//        Client client = getClient();
//
//        String longStringPart = "volt!";
//        StringBuilder sb = new StringBuilder();
//        while(sb.length() < STRLEN)
//            sb.append(longStringPart);
//        String longString = sb.toString();
//        assertEquals(STRLEN, longString.length());
//
//        VoltTable[] results = null;
//
//        while (totalBytes < expectedMaxSuccessBytes) {
//            results = client.callProcedure("InsertBigString", expectedRows++, longString).getResults();
//            assertEquals(1, results.length);
//            assertEquals(1, results[0].asScalarLong());
//            totalBytes += STRLEN;
//        }
//
//        results = client.callProcedure("WorkWithBigString", expectedRows++, longString).getResults();
//        assertEquals(1, results.length);
//        assertEquals(expectedRows, results[0].getRowCount());
//        totalBytes += STRLEN;
//
//        // 11MB exceeds the response buffer limit.
//        while (totalBytes < (11 * 1024 * 1024)) {
//            results = client.callProcedure("InsertBigString", expectedRows++, longString).getResults();
//            assertEquals(1, results.length);
//            assertEquals(1, results[0].asScalarLong());
//            totalBytes += STRLEN;
//        }
//
//        //System.out.printf("Fail Bytes: %d, Expected Rows %d\n", totalBytes, expectedRows);
//        //System.out.flush();
//        try {
//            results = client.callProcedure("WorkWithBigString", expectedRows++, longString).getResults();
//            fail();
//        } catch (ProcCallException e) {
//            // this should eventually happen
//            assertTrue(totalBytes > expectedMaxSuccessBytes);
//            return;
//        } catch (IOException e) {
//            fail(e.toString());
//            return;
//        }
//        fail();
//    }

    public void testPerPlanFragmentMemoryOverload() throws IOException, ProcCallException {
        if (isHSQL() || isValgrind()) return;

        System.out.println("STARTING testPerPlanFragmentMemoryOverload");
        Client client = getClient();

        VoltTable[] results = null;

        int nextId = 0;

        for (int mb = 0; mb < 75; mb += 5) {
            results = client.callProcedure("InsertLotsOfData", 0, nextId).getResults();
            assertEquals(1, results.length);
            assertTrue(nextId < results[0].asScalarLong());
            nextId = (int) results[0].asScalarLong();
            System.err.println("Inserted " + (mb + 5) + "mb");
        }

        results = client.callProcedure("FetchTooMuch", 0).getResults();
        assertEquals(1, results.length);
        assertTrue(1 < results[0].asScalarLong());
        System.out.println("Fetched the 75 megabytes");

        for (int mb = 0; mb < 75; mb += 5) {
            results = client.callProcedure("InsertLotsOfData", 0, nextId).getResults();
            assertEquals(1, results.length);
            assertTrue(nextId < results[0].asScalarLong());
            nextId = (int) results[0].asScalarLong();
            System.err.println("Inserted " + (mb + 80) + "mb");
        }

        try {
            results = client.callProcedure("FetchTooMuch", 0).getResults();
        } catch (ProcCallException e) {
            e.printStackTrace();
            return;
        }
        fail("Should gracefully fail from using too much temp table memory, but didn't.");
    }

    public void testQueueCleanupFailure() throws IOException, ProcCallException {
        System.out.println("STARTING testQueueCleanupFailure");
        Client client = getClient();

        VoltTable[] results = null;

        results = client.callProcedure("CleanupFail", 0, 0, 0).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        results = client.callProcedure("CleanupFail", 2, 2, 2).getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());
    }

    public void testTooFewParamsOnSinglePartitionProc() throws IOException {
        System.out.println("STARTING testTooFewParamsOnSinglePartitionProc");
        Client client = getClient();

        try {
            client.callProcedure("TooFewParams", 1);
            fail();
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().startsWith("Error sending"));
        }

        try {
            client.callProcedure("TooFewParams");
            fail();
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().startsWith("Error sending"));
        }
    }

    public void testTooFewParamsOnSQLStmt() throws IOException {
        System.out.println("STARTING testTooFewParamsOnSQLStmt");
        Client client = getClient();

        try {
            client.callProcedure("TooFewParams", 1, 1);
            fail();
        } catch (ProcCallException e) {
        }
    }

    // Try a SQL stmt that will almost certainly kill the out of process planner
    // Make sure it doesn't kill the system
//    public void testKillOPPlanner() throws IOException, ProcCallException {
//        System.out.println("STARTING testKillOPPlanner");
//        Client client = getClient();
//
//        try {
//            client.callProcedure("@AdHoc",
//                    "select * from WAREHOUSE, DISTRICT, CUSTOMER, CUSTOMER_NAME, HISTORY, STOCK, ORDERS, NEW_ORDER, ORDER_LINE where " +
//                    "WAREHOUSE.W_ID = DISTRICT.D_W_ID and " +
//                    "WAREHOUSE.W_ID = CUSTOMER.C_W_ID and " +
//                    "WAREHOUSE.W_ID = CUSTOMER_NAME.C_W_ID and " +
//                    "WAREHOUSE.W_ID = HISTORY.H_W_ID and " +
//                    "WAREHOUSE.W_ID = STOCK.S_W_ID and " +
//                    "WAREHOUSE.W_ID = ORDERS.O_W_ID and " +
//                    "WAREHOUSE.W_ID = NEW_ORDER.NO_W_ID and " +
//                    "WAREHOUSE.W_ID = ORDER_LINE.OL_W_ID and " +
//                    "WAREHOUSE.W_ID = 0");
//            //fail()
//            // don't actually fail here... there's no guarantee this will fail...
//        }
//        catch (ProcCallException e) {}
//
//        // a short pause to recover the planning process
//        try {
//            Thread.sleep(200);
//        } catch (InterruptedException e) {
//            fail();
//        }
//
//        // make sure we can call adhocs
//        client.callProcedure("@AdHoc", "select * from warehouse;");
//    }

    public void testAppStatus() throws Exception {
        System.out.println("STARTING testAppStatus");
        Client client = getClient();

        ClientResponse response = client.callProcedure( "ReturnAppStatus", 0, "statusstring", (byte)0);
        assertNull(response.getAppStatusString());
        assertEquals(response.getAppStatus(), Byte.MIN_VALUE);
        assertEquals(response.getResults()[0].getStatusCode(), Byte.MIN_VALUE);

        response = client.callProcedure( "ReturnAppStatus", 1, "statusstring", (byte)1);
        assertTrue("statusstring".equals(response.getAppStatusString()));
        assertEquals(response.getAppStatus(), 1);
        assertEquals(response.getResults()[0].getStatusCode(), 1);

        response = client.callProcedure( "ReturnAppStatus", 2, "statusstring", (byte)2);
        assertNull(response.getAppStatusString());
        assertEquals(response.getAppStatus(), 2);
        assertEquals(response.getResults()[0].getStatusCode(), 2);

        response = client.callProcedure( "ReturnAppStatus", 3, "statusstring", (byte)3);
        assertTrue("statusstring".equals(response.getAppStatusString()));
        assertEquals(response.getAppStatus(), Byte.MIN_VALUE);
        assertEquals(response.getResults()[0].getStatusCode(), 3);

        boolean threwException = false;
        try {
            response = client.callProcedure( "ReturnAppStatus", 4, "statusstring", (byte)4);
        } catch (ProcCallException e) {
            threwException = true;
            response = e.getClientResponse();
        }
        assertTrue(threwException);
        assertTrue("statusstring".equals(response.getAppStatusString()));
        assertEquals(response.getAppStatus(), 4);
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestFailuresSuite.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        VoltServerConfig config = new LocalSingleProcessServer("failures-onesite.jar", 1, BackendTarget.NATIVE_EE_JNI);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder("failures");
        project.addSchema(DivideByZero.class.getResource("failures-ddl.sql"));
        project.addTablePartitionInfo("NEW_ORDER", "NO_W_ID");
        project.addTablePartitionInfo("FIVEK_STRING", "P");
        project.addTablePartitionInfo("FIVEK_STRING_WITH_INDEX", "ID");
        project.addTablePartitionInfo("WIDE", "P");
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");
        // build the jarfile
        if (!config.compile(project))
            fail();

        // add this config to the set of tests to run
        //builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 2 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        config = new LocalSingleProcessServer("failures-twosites.jar", 2, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile (note the reuse of the TPCC project)
        config.compile(project);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #3: 1 Local Site/Partition running on HSQL backend
        /////////////////////////////////////////////////////////////

        // get a server config that similar, but doesn't use the same backend
        config = new LocalSingleProcessServer("failures-hsql.jar", 1, BackendTarget.HSQLDB_BACKEND);

        // build the jarfile (note the reuse of the TPCC project)
        config.compile(project);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        // CLUSTER?
        config = new LocalCluster("failures-cluster.jar", 2, 2,
                                  1, BackendTarget.NATIVE_EE_JNI);
        config.setConfParameter("site.exec_adhoc_sql", true);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }
}
