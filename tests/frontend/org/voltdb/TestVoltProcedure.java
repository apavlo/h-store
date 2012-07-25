/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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

package org.voltdb;

import junit.framework.TestCase;
import java.util.Date;

import org.voltdb.client.ClientResponse;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.DefaultHasher;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.utils.PartitionEstimator;

public class TestVoltProcedure extends TestCase {
    private static long CLIENT_HANDLE = 1;
    
    static class DateProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Date arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Date arg;
    }

    static class LongProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(long arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static long arg;
    }

    static class LongArrayProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(long[] arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static long[] arg;
    }

    static class NPEProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(String arg) {
            return new VoltTable[arg.length()];
        }
    }

    static class NullProcedureWrapper extends VoltProcedure {
        //NullProcedureWrapper(ExecutionSite site, Class<?> c) {
        //  super(site, new Procedure(), c);
        //}

        VoltTable runQueryStatement(SQLStmt stmt, Object... params) {
            assert false;
            return null;
        }
        long runDMLStatement(SQLStmt stmt, Object... params) {
            assert false;
            return -1;
        }
        void addQueryStatement(SQLStmt stmt, Object... args) {
            assert false;
        }
        void addDMLStatement(SQLStmt stmt, Object... args) {
            assert false;
        }
        VoltTable[] executeQueryBatch() {
            assert false;
            return null;
        }
        long[] executeDMLBatch() {
            assert false;
            return null;
        }
    }

    ParameterSet nullParam;

    public void testDummyTest() {
        // Blah blah blah
    }
    
//    @Override
//    public void setUp() {
//        VoltDB manager = VoltDB.instance();
//        manager.initializeForTest();
//        manager.addProcedureForTest(DateProcedure.class.getName()).setClassname(DateProcedure.class.getName());
//        manager.addProcedureForTest(LongProcedure.class.getName()).setClassname(LongProcedure.class.getName());
//        manager.addProcedureForTest(LongArrayProcedure.class.getName()).setClassname(LongArrayProcedure.class.getName());
//        manager.addProcedureForTest(NPEProcedure.class.getName()).setClassname(NPEProcedure.class.getName());
//        manager.addSiteForTest("1");
//        site = new MockExecutionSite(1, VoltDB.getCatalog().serialize());
//        nullParam = new ParameterSet();
//        nullParam.setParameters(new Object[]{null});
//    }
//
//    public void testNullDate() {
//        ClientResponse r = call(DateProcedure.class);
//        assertEquals(null, DateProcedure.arg);
//        assertEquals(Status.OK, r.getStatus());
//    }
//
//    public void testNullLong() {
//        ClientResponse r = call(LongProcedure.class);
//        assertEquals(Status.ABORT_GRACEFUL, r.getStatus());
//        assertTrue(r.getExtra().contains("cannot be null"));
//    }
//
//    public void testNullLongArray() {
//        ClientResponse r = call(LongArrayProcedure.class);
//        assertEquals(null, LongArrayProcedure.arg);
//        assertEquals(Status.OK, r.getStatus());
//    }
//
//    public void testNullPointerException() {
//        ClientResponse r = call(NPEProcedure.class);
//        assertEquals(Status.ABORT_UNEXPECTED, r.getStatus());
//        assertTrue(r.getExtra().contains("NullPointerException"));
//    }

//    public void testProcedureStatsCollector() {
//        NullProcedureWrapper wrapper = new LongProcedure();
//        Procedure catalog_proc = site.database.getProcedures().get(LongProcedure.class.getName());
//        Cluster catalog_clus = null;
//        Database catalog_db = (Database)catalog_proc.getParent();
//        PartitionEstimator p_estimator = new PartitionEstimator(catalogContext, new DefaultHasher(catalog_db, 1));
//        wrapper.init(site, catalog_proc, BackendTarget.NATIVE_EE_JNI, null, catalog_clus, p_estimator, 1);
//        
//        ParameterSet params = new ParameterSet();
//        params.m_params = new Object[1];
//        params.m_params[0] = new Long(1);
//        assertNotNull(agent.selector);
//        assertNotNull(agent.source);
//        assertEquals(agent.selector, SysProcSelector.PROCEDURE);
//        assertEquals(agent.catalogId, site.cluster.getSites().get(Integer.toString(site.siteId)).getGuid());
//        Object statsRow[][] = agent.source.getStatsRows();
//        assertNotNull(statsRow);
//        assertEquals(statsRow[0][1], new Long(site.siteId));
//        assertEquals(statsRow[0][2], LongProcedure.class.getName());
//        assertEquals(statsRow[0][3], 0L); //Starts with 0 invocations
//        assertEquals(statsRow[0][4], 0L); //Starts with 0 timed invocations time
//        assertEquals(statsRow[0][5], (long)0); //Starts with 0 min execution time
//        assertEquals(statsRow[0][6], (long)0); //Starts with 0 max execution time
//        assertEquals(statsRow[0][7], 0L); //Average invocation length is 0 to start
//        for (int ii = 1; ii < 200; ii++) {
//            wrapper.call(ii, params.m_params);
//            statsRow = agent.source.getStatsRows();
//            assertEquals(statsRow[0][3], new Long(ii));
//        }
//        assertTrue(((Long)statsRow[0][3]).longValue() > 0L);
//        assertTrue(((Long)statsRow[0][4]).longValue() > 0L);
//        assertFalse(statsRow[0][5].equals(0));
//        assertFalse(statsRow[0][6].equals(0));
//        assertTrue(((Long)statsRow[0][7]) > 0L);
//    }

//    private ClientResponse call(Class<? extends NullProcedureWrapper> procedure) {
//        NullProcedureWrapper wrapper = null;
//        try {
//            wrapper = procedure.newInstance();
//        }  catch (InstantiationException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//        Procedure catalog_proc = site.database.getProcedures().get(procedure.getName());
//        Cluster catalog_clus = null;
//        Database catalog_db = (Database)catalog_proc.getParent();
//        PartitionEstimator p_estimator = new PartitionEstimator(catalogContext, new DefaultHasher(catalog_db, 1));
//        wrapper.init(site, catalog_proc, BackendTarget.NATIVE_EE_JNI, null, catalog_clus, p_estimator, 1);
//        return wrapper.callAndBlock(1l, CLIENT_HANDLE++, (Object) null);
//    }

}
