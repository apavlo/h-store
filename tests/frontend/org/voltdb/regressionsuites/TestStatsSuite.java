package org.voltdb.regressionsuites;

import java.util.Map;
import java.util.TreeMap;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.StringUtil;

/**
 * Simple test suite for internal stats
 * @author pavlo
 */
public class TestStatsSuite extends RegressionSuite {
    
    private static final String PREFIX = "stats";
    private static final double SCALEFACTOR = 0.001;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestStatsSuite(String name) {
        super(name);
    }
    
    private void checkTupleAccessCount(String tableName, VoltTable result, int expected) {
        int total = 0;
        //System.err.println(VoltTableUtil.format(result));
        while (result.advanceRow()) {
            if (result.getString("TABLE_NAME").equalsIgnoreCase(tableName)) {
                total += result.getLong("TUPLE_ACCESSES");
            }
        } // WHILE
        assertEquals(expected, total);
    }
    
    private void checkIndexEntryCount(Client client, CatalogContext catalogContext, boolean checkMemory) throws Exception {
        // Get the row count per table per partition
        // We need this so we can check that the indexes have the proper number of entries
        Map<String, Map<Integer, Long>> rowCounts = RegressionSuiteUtil.getRowCountPerPartition(client);
        assertEquals(rowCounts.toString(), catalogContext.getDataTables().size(), rowCounts.size());
//        System.err.println(StringUtil.formatMaps(rowCounts));
        
        // Loop through each table and make sure that each index reports back at least
        // some amount of data.
        ClientResponse cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.INDEX);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable result = cresponse.getResults()[0];
        for (Table tbl : catalogContext.getDataTables()) {
            if (tbl.getIndexes().isEmpty()) continue;

            Map<Integer, Long> expected = rowCounts.get(tbl.getName());
            assertNotNull(tbl.toString(), expected);
            
            for (Index idx : tbl.getIndexes()) {
                result.resetRowPosition();
                boolean found = false;
                while (result.advanceRow()) {
                    String idxName = result.getString("INDEX_NAME");
                    String tblName = result.getString("TABLE_NAME");
                    String idxType = result.getString("INDEX_TYPE");
                    int partitionId = (int)result.getLong("PARTITION_ID");
                    long entryCount= result.getLong("ENTRY_COUNT");
                    if (tbl.getName().equalsIgnoreCase(tblName) && idx.getName().equalsIgnoreCase(idxName)) {
                        long memoryEstimate = result.getLong("MEMORY_ESTIMATE");
                        //System.err.println(tblName + "------" + entryCount + "-------" + idxName + "------" + idxType + "---------" + memoryEstimate);
                        
                        if (checkMemory) {
                            assert(memoryEstimate > 0) :
                                String.format("Unexpected zero memory estimate for index %s.%s", tblName, idxName);
                        }
                        found = true;
                        
                        // Check whether the entry count is correct if it's a unique index
                        if (idx.getUnique()) {
                            Long expectedCnt = expected.get(partitionId);
                            assertNotNull(String.format("TABLE:%s PARTITION:%d", tbl.getName(), partitionId), expectedCnt);
                            assertEquals(idx.fullName(), expectedCnt.longValue(), entryCount);
                        }
                    }
                } // WHILE
                // Make sure that we got all the indexes for the table.
                assert(found) : "Did not get index stats for " + idx.fullName();
            } // FOR
        } // FOR
    }
    
    /**
     * testTupleAccessCountIndex
     */
    public void testTupleAccessCountIndex() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client);
        
        ClientResponse cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.TABLE);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        this.checkTupleAccessCount(TPCCConstants.TABLENAME_ITEM, cresponse.getResults()[0], 0);
        
        int expected = 20;
        for (int i = 0; i < expected; i++) {
            cresponse = client.callProcedure("GetItemIndex", 1);
            assertNotNull(cresponse);
            assertEquals(Status.OK, cresponse.getStatus());
        } // FOR
        
        cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.TABLE);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        this.checkTupleAccessCount(TPCCConstants.TABLENAME_ITEM, cresponse.getResults()[0], expected);
    }
    
    /**
     * testTupleAccessCountNoIndex
     */
    public void testTupleAccessCountNoIndex() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client);
        
        ClientResponse cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.TABLE);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        this.checkTupleAccessCount(TPCCConstants.TABLENAME_ITEM, cresponse.getResults()[0], 0);
        
        int expected = 20;
        for (int i = 0; i < expected; i++) {
            cresponse = client.callProcedure("GetItemNoIndex");
            assertNotNull(cresponse);
            assertEquals(Status.OK, cresponse.getStatus());
        } // FOR
        
        cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.TABLE);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        this.checkTupleAccessCount(TPCCConstants.TABLENAME_ITEM, cresponse.getResults()[0], expected);
    }
    
    /**
     * testRemoteQueryProfiling
     */
    public void testRemoteQueryProfiling() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        
        // Skip this test if there is only one partition
        if (catalogContext.numberOfPartitions == 1) return;
        
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client);
        ClientResponse cresponse;

        // Invoke a NewOrder DTXN
        Object params[] = RegressionSuiteUtil.generateNewOrder(catalogContext.numberOfPartitions, true, 1, 1);
        cresponse = client.callProcedure(neworder.class.getSimpleName(), params);
        assertEquals(Status.OK, cresponse.getStatus());
        assertFalse(cresponse.toString(), cresponse.isSinglePartition());
        
        // Check that our remote query counters got increased 
        cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.TXNPROFILER);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        // System.out.println(VoltTableUtil.format(results[0]));
        
        Map<String, Long> profilerStats = new TreeMap<String, Long>();
        while (results[0].advanceRow()) {
            String procName = results[0].getString("PROCEDURE");
            if (procName.equalsIgnoreCase(neworder.class.getSimpleName()) == false) continue;
            
            for (int i = 0, cnt = results[0].getColumnCount(); i < cnt; i++) {
                String colName = results[0].getColumnName(i);
                if (colName.toUpperCase().startsWith("FIRST_") && results[0].getColumnType(i) == VoltType.BIGINT) {
                    profilerStats.put(colName, results[0].getLong(i));
                }
            } // FOR
        } // WHILE
        
        for (String key : profilerStats.keySet()) {
            // All count columns should be exactly one
            if (key.endsWith("_CNT")) {
                assertEquals(key, 1l, (long)profilerStats.get(key));
            }
            // Everything else just needs to be greater than zero
            else {
                assertTrue(key, (long)profilerStats.get(key) > 0);
            }
        } // FOR
        System.out.println(StringUtil.formatMaps(profilerStats));
    }

    /**
     * testIndexStats
     */
    public void testIndexStats() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client);
        this.checkIndexEntryCount(client, catalogContext, true);
    }
    
    /**
     * testIndexStatsAfterDelete
     */
    public void testIndexStatsAfterDelete() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client);
        
        // Delete all of the tuples from each table and check to make sure the index stats are still valid
        for (Table tbl : catalogContext.getDataTables()) {
            // long numRows = RegressionSuiteUtil.getRowCount(client, tbl);
            String sql = String.format("DELETE FROM %s", tbl.getName());
            RegressionSuiteUtil.sql(client, sql);
        } // FOR
        
        // Then insert one tuple back for each table
        for (Table tbl : catalogContext.getDataTables()) {
            RegressionSuiteUtil.loadRandomData(client, tbl, getRandom(), 1);
            assertEquals(tbl.getName(), 1, RegressionSuiteUtil.getRowCount(client, tbl));
        } // FOR
        
        // Then check to make sure the counts are correct
        // FIXME: We should really be check memory usage here but we're not!!
        this.checkIndexEntryCount(client, catalogContext, false);

    }
    
    public static Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestStatsSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);
        builder.setGlobalConfParameter("site.specexec_enable", true);
        builder.setGlobalConfParameter("site.specexec_nonblocking", true);
        builder.setGlobalConfParameter("site.txn_profiling", true);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addAllDefaults();
        project.addStmtProcedure("GetItemIndex", "SELECT * FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?");
        project.addStmtProcedure("GetItemNoIndex", "SELECT * FROM " + TPCCConstants.TABLENAME_ITEM + " LIMIT 1");

        VoltServerConfig config;
        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX + "-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX + "-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
