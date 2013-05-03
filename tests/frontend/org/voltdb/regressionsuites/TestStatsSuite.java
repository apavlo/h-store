package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.Hstoreservice.Status;

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
//        System.err.println(VoltTableUtil.format(result));
        while (result.advanceRow()) {
            if (result.getString("TABLE_NAME").equalsIgnoreCase(tableName)) {
                total += result.getLong("TUPLE_ACCESSES");
            }
        } // WHILE
        assertEquals(expected, total);
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

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestStatsSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addAllDefaults();
        project.addStmtProcedure("GetItemIndex", "SELECT * FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?");
        project.addStmtProcedure("GetItemNoIndex", "SELECT * FROM " + TPCCConstants.TABLENAME_ITEM + " LIMIT 1");
        
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
