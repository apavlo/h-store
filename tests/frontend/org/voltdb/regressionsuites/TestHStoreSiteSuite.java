package org.voltdb.regressionsuites;

import java.util.concurrent.CountDownLatch;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.sysprocs.AdHoc;

import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for HStoreSite features
 * @author pavlo
 */
public class TestHStoreSiteSuite extends RegressionSuite {
    
    private static final String PREFIX = "hstoresite";
    private static final double SCALEFACTOR = 0.001;
    private static final int NUM_WAREHOUSES = 4;
    private static final int WAREHOUSE_ID = 1;
    private static final int DISTRICT_ID = 1;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestHStoreSiteSuite(String name) {
        super(name);
    }
    
    private void executeTestWorkload(Client client) throws Exception {
        RegressionSuiteUtil.initializeTPCCDatabase(this.getCatalogContext(), client);
        Object params[] = RegressionSuiteUtil.generateNewOrder(NUM_WAREHOUSES, false, WAREHOUSE_ID, DISTRICT_ID);
        String procName = neworder.class.getSimpleName();
        ClientResponse cresponse = client.callProcedure(procName, params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
    }
    
    /**
     * testAdHocSQL
     */
    public void testAdHocSQL() throws Exception {
        // This was originally from org.voltdb.TestAdHocQueries
        
        Client client = this.getClient();
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        ClientResponse cr;
        
        cr = client.callProcedure(procName, "INSERT INTO NEW_ORDER VALUES (1, 1, 1);");
        VoltTable modCount = cr.getResults()[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);

        cr = client.callProcedure(procName, "SELECT * FROM NEW_ORDER;");
        VoltTable result = cr.getResults()[0];
        assertTrue(result.getRowCount() == 1);
        // System.out.println(result.toString());

        boolean caught = false;
        try {
            client.callProcedure("@AdHoc", "SLEECT * FROOM NEEEW_OOORDERERER;");
        } catch (Exception e) {
            caught = true;
        }
        assertTrue("Bad SQL failed to throw expected exception", caught);
    }
    
    /**
     * testTransactionRedirect
     */
    public void testTransactionRedirect() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        if (catalogContext.numberOfPartitions == 1) return;
        
        // TODO
    }
    
    /**
     * testNetworkThreadInitialization
     */
    public void testNetworkThreadInitialization() throws Exception {
        // Test transaction execution where the network processing threads are 
        // responsible for initializing the transactions.
        Client client = this.getClient();
        // RegressionSuiteUtil.setHStoreConf(client, "site.network_txn_initialization", true);
        this.executeTestWorkload(client);
    }
    
    /**
     * testStoredProcedureInvocationHints
     */
    public void testStoredProcedureInvocationHints() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client, true);
        
        final int repeat = 100;
        final StoredProcedureInvocationHints hints = new StoredProcedureInvocationHints();
        final ProcedureCallback callbacks[] = new ProcedureCallback[catalogContext.numberOfPartitions];
        final CountDownLatch latch = new CountDownLatch(catalogContext.numberOfPartitions * repeat);
        for (int p = 0; p < catalogContext.numberOfPartitions; p++) {
            final int partition = p;
            callbacks[p] = new ProcedureCallback() {
                @Override
                public void clientCallback(ClientResponse cresponse) {
                    assertEquals(Status.OK, cresponse.getStatus());
                    assertEquals(partition, cresponse.getBasePartition());
                    latch.countDown();
                }
            };
        } // FOR
        
        for (int i = 0; i < 100; i++) {
            for (int p = 0; p < catalogContext.numberOfPartitions; p++) {
                hints.basePartition = p;
                
                // Once with a callback
                client.callProcedure(callbacks[p], "GetItem", hints, 1);
                
                // And once without a callback
                ClientResponse cresponse = client.callProcedure("GetItem", hints, 1);
                assertNotNull(cresponse);
                assertEquals(Status.OK, cresponse.getStatus());
                assertEquals(p, cresponse.getBasePartition());
            } // FOR
        } // FOR
        
        latch.await();
    }
    
    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestHStoreSiteSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addAllDefaults();
        project.addStmtProcedure("GetItem", "SELECT * FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?");
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition
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
