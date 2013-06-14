package org.voltdb.regressionsuites;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.sysprocs.AdHoc;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

/**
 * Simple test suite for the TM1 benchmark
 * @author pavlo
 */
public class TestMarkovSuite extends RegressionSuite {
    
    private static final String PREFIX = "markov";
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestMarkovSuite(String name) {
        super(name);
    }
    
    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(this.getCatalogContext(), client);
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        for (String tableName : TPCCConstants.TABLENAMES) {
            String query = "SELECT COUNT(*) FROM " + tableName;
            ClientResponse cresponse = client.callProcedure(procName, query);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            long count = results[0].asScalarLong();
            assertTrue(tableName + " -> " + count, count > 0);
            // System.err.println(tableName + "\n" + VoltTableUtil.format(results[0]));
        } // FOR
    }
    
    /**
     * testSinglePartitionCaching
     */
    public void testSinglePartitionCaching() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client);

        // Enable the feature on the server
        RegressionSuiteUtil.setHStoreConf(client, "site.markov_path_caching", true);
        
        // Fire off a single-partition txn
        // It should always come back with zero restarts
        String procName = neworder.class.getSimpleName();
        Object params[] = RegressionSuiteUtil.generateNewOrder(catalogContext.numberOfPartitions, false, (short)1);
        ClientResponse cresponse = client.callProcedure(procName, params);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        assertTrue(cresponse.toString(), cresponse.isSinglePartition());
        assertEquals(cresponse.toString(), 0, cresponse.getRestartCounter());
        
        // Sleep a little bit to give them for the txn to get cleaned up
        ThreadUtil.sleep(2500);
        
        // Then execute the same thing again multiple times.
        // It should use the cache estimate from the first txn
        // We are going to execute them asynchronously to check whether we
        // can share the cache properly
        final int num_invocations = 10;
        final CountDownLatch latch = new CountDownLatch(num_invocations);
        final List<ClientResponse> cresponses = new ArrayList<ClientResponse>();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                cresponses.add(clientResponse);
                latch.countDown();
            }
        };
        for (int i = 0; i < num_invocations; i++) {
            client.callProcedure(callback, procName, params);
        } // FOR
        
        // Now wait for the responses
        boolean result = latch.await(5, TimeUnit.SECONDS);
        assertTrue(result);
        
        for (ClientResponse cr : cresponses) {
            assertEquals(cr.toString(), Status.OK, cr.getStatus());
            assertTrue(cr.toString(), cr.isSinglePartition());
            assertEquals(cr.toString(), 0, cr.getRestartCounter());
        } // FOR
        
        // So we need to grab the MarkovEstimatorProfiler stats and check 
        // that the cache counter is greater than one
        cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.MARKOVPROFILER);
        VoltTable results[] = cresponse.getResults();
        long cached_cnt = 0;
        boolean found = false;
        String targetCol = "CACHED_ESTIMATE_CNT";
        while (results[0].advanceRow()) {
            for (int i = 0; i < results[0].getColumnCount(); i++) {
                String col = results[0].getColumnName(i).toUpperCase();
                if (col.equalsIgnoreCase(targetCol)) {
                    cached_cnt += results[0].getLong(i);
                    found = true;
                }
            } // FOR
        } // WHILE
        System.err.println(VoltTableUtil.format(results[0]));
        assertTrue("Missing " + targetCol, found);
        assertEquals(targetCol, num_invocations, cached_cnt);
    }
    
    /**
     * testDistributedTxn
     */
    public void testDistributedTxn() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext, client);

        // Fire off a distributed neworder txn
        // It should always come back with zero restarts
        String procName = neworder.class.getSimpleName();
        Object params[] = RegressionSuiteUtil.generateNewOrder(catalogContext.numberOfPartitions, true, (short)2);
        
        ClientResponse cresponse = client.callProcedure(procName, params);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        assertFalse(cresponse.toString(), cresponse.isSinglePartition());
        assertEquals(cresponse.toString(), 0, cresponse.getRestartCounter());
//        System.err.println(cresponse);
        
        // Get the MarkovEstimatorProfiler stats
        cresponse = RegressionSuiteUtil.getStats(client, SysProcSelector.MARKOVPROFILER);
        System.err.println(VoltTableUtil.format(cresponse.getResults()[0]));
    }
    
    public static Test suite() throws Exception {
        File mappings = ParametersUtil.getParameterMappingsFile(ProjectType.TPCC);
        File markovs = new File("files/markovs/tpcc-2p.markov.gz"); // HACK
        
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMarkovSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", RegressionSuiteUtil.SCALEFACTOR);
        builder.setGlobalConfParameter("site.specexec_enable", true);
        builder.setGlobalConfParameter("site.specexec_ignore_all_local", false);
        builder.setGlobalConfParameter("site.markov_enable", true);
        builder.setGlobalConfParameter("site.markov_profiling", true);
        builder.setGlobalConfParameter("site.markov_path_caching", false);
        builder.setGlobalConfParameter("site.markov_path", markovs.getAbsolutePath());
        builder.setGlobalConfParameter("site.cpu_affinity", false);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addParameterMappings(mappings);
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX + "-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #2: Cluster of 2 sites each with 1 partition
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 1, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
