package org.voltdb.regressionsuites;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseDebug;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.specexecprocs.Sleeper;
import org.voltdb.sysprocs.MarkovUpdate;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.mappings.ParametersUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class TestMarkovSpecExecSuite extends RegressionSuite {
    
    private static final String PREFIX = "markovspecexec";
    private static final double SCALEFACTOR = 0.0001;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestMarkovSpecExecSuite(String name) {
        super(name);
    }
    
    /**
     * testRemoteQueryEstimates
     */
    public void testRemoteQueryEstimates() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext.catalog, client);
        int w_id = 1;
        int d_id = 1;

        // We have to turn off caching to ensure that we get a full path estimate each time
        RegressionSuiteUtil.setHStoreConf(client, "site.markov_path_caching", false);
        
        // Check to make sure that the system sends query estimates to remote sites for dtxns
        // This may not work correctly for one site tests.
        // We need to execute the same request multiple times to ensure that the MarkovGraph
        // has all of the vertices that we need. We'll then invoke @MarkovUpdate and then
        // request the txn one more time
        String procName = neworder.class.getSimpleName();
        Object params[] = RegressionSuiteUtil.generateNewOrder(catalogContext.numberOfPartitions, true, w_id, d_id);
        ClientResponse cr = null;
        int repeat = 2;
        for (int i = 0; i < repeat; i++) {
            cr = client.callProcedure(procName, params);
            assertFalse(cr.toString(), cr.isSinglePartition());
            // System.err.println(cr);
            
            if (i == 0) {
                // Sleep for a little bit to make sure that the MarkovEstimatorState is cleaned up
                ThreadUtil.sleep(1000);
                cr = client.callProcedure(VoltSystemProcedure.procCallName(MarkovUpdate.class), false);
                // System.err.println(cr);
                assertEquals(cr.toString(), Status.OK, cr.getStatus());
            }
        } // FOR
        assertTrue(cr.hasDebug());
        
        ClientResponseDebug crDebug = cr.getDebug();
        assertFalse(crDebug.toString(), crDebug.isPredictSinglePartition());
        assertFalse(crDebug.toString(), crDebug.isSpeculative());
        assertEquals(crDebug.toString(), crDebug.getPredictTouchedPartitions(), crDebug.getExecTouchedPartitions());
        assertEquals(crDebug.toString(), crDebug.getPredictTouchedPartitions(), crDebug.getExecTouchedPartitions());
        
        Procedure catalog_proc = catalogContext.procedures.getIgnoreCase(procName);
        Set<Statement> expectedStmts = new HashSet<Statement>();
        expectedStmts.add(catalog_proc.getStatements().getIgnoreCase("getStockInfo"));
        expectedStmts.add(catalog_proc.getStatements().getIgnoreCase("updateStock"));
        
        for (int partition : crDebug.getExecTouchedPartitions()) {
            List<CountedStatement> query_estimates[] = crDebug.getRemoteEstimates(catalogContext, partition);
            assertNotNull(query_estimates);

            System.err.println("PARTITION: " + partition);
            for (List<CountedStatement> queries : query_estimates) {
                System.err.println(StringUtil.join("\n", queries));
                System.err.println("------");
            } // FOR
            System.err.println();

            if (partition == cr.getBasePartition()) {
                // We can ignore anything on the base partition
            } else {
                // We should only see updateStock and getStockInfo on the remote partition
                // We'll only examine the last query estimate
                Set<CountedStatement> seenQueries = new HashSet<CountedStatement>();
                for (CountedStatement cs : CollectionUtil.last(query_estimates)) {
                    assertTrue(cs.toString(), expectedStmts.contains(cs.statement));
                    assertFalse(cs.toString(), seenQueries.contains(cs));
                    seenQueries.add(cs);
                } // FOR
                assertFalse(seenQueries.isEmpty());
            }
        } // FOR
    }
    
    /**
     * testRemoteIdle
     */
    public void testRemoteIdle() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext.catalog, client);
        
        int w_id = 1;
        int d_id = 1;
        ClientResponse cresponse = null;
        ClientResponseDebug crDebug = null;
        String procName = null;
        Object params[] = null;
        
        // First execute a single-partition txn that will sleep for a while at the 
        // partition that we're going to need to execute our dtxn on.
        // This will give us time to queue up a bunch of stuff to ensure that the 
        // SpecExecScheduler has stuff to look at when the PartitionExecutor is idle.
        final int sleepBefore = 5000; // ms
        final int sleepAfter = 5000; // ms
        procName = Sleeper.class.getSimpleName();
        params = new Object[]{ w_id+1, sleepBefore, sleepAfter };
        client.callProcedure(new NullCallback(), procName, params);
        
        // Now fire off a distributed NewOrder transaction
        final List<ClientResponse> dtxnResponse = new ArrayList<ClientResponse>();
        final ProcedureCallback dtxnCallback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                // System.err.println("DISTRUBTED RESULT " + clientResponse);
                dtxnResponse.add(clientResponse);
            }
        };
        procName = neworder.class.getSimpleName();
        params = RegressionSuiteUtil.generateNewOrder(catalogContext.numberOfPartitions, true, w_id, d_id);
        client.callProcedure(dtxnCallback, procName, params);
        long start = System.currentTimeMillis();
        
        // While we're waiting for that to come back, we're going to fire off 
        // a bunch of single-partition neworder txns that should all be executed
        // speculatively at the other partition
        final List<ClientResponse> spResponse = new ArrayList<ClientResponse>();
        final AtomicInteger spLatch = new AtomicInteger(0);
        final ProcedureCallback spCallback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                spResponse.add(clientResponse);
                spLatch.decrementAndGet();
            }
        };
        while (dtxnResponse.isEmpty()) {
            // Just sleep for a little bit so that we don't blast the cluster
            ThreadUtil.sleep(1000);
            
            spLatch.incrementAndGet();
            params = RegressionSuiteUtil.generateNewOrder(catalogContext.numberOfPartitions, true, w_id, d_id+1);
            client.callProcedure(spCallback, procName, params);
            
            // We'll only check the txns half way through the dtxns expected
            // sleep time
            long elapsed = System.currentTimeMillis() - start;
            assert(elapsed <= (sleepBefore+sleepAfter)*2);
        } // WHILE 

        cresponse = CollectionUtil.first(dtxnResponse);
        assertNotNull(cresponse);
        assertFalse(cresponse.isSinglePartition());
        assertTrue(cresponse.hasDebug());
        assertFalse(cresponse.getDebug().isSpeculative());
        
        // Spin and wait for the single-p txns to finish
        while (spLatch.get() > 0) {
            long elapsed = System.currentTimeMillis() - start;
            assert(elapsed <= (sleepBefore+sleepAfter)*3);
            ThreadUtil.sleep(500);
        } // WHILE
        // assert(spResponse.size() > 0);
        
        // Now we should check to see the txns were not speculative, then when they 
        // are speculative, afterwards none should be speculative
        boolean first_spec = false;
        boolean last_spec = false;
        Histogram<Boolean> specExecHistogram = new Histogram<Boolean>(); 
        for (ClientResponse cr : spResponse) {
            assertTrue(cr.hasDebug());
            crDebug = cr.getDebug();
            specExecHistogram.put(crDebug.isSpeculative());
            if (crDebug.isSpeculative()) {
                if (first_spec == false) {
                    first_spec = true;
                }
            } else {
                if (last_spec == false) {
                    last_spec = true;
                }
            }
        } // FOR
        System.err.println(specExecHistogram);
    }

    public static Test suite() throws Exception {
        File mappings = ParametersUtil.getParameterMappingsFile(ProjectType.TPCC);
        
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMarkovSpecExecSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);
        builder.setGlobalConfParameter("site.network_startup_wait", 30000);
        builder.setGlobalConfParameter("site.txn_client_debug", true);
        builder.setGlobalConfParameter("site.specexec_enable", true);
        builder.setGlobalConfParameter("site.specexec_idle", true);
        builder.setGlobalConfParameter("site.specexec_ignore_all_local", false);
        builder.setGlobalConfParameter("site.specexec_markov", true);
        builder.setGlobalConfParameter("site.network_txn_initialization", true);
        builder.setGlobalConfParameter("site.markov_enable", true);
        builder.setGlobalConfParameter("site.markov_profiling", true);
        builder.setGlobalConfParameter("site.markov_path_caching", true);
        builder.setGlobalConfParameter("site.markov_fast_path", true);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addParameterMappings(mappings);
        project.addProcedure(Sleeper.class);
        
        boolean success;
        VoltServerConfig config = null;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX + "-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config.setConfParameter("site.markov_path", new File("files/markovs/vldb-august2012/tpcc-2p.markov.gz").getAbsolutePath());
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #2: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        config.setConfParameter("site.markov_path", new File("files/markovs/vldb-august2012/tpcc-4p.markov.gz").getAbsolutePath());
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
