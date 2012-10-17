package org.voltdb.regressionsuites;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Test;

import org.voltdb.regressionsuites.TestTM1Suite;
import org.voltdb.BackendTarget;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.specexecprocs.RemoteIdle;
import org.voltdb.regressionsuites.specexecprocs.UpdateAll;
import org.voltdb.regressionsuites.specexecprocs.UpdateOne;
import org.voltdb.sysprocs.AdHoc;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Client;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.TM1Loader;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
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
    private static final double SCALEFACTOR = 0.0001;
    
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
        RegressionSuiteUtil.initializeTPCCDatabase(this.getCatalog(), client);
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        for (String tableName : TPCCConstants.TABLENAMES) {
            String query = "SELECT COUNT(*) FROM " + tableName;
            ClientResponse cresponse = client.callProcedure(procName, query);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            long count = results[0].asScalarLong();
            assertTrue(tableName + " -> " + count, count > 0);
            System.err.println(tableName + "\n" + results[0]);
        } // FOR
    }
    
//    /**
//     * testDistributedTxn
//     */
//    public void testDistributedTxn() throws Exception {
//        Client client = this.getClient();
//        TestMarkovSuite.initializeTPCCDatabase(this.getCatalog(), client);
//        
//        // Submit a distributed txn and make sure that our conflicting
//        // txn is not speculatively executed
//        final int sleepTime = 5000; // ms
//        final ClientResponse dtxnResponse[] = new ClientResponse[1];
//        final CountDownLatch dtxnLatch = new CountDownLatch(1);
//        final ProcedureCallback dtxnCallback = new ProcedureCallback() {
//            @Override
//            public void clientCallback(ClientResponse clientResponse) {
//                System.err.println("DISTRUBTED RESULT " + clientResponse);
//                dtxnResponse[0] = clientResponse;
//                dtxnLatch.countDown();
//            }
//        };
//        
//        // We're going to first execute a dtxn that updates all SUBSCRIBER records
//        String dtxnProcName = UpdateAll.class.getSimpleName();
//        Object dtxnParams[] = { 0, sleepTime };
//        client.callProcedure(dtxnCallback, dtxnProcName, dtxnParams);
//        
//        // Then fire off a proc that updates SUBSCRIBER as well. This should never
//        // be allowed to execute speculatively
//        String spProcName = UpdateOne.class.getSimpleName();
//        Object spParams[] = new Object[]{ 1 };
//        final ClientResponse spResponse[] = new ClientResponse[1];
//        final CountDownLatch spLatch = new CountDownLatch(1);
//        final ProcedureCallback spCallback = new ProcedureCallback() {
//            @Override
//            public void clientCallback(ClientResponse clientResponse) {
//                System.err.println("SINGLE-PARTITION RESULT " + clientResponse);
//                spResponse[0] = clientResponse;
//                spLatch.countDown();
//            }
//        };
//        ThreadUtil.sleep(1000);
//        client.callProcedure(spCallback, spProcName, spParams);
//        
//        // Wait until we have both latches
//        dtxnLatch.await(sleepTime*2, TimeUnit.MILLISECONDS);
//        spLatch.await(sleepTime*2, TimeUnit.MILLISECONDS);
//        
//        // Then verify the DTXN results
//        assertNotNull(dtxnResponse[0]);
//        assertEquals(Status.OK, dtxnResponse[0].getStatus());
//        assertFalse(dtxnResponse[0].isSinglePartition());
//        assertFalse(dtxnResponse[0].isSpeculative());
//        
//        // And the SP results. Where is your god now?
//        assertNotNull(spResponse[0]);
//        assertEquals(Status.OK, spResponse[0].getStatus());
//        assertTrue(spResponse[0].isSinglePartition());
//        assertFalse(spResponse[0].isSpeculative());
//        
//        // SANITY CHECK
//        // We should have exaclty two different MSC_LOCATION values
//        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
//        String query = "SELECT COUNT(DISTINCT MSC_LOCATION) FROM " + TM1Constants.TABLENAME_SUBSCRIBER;
//        ClientResponse cresponse = client.callProcedure(procName, query);
//        assertEquals(Status.OK, cresponse.getStatus());
//        assertEquals(2, cresponse.getResults()[0].asScalarLong());
//        System.err.println(cresponse);
//    }
    
    public static Test suite() throws Exception {
        File mappings = ParametersUtil.getParameterMappingsFile(ProjectType.TPCC);
        File markovs = new File("files/markovs/vldb-august2012/tpcc-2p.markov.gz"); // HACK
        
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMarkovSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);
        builder.setGlobalConfParameter("site.specexec_enable", true);
        builder.setGlobalConfParameter("site.specexec_idle", true);
        builder.setGlobalConfParameter("site.specexec_ignore_all_local", false);
        builder.setGlobalConfParameter("site.network_txn_initialization", true);
        builder.setGlobalConfParameter("site.markov_enable", true);
        builder.setGlobalConfParameter("site.markov_path", markovs.getAbsolutePath());

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
        // CONFIG #2: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
//        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);

        return builder;
    }

}
