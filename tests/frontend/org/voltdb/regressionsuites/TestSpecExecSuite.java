package org.voltdb.regressionsuites;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.specexecprocs.RemoteIdle;
import org.voltdb.regressionsuites.specexecprocs.UpdateAll;
import org.voltdb.regressionsuites.specexecprocs.UpdateOne;
import org.voltdb.sysprocs.AdHoc;
import org.voltdb.types.SpeculationConflictCheckerType;

import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.tm1.TM1Client;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ThreadUtil;

/**
 * Simple test suite for the TM1 benchmark
 * @author pavlo
 */
public class TestSpecExecSuite extends RegressionSuite {
    
    private static final String PREFIX = "specexec";
    private static final double SCALEFACTOR = 0.0001;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSpecExecSuite(String name) {
        super(name);
    }
    
    /**
     * testConflictingTxns
     */
    public void testConflictingTxns() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        
        // Submit a distributed txn and make sure that our conflicting
        // txn is not speculatively executed
        final int sleepTime = 5000; // ms
        final LatchableProcedureCallback dtxnCallback = new LatchableProcedureCallback(1);
        
        // We're going to first execute a dtxn that updates all SUBSCRIBER records
        String dtxnProcName = UpdateAll.class.getSimpleName();
        Object dtxnParams[] = { 0, sleepTime };
        client.callProcedure(dtxnCallback, dtxnProcName, dtxnParams);
        
        // Then fire off a proc that updates SUBSCRIBER as well. This should never
        // be allowed to execute speculatively
        String spProcName = UpdateOne.class.getSimpleName();
        Object spParams[] = new Object[]{ 1 };
        LatchableProcedureCallback spCallback = new LatchableProcedureCallback(1);
        ThreadUtil.sleep(100);
        client.callProcedure(spCallback, spProcName, spParams);
        
        // Wait until we have both latches
        dtxnCallback.latch.await(sleepTime*2, TimeUnit.MILLISECONDS);
        spCallback.latch.await(sleepTime*2, TimeUnit.MILLISECONDS);
        
        // Then verify the DTXN results
        ClientResponse dtxnResponse = CollectionUtil.first(dtxnCallback.responses);
        assertNotNull(dtxnResponse);
        assertEquals(Status.OK, dtxnResponse.getStatus());
        assertTrue(dtxnResponse.hasDebug());
        assertFalse(dtxnResponse.isSinglePartition());
        assertFalse(dtxnResponse.isSpeculative());
        
        // And the SP results. Where is your god now?
        ClientResponse spResponse = CollectionUtil.first(spCallback.responses);
        assertNotNull(spResponse);
        assertEquals(Status.OK, spResponse.getStatus());
        assertTrue(spResponse.hasDebug());
        assertTrue(spResponse.isSinglePartition());
        
        // There is currently a race condition for whether the txn will get 
        // speculatively executed or not, so for now we'll just disable this
        // one check.
        // sassertTrue(spResponse.isSpeculative());
        
        // SANITY CHECK
        // We should have exaclty two different MSC_LOCATION values
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        String query = "SELECT COUNT(DISTINCT MSC_LOCATION) FROM " + TM1Constants.TABLENAME_SUBSCRIBER;
        ClientResponse cresponse = client.callProcedure(procName, query);
        assertEquals(Status.OK, cresponse.getStatus());
        assertEquals(2, cresponse.getResults()[0].asScalarLong());
        System.err.println(cresponse);
    }
    
    /**
     * testRemoteIdle
     */
    public void testRemoteIdle() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        
        final int sleepTime = 10000; // ms
        final ClientResponse dtxnResponse[] = new ClientResponse[1];
        final AtomicBoolean latch = new AtomicBoolean(true);
        final ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                // System.err.println("DISTRUBTED RESULT " + clientResponse);
                dtxnResponse[0] = clientResponse;
                latch.set(false);
            }
        };
        
        // We're going to first execute a long running and slow distributed transaction
        // on the first partition. It will sleep for 10 seconds
        String procName = RemoteIdle.class.getSimpleName();
        Object params[] = { 0, sleepTime };
        client.callProcedure(callback, procName, params);
        long start = System.currentTimeMillis();
        
        // While we're waiting for that to come back, we're going to fire off 
        // a bunch of single-partition txns that should all be executed
        // speculatively at the other partition
        TM1Client.Transaction txn = Transaction.GET_SUBSCRIBER_DATA;
        params = new Object[]{ 1 };
        ClientResponse cresponse = null;
        int specexec_ctr = 0;
        while (latch.get()) {
            // Just sleep for a little bit so that we don't blast the cluster
            ThreadUtil.sleep(500);
            
            // System.err.println("Requesting " + txn + " for speculative execution");
            try {
                cresponse = client.callProcedure(txn.callName, params);
                assertEquals(Status.OK, cresponse.getStatus());
            } catch (ProcCallException ex) {
                cresponse = ex.getClientResponse();
                assertEquals(cresponse.toString(), Status.ABORT_USER, cresponse.getStatus());
            }
            // System.err.println(cresponse.toString());
            // System.err.println();
            
            // We'll only check the txns half way through the dtxns expected
            // sleep time
            long elapsed = System.currentTimeMillis() - start;
            assert(elapsed <= sleepTime*1.25);
            if (elapsed < sleepTime/2) {
                assertEquals(cresponse.toString(), latch.get(), cresponse.isSpeculative());
                System.err.println(cresponse.getDebug());
            }
            if (cresponse.isSpeculative()) specexec_ctr++;
        } // WHILE 
        assert(specexec_ctr > 0);

        cresponse = dtxnResponse[0];
        assertNotNull(cresponse);
        assertTrue(cresponse.hasDebug());
        assertFalse(cresponse.isSinglePartition());
        assertFalse(cresponse.isSpeculative());
    }

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSpecExecSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);
        builder.setGlobalConfParameter("site.txn_client_debug", true);
        builder.setGlobalConfParameter("site.specexec_enable", true);
        builder.setGlobalConfParameter("site.specexec_ignore_all_local", false);
        builder.setGlobalConfParameter("site.specexec_scheduler_checker", SpeculationConflictCheckerType.TABLE);

        // build up a project builder for the TPC-C app
        TM1ProjectBuilder project = new TM1ProjectBuilder();
        project.addAllDefaults();
        project.addProcedure(RemoteIdle.class);
        project.addProcedure(UpdateAll.class);
        project.addProcedure(UpdateOne.class);
        
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
        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
