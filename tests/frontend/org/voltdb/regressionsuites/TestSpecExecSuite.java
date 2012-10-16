package org.voltdb.regressionsuites;

import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.specexecprocs.RemoteIdle;

import edu.brown.benchmark.tm1.TM1Client;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tm1.TM1Loader;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.ThreadUtil;

/**
 * Simple test suite for the TM1 benchmark
 * @author pavlo
 */
public class TestSpecExecSuite extends RegressionSuite {
    
    private static final String PREFIX = "specexec";
    private static final double SCALEFACTOR = 0.0001;
    
    private static final String args[] = {
        "NOCONNECTIONS=true",
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSpecExecSuite(String name) {
        super(name);
    }
    
    private void initializeDatabase(final Client client) throws Exception {
        TM1Loader loader = new TM1Loader(args) {
            {
                this.setCatalog(TestSpecExecSuite.this.getCatalog());
                this.setClientHandle(client);
            }
            @Override
            public Catalog getCatalog() {
                return TestSpecExecSuite.this.getCatalog();
            }
        };
        loader.load();
    }
    
    /**
     * testRemoteIdle
     */
    public void testRemoteIdle() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        final int sleepTime = 10000; // ms
        final ClientResponse dtxnResponse[] = new ClientResponse[1];
        final AtomicBoolean latch = new AtomicBoolean(true);
        final ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                System.err.println("DISTRUBTED RESULT " + clientResponse);
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
            
            System.err.println("Requesting " + txn + " for speculative execution");
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
            }
            if (cresponse.isSpeculative()) specexec_ctr++;
        } // WHILE 
        assert(specexec_ctr > 0);

        cresponse = dtxnResponse[0];
        assertNotNull(cresponse);
        assertFalse(cresponse.isSinglePartition());
        assertFalse(cresponse.isSpeculative());
    }

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSpecExecSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);
        builder.setGlobalConfParameter("site.specexec_enable", true);
        builder.setGlobalConfParameter("site.specexec_idle", true);

        // build up a project builder for the TPC-C app
        TM1ProjectBuilder project = new TM1ProjectBuilder();
        project.addAllDefaults();
        project.addProcedure(RemoteIdle.class);
        
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
