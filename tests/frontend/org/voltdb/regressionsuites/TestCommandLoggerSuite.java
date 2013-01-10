package org.voltdb.regressionsuites;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.benchmark.voter.VoterConstants;
import edu.brown.benchmark.voter.VoterProjectBuilder;
import edu.brown.benchmark.voter.procedures.Initialize;
import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Test suite for the CommandLogger using the VOTER benchmark
 * @author pavlo
 */
public class TestCommandLoggerSuite extends RegressionSuite {

    private static final String PREFIX = "commandlog";
    private static final long PHONE_NUMBER = 8675309; // Jenny
    private static final long MAX_VOTES_PER_PHONE_NUMBER = 5;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestCommandLoggerSuite(String name) {
        super(name);
    }
    
    private void initializeDatabase(Client client) throws Exception {
        Object params[] = {
            VoterConstants.NUM_CONTESTANTS,
            VoterConstants.CONTESTANT_NAMES_CSV
        };
        
        System.err.println("Initializing database...");
        ClientResponse cresponse = client.callProcedure(Initialize.class.getSimpleName(), params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
    }

    /**
     * testConcurrentTxns
     */
    public void testConcurrentTxns() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        final int num_txns = 500;
        final CountDownLatch latch = new CountDownLatch(num_txns);
        final AtomicInteger numTotal = new AtomicInteger(0);
        final AtomicInteger numCompleted = new AtomicInteger(0);
        final AtomicInteger numFailed = new AtomicInteger(0);
        
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse cr) {
                switch (cr.getStatus()) {
                    case OK:
                        if (cr.getResults()[0].asScalarLong() == VoterConstants.VOTE_SUCCESSFUL) { 
                            numCompleted.incrementAndGet();
                        }
                        break;
                    default:
                        numFailed.incrementAndGet();
                        System.err.printf("UNEXPECTED: %s [%d]\n", cr.getStatus(), numFailed.get());
                } // SWITCH
                numTotal.incrementAndGet();
                latch.countDown();
            }
        };
        
        for (int i = 0; i < num_txns; i++) {
            Object params[] = {
                new Long(i),
                PHONE_NUMBER + (i % 10),
                i % VoterConstants.NUM_CONTESTANTS,
                MAX_VOTES_PER_PHONE_NUMBER
            };
            client.callProcedure(callback, Vote.class.getSimpleName(), params);
        } // FOR
        System.err.printf("Invoked %d txns. Waiting for responses...\n", num_txns);
        boolean result = latch.await(15, TimeUnit.SECONDS);
        System.err.println("LATCH RESULT: " + result);
        System.err.println("TOTAL:        " + numTotal.get() + " / " + num_txns);
        System.err.println("COMPLETED:    " + numCompleted.get());
        System.err.println("FAILED:       " + numFailed.get());
        if (result == false) {
            System.err.println("BAD MOJO");
        }
        
        assertTrue("Timed out [latch="+latch.getCount() + "]", result);
        assertEquals(0, numFailed.get());
        
        // At this point we know that all of our txns have been committed to disk
        // Make sure that our vote is actually in the real table and materialized views
        String query = "SELECT COUNT(*) FROM votes";
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        System.err.println(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(numCompleted.get(), results[0].asScalarLong());
        
        
        // TODO: We should go through the log and make sure that all of our
        // transactions are still in there...
    }

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCommandLoggerSuite.class);
        builder.setGlobalConfParameter("site.commandlog_enable", true);
        builder.setGlobalConfParameter("site.commandlog_timeout", 1000);
        builder.setGlobalConfParameter("site.status_enable", true);
        builder.setGlobalConfParameter("site.status_exec_info", true);
        
        VoterProjectBuilder project = new VoterProjectBuilder();
        project.addAllDefaults();
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
//        config = new LocalSingleProcessServer(PREFIX + "-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);
        
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
//        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);

        return builder;
    }

}
