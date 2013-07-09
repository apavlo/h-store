package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.AdHoc;

import edu.brown.benchmark.tm1.TM1Client;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for the TM1 benchmark
 * @author pavlo
 */
public class TestTM1Suite extends RegressionSuite {
    
    private static final String PREFIX = "tm1";
    private static final double SCALEFACTOR = 0.0001;
    private static final long NUM_SUBSCRIBERS = (long)(SCALEFACTOR * TM1Constants.SUBSCRIBER_SIZE);
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestTM1Suite(String name) {
        super(name);
    }
    
    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        for (String tableName : TM1Constants.TABLENAMES) {
            String query = "SELECT COUNT(*) FROM " + tableName;
            ClientResponse cresponse = client.callProcedure(procName, query);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            long count = results[0].asScalarLong();
            if (tableName.equals(TM1Constants.TABLENAME_SUBSCRIBER)) {
                assertEquals(tableName,  NUM_SUBSCRIBERS, count);
            } else {
                assertTrue(tableName + " -> " + count, count > 0);
            }
            System.err.println(tableName + "\n" + results[0]);
        } // FOR
    }
    
    /**
     * testDeleteCallForwarding
     */
    public void testDeleteCallForwarding() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        TM1Client.Transaction txn = Transaction.DELETE_CALL_FORWARDING;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        
        for (int i = 0; i < 1000; i++) {
            ClientResponse cresponse = null;
            try {
                cresponse = client.callProcedure(txn.callName, params);
                assertEquals(Status.OK, cresponse.getStatus());
            } catch (ProcCallException ex) {
                cresponse = ex.getClientResponse();
//                System.err.println();
                assertEquals(cresponse.toString(), Status.ABORT_USER, cresponse.getStatus());
            }
            assertNotNull(cresponse);
        } // FOR
    }
    
    /**
     * testGetAccessData
     */
    public void testGetAccessData() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        TM1Client.Transaction txn = Transaction.GET_ACCESS_DATA;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        ClientResponse cresponse = client.callProcedure(txn.callName, params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        assertTrue(cresponse.toString(), cresponse.isSinglePartition());
    }
    
    /**
     * testGetNewDestination
     */
    public void testGetNewDestination() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        TM1Client.Transaction txn = Transaction.DELETE_CALL_FORWARDING;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        ClientResponse cresponse = null;
        try {
            cresponse = client.callProcedure(txn.callName, params);
            assertEquals(Status.OK, cresponse.getStatus());
        } catch (ProcCallException ex) {
            cresponse = ex.getClientResponse();
            assertEquals(cresponse.toString(), Status.ABORT_USER, cresponse.getStatus());
        }
        assertNotNull(cresponse);
        assertTrue(cresponse.toString(), cresponse.isSinglePartition());
    }
    
    /**
     * testGetSubscriberData
     */
    public void testGetSubscriberData() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        TM1Client.Transaction txn = Transaction.GET_SUBSCRIBER_DATA;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        ClientResponse cresponse = client.callProcedure(txn.callName, params);
        assertNotNull(cresponse);
        assertTrue(cresponse.toString(), cresponse.isSinglePartition());
    }
    
    /**
     * testInsertCallForwarding
     */
    public void testInsertCallForwarding() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        TM1Client.Transaction txn = Transaction.INSERT_CALL_FORWARDING;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        ClientResponse cresponse = null;
        try {
            cresponse = client.callProcedure(txn.callName, params);
            assertEquals(Status.OK, cresponse.getStatus());
        } catch (ProcCallException ex) {
            cresponse = ex.getClientResponse();
            assertEquals(Status.ABORT_USER, cresponse.getStatus());
        }
        assertNotNull(cresponse);
    }
    
    /**
     * testUpdateLocation
     */
    public void testUpdateLocation() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        TM1Client.Transaction txn = Transaction.UPDATE_LOCATION;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        ClientResponse cresponse = client.callProcedure(txn.callName, params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
    }
    
    /**
     * testUpdateSubscriberData
     */
    public void testUpdateSubscriberData() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
        TM1Client.Transaction txn = Transaction.UPDATE_SUBSCRIBER_DATA;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        ClientResponse cresponse = null;
        try {
            cresponse = client.callProcedure(txn.callName, params);
            assertEquals(Status.OK, cresponse.getStatus());
        } catch (ProcCallException ex) {
            cresponse = ex.getClientResponse();
            assertEquals(Status.ABORT_USER, cresponse.getStatus());
        }
        assertNotNull(cresponse);
        assertTrue(cresponse.toString(), cresponse.isSinglePartition());
    }

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestTM1Suite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);

        // build up a project builder for the TPC-C app
        TM1ProjectBuilder project = new TM1ProjectBuilder();
        project.addAllDefaults();
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX+"-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
