package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.SetConfiguration;

import edu.brown.benchmark.tm1.TM1Client;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for HStoreSite features
 * @author pavlo
 */
public class TestHStoreSiteSuite extends RegressionSuite {
    
    private static final String PREFIX = "hstoresite";
    private static final double SCALEFACTOR = 0.0001;
    private static final long NUM_SUBSCRIBERS = (long)(SCALEFACTOR * TM1Constants.SUBSCRIBER_SIZE);
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestHStoreSiteSuite(String name) {
        super(name);
    }
    
    /**
     * testNetworkThreadInitialization
     */
    public void testNetworkThreadInitialization() throws Exception {
        Client client = this.getClient();
        
        // Enable the feature on the server
        String procName = VoltSystemProcedure.procCallName(SetConfiguration.class);
        String confParams[] = {"site.network_txn_initialization"};
        String confValues[] = {"true"};
        ClientResponse cresponse = client.callProcedure(procName, confParams, confValues);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        
        TestTM1Suite.initializeTM1Database(this.getCatalog(), client);
        TM1Client.Transaction txn = Transaction.UPDATE_LOCATION;
        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
        cresponse = client.callProcedure(txn.callName, params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
    }

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestHStoreSiteSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);

        // build up a project builder for the TPC-C app
        TM1ProjectBuilder project = new TM1ProjectBuilder();
        project.addAllDefaults();
        
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
