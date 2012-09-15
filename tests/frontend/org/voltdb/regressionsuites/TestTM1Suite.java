package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.AdHoc;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.TM1Loader;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for the TM1 benchmark
 * @author pavlo
 */
public class TestTM1Suite extends RegressionSuite {
    
    private static final double SCALEFACTOR = 0.0001;
    
    private static final String args[] = {
        "NOCONNECTIONS=true",
    };

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestTM1Suite(String name) {
        super(name);
    }
    
    private void initializeDatabase(final Client client) throws Exception {
        TM1Loader loader = new TM1Loader(args) {
            {
                this.setCatalog(TestTM1Suite.this.getCatalog());
                this.setClientHandle(client);
            }
            @Override
            public Catalog getCatalog() {
                return TestTM1Suite.this.getCatalog();
            }
        };
        loader.load();
    }

    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        for (String tableName : TM1Constants.TABLENAMES) {
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
        config = new LocalSingleProcessServer("tm1-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
//        config = new LocalSingleProcessServer("tm1-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);
//
//        ////////////////////////////////////////////////////////////
//        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
//        ////////////////////////////////////////////////////////////
//        config = new LocalCluster("tm1-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);

        return builder;
    }

}
