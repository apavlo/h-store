package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import edu.brown.benchmark.users.UsersConstants;
import edu.brown.benchmark.users.UsersLoader;
import edu.brown.benchmark.users.UsersProjectBuilder;
import edu.brown.benchmark.users.procedures.GetUsers;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for the Users benchmark
 */
public class TestUsersSuite extends RegressionSuite {

    private static final String PREFIX = "users";
    private static final int NUM_TUPLES = 10000;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestUsersSuite(String name) {
        super(name);
    }
    
    private void initializeDatabase(final Client client, final int num_tuples) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "BENCHMARK.FIXED_SIZE=true",
            "BENCHMARK.NUM_RECORDS="+num_tuples,
            "BENCHMARK.LOADTHREADS=1",
        };
        final CatalogContext catalogContext = this.getCatalogContext();
        UsersLoader loader = new UsersLoader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
        };
        loader.load();
    }

    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client, NUM_TUPLES);
        
        String query = "SELECT COUNT(*) FROM " + UsersConstants.TABLENAME_USERS;
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(NUM_TUPLES, results[0].asScalarLong());
        System.err.println(results[0]);
    }

    /**
     * testReadRecord
     */
    public void testReadRecords() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client, NUM_TUPLES);
        
        String procName = GetUsers.class.getSimpleName();
        ClientResponse cresponse = client.callProcedure(procName);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        assertEquals(NUM_TUPLES, cresponse.getResults()[0].getRowCount());
    }
    

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUsersSuite.class);

        UsersProjectBuilder project = new UsersProjectBuilder();
        project.addAllDefaults();
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #2: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX+"-cluster.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
