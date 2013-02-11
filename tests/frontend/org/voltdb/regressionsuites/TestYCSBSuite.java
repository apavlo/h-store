package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import edu.brown.benchmark.tm1.TM1Client;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBLoader;
import edu.brown.benchmark.ycsb.YCSBProjectBuilder;
import edu.brown.benchmark.ycsb.procedures.ReadRecord;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for the YCSB benchmark
 * @author pavlo
 */
public class TestYCSBSuite extends RegressionSuite {

    private static final String PREFIX = "ycsb";
    private static final int NUM_TUPLES = 1000;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestYCSBSuite(String name) {
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
        YCSBLoader loader = new YCSBLoader(args) {
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
        
        String query = "SELECT COUNT(*) FROM " + YCSBConstants.TABLE_NAME;
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
    public void testReadRecord() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client, NUM_TUPLES);
        
        long key = NUM_TUPLES / 2;
        String procName = ReadRecord.class.getSimpleName();
        Object params[] = { key };
        ClientResponse cresponse = client.callProcedure(procName, params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        assertEquals(1, cresponse.getResults().length);
        
        VoltTable vt = cresponse.getResults()[0];
        boolean adv = vt.advanceRow();
        assert(adv);
        assertEquals(key, vt.getLong(0));
    }
    

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestYCSBSuite.class);

        YCSBProjectBuilder project = new YCSBProjectBuilder();
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
