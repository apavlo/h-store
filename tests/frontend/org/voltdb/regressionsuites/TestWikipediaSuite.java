package org.voltdb.regressionsuites;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.AdHoc;

import edu.brown.benchmark.wikipedia.WikipediaClient;
import edu.brown.benchmark.wikipedia.WikipediaLoader;
import edu.brown.benchmark.wikipedia.WikipediaProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;

/**
 * Simple test suite for the SEATS benchmark
 * @author pavlo
 */
public class TestWikipediaSuite extends RegressionSuite {
    
    private static final String PREFIX = "wikipedia";
    private static final double SCALEFACTOR = 0.01;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestWikipediaSuite(String name) {
        super(name);
    }
    
    
    public WikipediaLoader initWikipediaDatabase(final CatalogContext catalogContext, final Client client) throws Exception {
        HStoreConf hstore_conf = HStoreConf.singleton();
        String args[] = {
            "NOCONNECTIONS=true",
            "CLIENT.SCALEFACTOR=" + hstore_conf.client.scalefactor, 
        };
        WikipediaLoader loader = new WikipediaLoader(args) {
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
        return (loader);
    }
    
    public WikipediaClient initWikipediaClient(final CatalogContext catalogContext, final Client client) throws Exception {
        
        HStoreConf hstore_conf = HStoreConf.singleton();
        String args[] = {
            "NOCONNECTIONS=true",
            "CLIENT.SCALEFACTOR=" + hstore_conf.client.scalefactor, 
        };
        WikipediaClient benchmarkClient = new WikipediaClient(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
        };
        return (benchmarkClient);
    }
    
    
    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        this.initWikipediaDatabase(this.getCatalogContext(), client);
        
        Set<String> allTables = new HashSet<String>();
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        for (String tableName : allTables) {
            String query = "SELECT COUNT(*) FROM " + tableName;
            ClientResponse cresponse = client.callProcedure(procName, query);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            long count = results[0].asScalarLong();
            assertTrue(tableName + " -> " + count, count > 0);
            // System.err.println(tableName + "\n" + results[0]);
        } // FOR
    }

    
    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestWikipediaSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);

        // build up a project builder for the benchmark
        WikipediaProjectBuilder project = new WikipediaProjectBuilder();
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
//        config = new LocalSingleProcessServer(PREFIX + "-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);
//
//        ////////////////////////////////////////////////////////////
//        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
//        ////////////////////////////////////////////////////////////
//        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);

        return builder;
    }

}
