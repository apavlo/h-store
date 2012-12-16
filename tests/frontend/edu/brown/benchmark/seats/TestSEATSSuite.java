package edu.brown.benchmark.seats;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.regressionsuites.LocalSingleProcessServer;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.RegressionSuiteUtil;
import org.voltdb.regressionsuites.VoltServerConfig;
import org.voltdb.sysprocs.AdHoc;

import edu.brown.benchmark.seats.RandomGenerator;
import edu.brown.benchmark.seats.SEATSConstants;
import edu.brown.benchmark.seats.SEATSLoader;
import edu.brown.benchmark.seats.SEATSProfile;
import edu.brown.benchmark.seats.SEATSProjectBuilder;
import edu.brown.benchmark.seats.util.SEATSHistogramUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;

/**
 * Simple test suite for the SEATS benchmark
 * @author pavlo
 */
public class TestSEATSSuite extends RegressionSuite {
    
    private static final String PREFIX = "seats";
    private static final double SCALEFACTOR = 0.01;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSEATSSuite(String name) {
        super(name);
    }
    
    
    public SEATSLoader initializeSEATSDatabase(final CatalogContext catalogContext, final Client client) throws Exception {
        File dataDir = SEATSHistogramUtil.findDataDir();
        assert(dataDir != null);
        
        HStoreConf hstore_conf = HStoreConf.singleton();
        String args[] = {
            "NOCONNECTIONS=true",
            "CLIENT.SCALEFACTOR=" + hstore_conf.client.scalefactor, 
            "BENCHMARK.DATADIR=" + dataDir.getAbsolutePath()
        };
        SEATSLoader loader = new SEATSLoader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public Catalog getCatalog() {
                return (catalogContext.catalog);
            }
        };
        loader.load();
        return (loader);
    }
    
    public SEATSClient initializeSEATSClient(final CatalogContext catalogContext, final Client client) throws Exception {
        File dataDir = SEATSHistogramUtil.findDataDir();
        assert(dataDir != null);
        
        HStoreConf hstore_conf = HStoreConf.singleton();
        String args[] = {
            "NOCONNECTIONS=true",
            "CLIENT.SCALEFACTOR=" + hstore_conf.client.scalefactor, 
            "BENCHMARK.DATADIR=" + dataDir.getAbsolutePath()
        };
        SEATSClient benchmarkClient = new SEATSClient(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public Catalog getCatalog() {
                return (catalogContext.catalog);
            }
        };
        return (benchmarkClient);
    }
    
    
    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        this.initializeSEATSDatabase(this.getCatalogContext(), client);
        
        Set<String> allTables = new HashSet<String>();
        CollectionUtil.addAll(allTables, SEATSConstants.TABLES_SCALING);
        CollectionUtil.addAll(allTables, SEATSConstants.TABLES_DATAFILES);
        
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
    
    /**
     * testSaveLoadProfile
     */
    public void testSaveLoadProfile() throws Exception {
        Client client = this.getClient();
        CatalogContext catalogContext = this.getCatalogContext();
        SEATSLoader loader = this.initializeSEATSDatabase(catalogContext, client);
        
        SEATSProfile orig = loader.getProfile();
        assertNotNull(orig);
        
        SEATSProfile copy = new SEATSProfile(catalogContext.catalog, new RandomGenerator(0));
        assert(copy.airport_histograms.isEmpty());
        copy.loadProfile(client);
        
        assertEquals(orig.scale_factor, copy.scale_factor);
        assertEquals(orig.airport_max_customer_id, copy.airport_max_customer_id);
        assertEquals(orig.flight_start_date.toString(), copy.flight_start_date.toString());
        assertEquals(orig.flight_upcoming_date.toString(), copy.flight_upcoming_date.toString());
        assertEquals(orig.flight_past_days, copy.flight_past_days);
        assertEquals(orig.flight_future_days, copy.flight_future_days);
        assertEquals(orig.flight_upcoming_offset, copy.flight_upcoming_offset);
        assertEquals(orig.reservation_upcoming_offset, copy.reservation_upcoming_offset);
        assertEquals(orig.num_reservations, copy.num_reservations);
        assertEquals(orig.histograms, copy.histograms);
        assertEquals(orig.airport_histograms, copy.airport_histograms);
    }
    
    /**
     * testFindFlights
     */
    public void testFindFlights() throws Exception {
        Client client = this.getClient();
        CatalogContext catalogContext = this.getCatalogContext();
        this.initializeSEATSDatabase(catalogContext, client);
        SEATSClient benchmarkClient = this.initializeSEATSClient(catalogContext, client);
        assertNotNull(benchmarkClient);
        

        
    }
    
    
//    /**
//     * testDeleteCallForwarding
//     */
//    public void testDeleteCallForwarding() throws Exception {
//        TM1Client.Transaction txn = Transaction.DELETE_CALL_FORWARDING;
//        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
//        
//        for (int i = 0; i < 1000; i++) {
//            ClientResponse cresponse = null;
//            try {
//                cresponse = client.callProcedure(txn.callName, params);
//                assertEquals(Status.OK, cresponse.getStatus());
//            } catch (ProcCallException ex) {
//                cresponse = ex.getClientResponse();
////                System.err.println();
//                assertEquals(cresponse.toString(), Status.ABORT_USER, cresponse.getStatus());
//            }
//            assertNotNull(cresponse);
//        } // FOR
//    }
//    
//    /**
//     * testGetAccessData
//     */
//    public void testGetAccessData() throws Exception {
//        Client client = this.getClient();
//        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
//        TM1Client.Transaction txn = Transaction.GET_ACCESS_DATA;
//        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
//        ClientResponse cresponse = client.callProcedure(txn.callName, params);
//        assertNotNull(cresponse);
//        assertEquals(Status.OK, cresponse.getStatus());
//    }
//    
//    /**
//     * testGetNewDestination
//     */
//    public void testGetNewDestination() throws Exception {
//        Client client = this.getClient();
//        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
//        TM1Client.Transaction txn = Transaction.DELETE_CALL_FORWARDING;
//        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
//        ClientResponse cresponse = null;
//        try {
//            cresponse = client.callProcedure(txn.callName, params);
//            assertEquals(Status.OK, cresponse.getStatus());
//        } catch (ProcCallException ex) {
//            cresponse = ex.getClientResponse();
//            assertEquals(cresponse.toString(), Status.ABORT_USER, cresponse.getStatus());
//        }
//        assertNotNull(cresponse);
//        
//    }
//    
//    /**
//     * testGetSubscriberData
//     */
//    public void testGetSubscriberData() throws Exception {
//        Client client = this.getClient();
//        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
//        TM1Client.Transaction txn = Transaction.GET_SUBSCRIBER_DATA;
//        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
//        ClientResponse cresponse = client.callProcedure(txn.callName, params);
//        assertNotNull(cresponse);
//    }
//    
//    /**
//     * testInsertCallForwarding
//     */
//    public void testInsertCallForwarding() throws Exception {
//        Client client = this.getClient();
//        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
//        TM1Client.Transaction txn = Transaction.INSERT_CALL_FORWARDING;
//        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
//        ClientResponse cresponse = null;
//        try {
//            cresponse = client.callProcedure(txn.callName, params);
//            assertEquals(Status.OK, cresponse.getStatus());
//        } catch (ProcCallException ex) {
//            cresponse = ex.getClientResponse();
//            assertEquals(Status.ABORT_USER, cresponse.getStatus());
//        }
//        assertNotNull(cresponse);
//    }
//    
//    /**
//     * testUpdateLocation
//     */
//    public void testUpdateLocation() throws Exception {
//        Client client = this.getClient();
//        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
//        TM1Client.Transaction txn = Transaction.UPDATE_LOCATION;
//        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
//        ClientResponse cresponse = client.callProcedure(txn.callName, params);
//        assertNotNull(cresponse);
//        assertEquals(Status.OK, cresponse.getStatus());
//    }
//    
//    /**
//     * testUpdateSubscriberData
//     */
//    public void testUpdateSubscriberData() throws Exception {
//        Client client = this.getClient();
//        RegressionSuiteUtil.initializeTM1Database(this.getCatalogContext(), client);
//        TM1Client.Transaction txn = Transaction.UPDATE_SUBSCRIBER_DATA;
//        Object params[] = txn.generateParams(NUM_SUBSCRIBERS);
//        ClientResponse cresponse = null;
//        try {
//            cresponse = client.callProcedure(txn.callName, params);
//            assertEquals(Status.OK, cresponse.getStatus());
//        } catch (ProcCallException ex) {
//            cresponse = ex.getClientResponse();
//            assertEquals(Status.ABORT_USER, cresponse.getStatus());
//        }
//        assertNotNull(cresponse);
//    }

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSEATSSuite.class);
        builder.setGlobalConfParameter("client.scalefactor", SCALEFACTOR);

        // build up a project builder for the benchmark
        SEATSProjectBuilder project = new SEATSProjectBuilder();
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
