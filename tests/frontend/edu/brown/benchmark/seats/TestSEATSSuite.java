package edu.brown.benchmark.seats;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.LocalSingleProcessServer;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.RegressionSuiteUtil;
import org.voltdb.regressionsuites.VoltServerConfig;
import org.voltdb.sysprocs.AdHoc;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.HStoreSiteTestUtil.WrapperProcedureCallback;
import edu.brown.benchmark.seats.SEATSClient.Transaction;
import edu.brown.benchmark.seats.util.SEATSHistogramUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

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
        SEATSProfile.clearCachedProfile();
        File dataDir = SEATSHistogramUtil.findDataDir();
        assert(dataDir != null);
        
        String args[] = {
            "NOCONNECTIONS=true",
            "CLIENT.SCALEFACTOR=" + SCALEFACTOR, 
            "BENCHMARK.DATADIR=" + dataDir.getAbsolutePath()
        };
        SEATSLoader loader = new SEATSLoader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
            @Override
            public double getScaleFactor() {
                return (SCALEFACTOR);
            }
        };
        loader.load();
        return (loader);
    }
    
    public SEATSClient initializeSEATSClient(final CatalogContext catalogContext, final Client client) throws Exception {
        File dataDir = SEATSHistogramUtil.findDataDir();
        assert(dataDir != null);
        
        String args[] = {
            "NOCONNECTIONS=true",
            "CLIENT.SCALEFACTOR=" + SCALEFACTOR, 
            "BENCHMARK.DATADIR=" + dataDir.getAbsolutePath()
        };
        SEATSClient benchmarkClient = new SEATSClient(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
            @Override
            public double getScaleFactor() {
                return (SCALEFACTOR);
            }
        };
        benchmarkClient.getProfile().loadProfile(client);
        
        // Fire off a FindOpenSeats so that we can prime ourselves
        Pair<Object[], ProcedureCallback> pair = benchmarkClient.getFindOpenSeatsParams();
        assert(pair != null);
        Object params[] = pair.getFirst();
        WrapperProcedureCallback callback = new WrapperProcedureCallback(1, pair.getSecond());
        client.callProcedure(callback, Transaction.FIND_OPEN_SEATS.getExecName(), params);
        
        // Wait until it's done
        boolean ret = callback.latch.await(1000, TimeUnit.MILLISECONDS);
        assertTrue(callback.latch.toString(), ret);
        
        return (benchmarkClient);
    }
    
    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        CatalogContext catalogContext = this.getCatalogContext();
        SEATSLoader loader = this.initializeSEATSDatabase(catalogContext, client);
        SEATSProfile profile = loader.getProfile();
        assertNotNull(profile);
        
        Set<String> allTables = new HashSet<String>();
        CollectionUtil.addAll(allTables, SEATSConstants.TABLES_SCALING);
        CollectionUtil.addAll(allTables, SEATSConstants.TABLES_DATAFILES);
        
        Map<String, Long> expected = new HashMap<String, Long>();
        expected.put(SEATSConstants.TABLENAME_FLIGHT, profile.num_flights);
        expected.put(SEATSConstants.TABLENAME_CUSTOMER, profile.num_customers);
        expected.put(SEATSConstants.TABLENAME_RESERVATION, profile.num_reservations);
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        for (String tableName : allTables) {
            String query = "SELECT COUNT(*) FROM " + tableName;
            ClientResponse cresponse = client.callProcedure(procName, query);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            long count = results[0].asScalarLong();
            
            if (expected.containsKey(tableName)) {
                assertEquals(tableName, expected.get(tableName).longValue(), count);
            }
            else {
                assertTrue(tableName + " -> " + count, count > 0);
            }
        } // FOR
        
        // Make sure that our FLIGHT rows are correct
        String query = "SELECT * FROM " + SEATSConstants.TABLENAME_FLIGHT;
        ClientResponse cresponse = RegressionSuiteUtil.sql(client, query);
        VoltTable results[] = cresponse.getResults();
        assertEquals(profile.num_flights, results[0].getRowCount());
        Table tbl = catalogContext.getTableByName(SEATSConstants.TABLENAME_FLIGHT);
        assertEquals(tbl.getColumns().size(), results[0].getColumnCount());
        List<Column> notNullCols = new ArrayList<Column>();
        for (Column col : tbl.getColumns()) {
            if (col.getNullable() == false) notNullCols.add(col);
        }
        
        while (results[0].advanceRow()) {
            for (Column col : notNullCols) {
                results[0].get(col.getIndex());
                assertFalse(col.fullName(), results[0].wasNull());
            }
        } // WHILE
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
        
        String sql = "SELECT CFP_NUM_FLIGHTS, CFP_NUM_CUSTOMERS, CFP_NUM_RESERVATIONS " +
                     "  FROM " + SEATSConstants.TABLENAME_CONFIG_PROFILE;
        ClientResponse cresponse = RegressionSuiteUtil.sql(client, sql);
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].getRowCount());
        assertTrue(results[0].advanceRow());
        assertEquals("num_flights", orig.num_flights, results[0].getLong("CFP_NUM_FLIGHTS"));
        assertEquals("num_customers", orig.num_customers, results[0].getLong("CFP_NUM_CUSTOMERS"));
        assertEquals("num_reservations", orig.num_reservations, results[0].getLong("CFP_NUM_RESERVATIONS"));
        
        SEATSProfile copy = new SEATSProfile(catalogContext, orig.rng);
        assert(copy.airport_histograms.isEmpty());
        copy.loadProfile(client);

        try {
            assertEquals("scale_factor", orig.scale_factor, copy.scale_factor);
            // BUSTED??? assertEquals(orig.airport_max_customer_id, copy.airport_max_customer_id);
            // BUSTED??? assertEquals(orig.flight_start_date, copy.flight_start_date);
            // BUSTED??? assertEquals(orig.flight_upcoming_date, copy.flight_upcoming_date);
            assertEquals("flight_past_days", orig.flight_past_days, copy.flight_past_days);
            assertEquals("flight_future_days", orig.flight_future_days, copy.flight_future_days);
            assertEquals("num_flights", orig.num_flights, copy.num_flights);
            assertEquals("num_customers", orig.num_customers, copy.num_customers);
            assertEquals("num_reservations", orig.num_reservations, copy.num_reservations);
            assertEquals("histograms", orig.histograms, copy.histograms);
            assertEquals("airport_histograms", orig.airport_histograms, copy.airport_histograms);
        } catch (AssertionError ex) {
            System.err.println(StringUtil.columns(orig.toString(), copy.toString()));
            throw ex;
        }
    }
    
    /**
     * testFindOpenSeats
     */
    public void testFindOpenSeats() throws Exception {
        Client client = this.getClient();
        CatalogContext catalogContext = this.getCatalogContext();
        this.initializeSEATSDatabase(catalogContext, client);
        SEATSClient benchmarkClient = this.initializeSEATSClient(catalogContext, client);
        assertNotNull(benchmarkClient);
     
        Transaction txn = Transaction.FIND_OPEN_SEATS;
        Pair<Object[], ProcedureCallback> pair = benchmarkClient.getFindOpenSeatsParams();
        assertNotNull(pair);
        Object params[] = pair.getFirst();
     
        ClientResponse cresponse = null;
        try {
            cresponse = client.callProcedure(txn.getExecName(), params);
            assertEquals(Status.OK, cresponse.getStatus());
        } catch (ProcCallException ex) {
            cresponse = ex.getClientResponse();
            assertEquals(cresponse.toString(), Status.ABORT_USER, cresponse.getStatus());
        }
        
        VoltTable results[] = cresponse.getResults();
        assertEquals(2, results.length);
        
        
        // Make sure that the flight ids all match
        assertEquals(1, results[0].getRowCount());
        assertTrue(results[0].advanceRow());
        long flight_id = results[0].getLong("F_ID");
        assertNotSame(VoltType.NULL_BIGINT, flight_id);
        
        while (results[1].advanceRow()) {
            long seat_flight_id = results[1].getLong("F_ID");
            assertEquals(flight_id, seat_flight_id);
        } // WHILE
    }
    
    /**
     * testNewReservation
     */
    public void testNewReservation() throws Exception {
        Client client = this.getClient();
        CatalogContext catalogContext = this.getCatalogContext();
        this.initializeSEATSDatabase(catalogContext, client);

        ClientResponse cresponse;
        String sql;
        
        Random rng = new Random();
        long r_id = 1000l;
        long c_id = 10; // rng.nextInt((int)profile.num_customers);
        long f_id = 10; // rng.nextInt((int)profile.num_flights);
        long seatnum = rng.nextInt(SEATSConstants.FLIGHTS_NUM_SEATS);
        long attrs[] = new long[SEATSConstants.NEW_RESERVATION_ATTRS_SIZE];
        Arrays.fill(attrs, 9999l);
        Object params[] = { r_id, c_id, f_id, seatnum, 100d, attrs, new TimestampType() };
        
        // Check the number of available seats for this flight
        sql = "SELECT F_SEATS_LEFT " +
              "  FROM " + SEATSConstants.TABLENAME_FLIGHT +
              " WHERE F_ID = " + f_id;
        cresponse = RegressionSuiteUtil.sql(client, sql);
        long orig_num_seats = cresponse.getResults()[0].asScalarLong();
        assert(orig_num_seats > 0);
        
        // Then insert a new reservation
        Transaction txn = Transaction.NEW_RESERVATION;
        cresponse = client.callProcedure(txn.getExecName(), params);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        
        // Then check that the number of available seats is reduced by one 
        cresponse = RegressionSuiteUtil.sql(client, sql);
        long new_num_seats = cresponse.getResults()[0].asScalarLong();
        assert(new_num_seats > 0);
        assertEquals(orig_num_seats-1, new_num_seats);
        
        // Make sure that our customer has the reservation
        sql = "SELECT R_C_ID " +
              " FROM " + SEATSConstants.TABLENAME_RESERVATION +
              " WHERE R_F_ID = " + f_id +
              "   AND R_SEAT = " + seatnum;
        cresponse = RegressionSuiteUtil.sql(client, sql);
        long r_c_id = cresponse.getResults()[0].asScalarLong();
        assertEquals(c_id, r_c_id);
    }
    
//    /**
//     * testFindFlights
//     */
//    public void testFindFlights() throws Exception {
//        Client client = this.getClient();
//        CatalogContext catalogContext = this.getCatalogContext();
//        this.initializeSEATSDatabase(catalogContext, client);
//        SEATSClient benchmarkClient = this.initializeSEATSClient(catalogContext, client);
//        assertNotNull(benchmarkClient);
//        
//        Transaction txn = Transaction.FIND_FLIGHTS;
//        Pair<Object[], ProcedureCallback> pair = benchmarkClient.getFindFlightsParams();
//        assertNotNull(pair);
//        Object params[] = pair.getFirst();
//        
//        ClientResponse cresponse = null;
//        try {
//            cresponse = client.callProcedure(txn.getExecName(), params);
//            assertEquals(Status.OK, cresponse.getStatus());
//        } catch (ProcCallException ex) {
//            cresponse = ex.getClientResponse();
//            assertEquals(cresponse.toString(), Status.ABORT_USER, cresponse.getStatus());
//        }
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
