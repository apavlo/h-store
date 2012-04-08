package edu.brown.benchmark.seats;

import java.io.IOException;
import java.util.regex.Pattern;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.LocalSingleProcessServer;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.VoltServerConfig;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.seats.procedures.FindFlights;
import edu.brown.benchmark.seats.procedures.GetTableCounts;
import edu.brown.benchmark.seats.util.FlightId;
import edu.brown.benchmark.seats.util.SEATSHistogramUtil;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * SEATS Benchmark Regression Tests
 */
public class TestSEATSSuite extends RegressionSuite {

    private static final double SCALE_FACTOR = 0.05;
    private static final int RANDOM_SEED = 1;
    
    private final String loaderArgs[] = {
        "CLIENT.SCALEFACTOR=" + SCALE_FACTOR, 
        "HOST=localhost",
        "NUMCLIENTS=1",
        "NOCONNECTIONS=true",
        "BENCHMARK.DATADIR=" + SEATSHistogramUtil.findDataDir()
    };
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestSEATSSuite(String name) {
        super(name);
    }
    
    /**
     * testLOADER
     */
    @org.junit.Test
    public void testLOADER() throws IOException, ProcCallException {
        // Load the mofo up and then check to make sure that if we load 
        // the profile back in that it has the values that we expect it to have
        SEATSProfile orig = this.loadDatabase();
        assertNotNull(orig);
        
        SEATSProfile copy = new SEATSProfile(this.getCatalog(), new RandomGenerator(RANDOM_SEED));
        assert(copy.airport_histograms.isEmpty());
        copy.loadProfile(this.getClient());
        
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
     * testFINDFLIGHTS
     */
    @org.junit.Test
    public void testFINDFLIGHTS() throws IOException, ProcCallException {
        SEATSProfile profile = this.loadDatabase();
        assertNotNull(profile);
        
        FlightId flight = profile.getRandomFlightId();
        assertNotNull(flight);
        
        Object params[] = {
            flight.getDepartAirportId(),
            flight.getArriveAirportId(),
            profile.flight_start_date,
            new TimestampType(2524626000l * 1000000), // 2050-01-01
            50 // miles
        };
        
        Client client = this.getClient();
        ClientResponse cr = client.callProcedure(FindFlights.class.getSimpleName(), params);
        assertNotNull(cr);
        assertEquals(Status.OK, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        
        // We should at least the flight that we already knew about
        VoltTable vt = cr.getResults()[0];
        assertNotNull(vt);
        boolean found = false;
        long expected = flight.encode();
        while (vt.advanceRow()) {
            long f_id = vt.getLong("F_ID");
            if (f_id == expected) {
                found = true;
            }
        } // WHILE
        assertTrue(flight.toString(), found);
        System.err.println(flight);
        System.err.println("==================");
        System.err.println(vt.toString());
    }

//    /**
//     * testTABLECOUNTS
//     */
//    @org.junit.Test
//    public void testTABLECOUNTS() throws IOException, ProcCallException {
//        Client client = getClient();
//        ClientResponse cr = null;
//        Random rand = new Random();
//        int num_tuples = 11;
//        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
//        for (Table catalog_tbl : catalog_db.getTables()) {
//            RegressionSuiteUtil.loadRandomData(client, catalog_tbl, rand, num_tuples);
//        } // FOR
//        
//        // Now get the counts for the tables that we just loaded
//        cr = client.callProcedure(GetTableCounts.class.getSimpleName());
//        System.err.println(cr);
//        assertEquals(Status.OK, cr.getStatus());
//        assertEquals(1, cr.getResults().length);
//        VoltTable vt = cr.getResults()[0];
//        while (vt.advanceRow()) {
//            String tableName = vt.getString(0);
//            int count = (int)vt.getLong(1);
//            assertEquals(tableName, num_tuples, count);
//        } // WHILE
//    }
        
    
    protected SEATSProfile loadDatabase() throws IOException, ProcCallException {
        final Catalog catalog = this.getCatalog();
        final Client client = this.getClient();
        SEATSProfile.clearCachedProfile();
        SEATSLoader loader = new SEATSLoader(loaderArgs) {
            {
                this.setClientHandle(client);
                this.setCatalog(catalog);
            }
            @Override
            public Catalog getCatalog() {
                return (catalog);
            }
        };
        loader.load();
        return (loader.getProfile());
    }
    

    /**
     * Build a list of the tests that will be run when TestSEATSSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the JNI and HSQL backends.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSEATSSuite.class);

        // build up a project builder for the SEATS app
        SEATSProjectBuilder project = new SEATSProjectBuilder();
        project.addAllDefaults();
        project.addProcedure(GetTableCounts.class);

        // Remove any MapReduce and OLAP transactions
        project.removeProcedures(Pattern.compile("^MR.*", Pattern.CASE_INSENSITIVE));
        project.removeProcedures(Pattern.compile("^OLAP.*", Pattern.CASE_INSENSITIVE));
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer("seats-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
//        config = new LocalSingleProcessServer("tpcc-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
//        config = new LocalCluster("seats-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);

        return builder;
    }

}
