package edu.brown.benchmark.airline;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.types.TimestampType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.airline.AirlineClient.NewReservationCallback;
import edu.brown.benchmark.airline.AirlineClient.Reservation;
import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.hstore.Hstore;
import edu.mit.hstore.HStoreConstants;

public class TestAirlineClient extends AirlineBaseTestCase {
    
    static long FLIGHT_ID = 100001;
    static long AIRLINE_ID = 100;
    static long DEPART_AIRPORT_ID = 10;
    static long ARRIVE_AIRPORT_ID = 15;
    static int SEATNUM = 13;
    static TimestampType BENCHMARK_START = new TimestampType();
    static TimestampType FLIGHT_DATE = new TimestampType(BENCHMARK_START.getTime() + 1000000l);
    
    static long TXN_ID = 1000;
    static long CLIENT_HANDLE = 1234;
    static long RESERVATION_ID = 9999;
    
    protected static final double SCALE_FACTOR = 1000;
    protected static final String MOCK_ARGS[] = {
        "CLIENT.SCALEFACTOR=" + SCALE_FACTOR, 
        "HOST=localhost",
        "NUMCLIENTS=1",
        "NOCONNECTIONS=true",
        ""
    };
    
    private class MockClient extends AirlineClient {

        public MockClient(String[] args) {
            super(args);
            
            for (BitSet seats : CACHE_BOOKED_SEATS.values()) {
                seats.clear();
            } // FOR
            for (List<Reservation> queue : CACHE_LOCKS.keySet()) {
                queue.clear();
            } // FOR
            for (Set<FlightId> f_ids : CACHE_CUSTOMER_BOOKED_FLIGHTS.values()) {
                f_ids.clear();
            } // FOR
        }
        @Override
        public Catalog getCatalog() {
            return (BaseTestCase.catalog);
        }
    }
    
    private FlightId flight_id;
    private CustomerId customer_id;
    private AirlineClient client; 
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.flight_id = new FlightId(AIRLINE_ID,
                                      DEPART_AIRPORT_ID,
                                      ARRIVE_AIRPORT_ID,
                                      BENCHMARK_START,
                                      FLIGHT_DATE);
        this.customer_id = new CustomerId(1001, DEPART_AIRPORT_ID);
        
        MOCK_ARGS[MOCK_ARGS.length-1] = HStoreConstants.BENCHMARK_PARAM_PREFIX + "DATADIR=" + AirlineBaseTestCase.AIRLINE_DATA_DIR;
//        System.err.println(StringUtil.join("\n", MOCK_ARGS));
        this.client = new MockClient(MOCK_ARGS);
    }
    
    /**
     * testSeatBitMap
     */
    @Test
    public void testSeatBitMap() throws Exception {
        BitSet seats = AirlineClient.getSeatsBitSet(flight_id);
        assertNotNull(seats);
        assertFalse(AirlineClient.isFlightFull(seats));
        
        for (int i = 0; i < AirlineConstants.NUM_SEATS_PER_FLIGHT; i++) {
            assertFalse("SEAT #" + i, seats.get(i));
            seats.set(i);
            assertTrue("SEAT #" + i, seats.get(i));
            if (i+1 < AirlineConstants.NUM_SEATS_PER_FLIGHT)
                assertFalse(AirlineClient.isFlightFull(seats));
        } // FOR
        
        assertTrue(AirlineClient.isFlightFull(seats));
    }
    
    /**
     * testIsFlightFull
     */
    @Test
    public void testIsFlightFull() throws Exception {
        BitSet seats = new BitSet(AirlineConstants.NUM_SEATS_PER_FLIGHT);
        
        int seatnum = rand.nextInt(AirlineConstants.NUM_SEATS_PER_FLIGHT);
        assertFalse(seats.get(seatnum));
        seats.set(seatnum);
        assertTrue(seats.get(seatnum));
        assertFalse(AirlineClient.isFlightFull(seats));
    }
    
    /**
     * testNewReservationCallback
     */
    @Test
    public void testNewReservationCallback() throws Exception {
        int seatnum = rand.nextInt(AirlineConstants.NUM_SEATS_PER_FLIGHT);
        
        BitSet seats = AirlineClient.getSeatsBitSet(this.flight_id);
        assertNotNull(seats);
        assertFalse(seats.toString(), AirlineClient.isFlightFull(seats));
        assertFalse(seats.get(seatnum));
//        System.err.println(seats);
        
        // Fake a ClientResponse from a NewReservation txn
        VoltTable results[] = new VoltTable[] {
            new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT)),
            new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT))
        };
        results[0].addRow(new Long(1));
        ClientResponseImpl cresponse = new ClientResponseImpl(TXN_ID, CLIENT_HANDLE,
                                                              0, Hstore.Status.OK,
                                                              results, "");
        
        Reservation r = new Reservation(RESERVATION_ID, this.flight_id, customer_id, seatnum);
        assertNotNull(r);
        NewReservationCallback callback = this.client.new NewReservationCallback(r);
        callback.clientCallback(cresponse);
        
        // Check to make sure that our seat is now reserved
        assertFalse("Flight is incorrectly marked as full\n" + seats, AirlineClient.isFlightFull(seats));
        // FIXME assertTrue("Failed to mark seat as reserved?", seats.get(seatnum));
    }

}
