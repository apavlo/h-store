package edu.brown.benchmark.seats;

import java.util.BitSet;

import org.junit.Test;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.seats.SEATSClient.NewReservationCallback;
import edu.brown.benchmark.seats.util.Reservation;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;

public class TestSEATSClient extends SEATSBaseTestCase {
    
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
    
    private class MockClient extends SEATSClient {

        public MockClient(String[] args) {
            super(args);
            super.clearCache();
        }
        @Override
        public CatalogContext getCatalogContext() {
            return (BaseTestCase.catalogContext);
        }
    }
    
    private long flight_id;
    private long customer_id;
    private SEATSClient client; 
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.flight_id = 10000l;
        this.customer_id = 99999l;
        
        MOCK_ARGS[MOCK_ARGS.length-1] = HStoreConstants.BENCHMARK_PARAM_PREFIX + "DATADIR=" + SEATSBaseTestCase.AIRLINE_DATA_DIR;
//        System.err.println(StringUtil.join("\n", MOCK_ARGS));
        this.client = new MockClient(MOCK_ARGS);
    }
    
    /**
     * testSeatBitMap
     */
    @Test
    public void testSeatBitMap() throws Exception {
        BitSet seats = client.getSeatsBitSet(flight_id);
        assertNotNull(seats);
        assertFalse(client.isFlightFull(seats));
        
        for (int i = 0; i < SEATSConstants.FLIGHTS_NUM_SEATS; i++) {
            assertFalse("SEAT #" + i, seats.get(i));
            seats.set(i);
            assertTrue("SEAT #" + i, seats.get(i));
            if (i+1 < SEATSConstants.FLIGHTS_NUM_SEATS)
                assertFalse(client.isFlightFull(seats));
        } // FOR
        
        assertTrue(client.isFlightFull(seats));
    }
    
    /**
     * testIsFlightFull
     */
    @Test
    public void testIsFlightFull() throws Exception {
        BitSet seats = new BitSet(SEATSConstants.FLIGHTS_NUM_SEATS);
        
        int seatnum = rand.nextInt(SEATSConstants.FLIGHTS_NUM_SEATS);
        assertFalse(seats.get(seatnum));
        seats.set(seatnum);
        assertTrue(seats.get(seatnum));
        assertFalse(client.isFlightFull(seats));
    }
    
    /**
     * testNewReservationCallback
     */
    @Test
    public void testNewReservationCallback() throws Exception {
        int seatnum = rand.nextInt(SEATSConstants.FLIGHTS_NUM_SEATS);
        
        BitSet seats = client.getSeatsBitSet(this.flight_id);
        assertNotNull(seats);
        assertFalse(seats.toString(), client.isFlightFull(seats));
        assertFalse(seats.get(seatnum));
//        System.err.println(seats);
        
        // Fake a ClientResponse from a NewReservation txn
        VoltTable results[] = new VoltTable[] {
            new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT)),
            new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT))
        };
        results[0].addRow(new Long(1));
        ClientResponseImpl cresponse = new ClientResponseImpl(TXN_ID, CLIENT_HANDLE,
                                                              0, Status.OK,
                                                              results, "");
        
        Reservation r = new Reservation(RESERVATION_ID, this.flight_id, customer_id, seatnum);
        assertNotNull(r);
        NewReservationCallback callback = this.client.new NewReservationCallback(r);
        callback.clientCallback(cresponse);
        
        // Check to make sure that our seat is now reserved
        assertFalse("Flight is incorrectly marked as full\n" + seats, client.isFlightFull(seats));
        // FIXME assertTrue("Failed to mark seat as reserved?", seats.get(seatnum));
    }

}
