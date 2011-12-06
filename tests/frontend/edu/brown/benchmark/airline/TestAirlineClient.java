package edu.brown.benchmark.airline;

import java.util.BitSet;

import org.junit.Test;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.airline.util.FlightId;

public class TestAirlineClient extends AirlineBaseTestCase {
    
    static long AIRLINE_ID = 100;
    static long DEPART_AIRPORT_ID = 10;
    static long ARRIVE_AIRPORT_ID = 15;
    static TimestampType BENCHMARK_START = new TimestampType();
    static TimestampType FLIGHT_DATE = new TimestampType(BENCHMARK_START.getTime() + 1000000l);
    
    private FlightId flight_id;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.flight_id = new FlightId(AIRLINE_ID, DEPART_AIRPORT_ID, ARRIVE_AIRPORT_ID, BENCHMARK_START, FLIGHT_DATE);
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
        } // FOR
        
        assertTrue(AirlineClient.isFlightFull(seats));
    }

}
