package edu.brown.benchmark.airline.util;

import java.util.*;

import junit.framework.TestCase;

public class TestReturnFlight extends TestCase {
    
    private final long customer_base_id  = 1000;
    private final long depart_airport_id = 9999;
    private final int return_days[]      = { 1, 5, 14 };
    
    private Date flight_date;
    private CustomerId customer_id;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.customer_id = new CustomerId(this.customer_base_id, this.depart_airport_id);
        assertNotNull(this.customer_id);
        this.flight_date = Calendar.getInstance().getTime();
        assertNotNull(this.flight_date);
    }

    /**
     * testReturnFlight
     */
    public void testReturnFlight() {
        for (int return_day : this.return_days) {
            ReturnFlight return_flight = new ReturnFlight(this.customer_id, this.depart_airport_id, this.flight_date, return_day);
            assertNotNull(return_flight);
            assertEquals(this.customer_id, return_flight.getCustomerId());
            assertEquals(this.depart_airport_id, return_flight.getReturnAirportId());
            assertTrue(this.flight_date.before(return_flight.getReturnDate()));
        } // FOR
    }
    
    /**
     * testCalculateReturnDate
     */
    public void testCalculateReturnDate() {
        for (int return_day : this.return_days) {
            Date return_flight_date = ReturnFlight.calculateReturnDate(this.flight_date, return_day);
            assertNotNull(return_flight_date);
            assertTrue(this.flight_date.before(return_flight_date));
        } // FOR
    }
}
