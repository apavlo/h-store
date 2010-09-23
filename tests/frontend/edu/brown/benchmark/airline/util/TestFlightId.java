package edu.brown.benchmark.airline.util;

import java.util.Calendar;
import java.util.Date;

import edu.brown.benchmark.airline.AirlineConstants;

import junit.framework.TestCase;

public class TestFlightId extends TestCase {

    private final long base_ids[]           = { 111, 222, 333 };  
    private final long depart_airport_ids[] = { 444, 555, 666 };
    private final long arrive_airport_ids[] = { 777, 888, 999 };
    private final int flight_offset_days[]  = { 1, 2, 4, 8 };
    private final Date flight_dates[]       = new Date[this.flight_offset_days.length];
    private Date start_date;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.start_date = Calendar.getInstance().getTime();
        for (int i = 0; i < this.flight_dates.length; i++) {
            int day = this.flight_offset_days[i];
            this.flight_dates[i] = new Date(this.start_date.getTime() + (day * AirlineConstants.MILISECONDS_PER_DAY));
        } // FOR
    }
    
    /**
     * testFlightId
     */
    public void testFlightId() {
        for (long base_id : this.base_ids) {
            for (long depart_airport_id : this.depart_airport_ids) {
                for (long arrive_airport_id : this.arrive_airport_ids) {
                    for (Date flight_date : this.flight_dates) {
                        FlightId flight_id = new FlightId(base_id, depart_airport_id, arrive_airport_id, this.start_date, flight_date);
                        assertNotNull(flight_id);
                        assertEquals(base_id, flight_id.getId());
                        assertEquals(depart_airport_id, flight_id.getDepartAirportId());
                        assertEquals(arrive_airport_id, flight_id.getArriveAirportId());
                        assertEquals(flight_date, flight_id.getDepartDate(this.start_date));
                    } // FOR (time_code)
                } // FOR (arrive_airport_id)
            } // FOR (depart_airport_id)
        } // FOR (base_ids)
    }
    
    /**
     * testFlightIdEncode
     */
    public void testFlightIdEncode() {
        for (long base_id : this.base_ids) {
            for (long depart_airport_id : this.depart_airport_ids) {
                for (long arrive_airport_id : this.arrive_airport_ids) {
                    for (Date flight_date : this.flight_dates) {
                        long values[] = { base_id, depart_airport_id, arrive_airport_id, FlightId.calculateFlightDate(this.start_date, flight_date) };
                        long encoded = FlightId.encode(values);
                        assert(encoded >= 0) : "Invalid encoded value '" + encoded + "'";
                
                        FlightId flight_id = new FlightId(encoded);
                        assertNotNull(flight_id);
                        assertEquals(base_id, flight_id.getId());
                        assertEquals(depart_airport_id, flight_id.getDepartAirportId());
                        assertEquals(arrive_airport_id, flight_id.getArriveAirportId());
                        assertEquals(flight_date, flight_id.getDepartDate(this.start_date));
                    } // FOR (time_code)
                } // FOR (arrive_airport_id)
            } // FOR (depart_airport_id)
        } // FOR (base_ids)
    }
    
    /**
     * testFlightIdDecode
     */
    public void testFlightIdDecode() {
        for (long base_id : this.base_ids) {
            for (long depart_airport_id : this.depart_airport_ids) {
                for (long arrive_airport_id : this.arrive_airport_ids) {
                    for (Date flight_date : this.flight_dates) {
                        long values[] = { base_id, depart_airport_id, arrive_airport_id, FlightId.calculateFlightDate(this.start_date, flight_date) };
                        long encoded = FlightId.encode(values);
                        assert(encoded >= 0);

                        long new_values[] = FlightId.decode(encoded);
                        assertEquals(values.length, new_values.length);
                        for (int i = 0; i < new_values.length; i++) {
                            assertEquals(values[i], new_values[i]);
                        } // FOR
                    } // FOR (time_code)
                } // FOR (arrive_airport_id)
            } // FOR (depart_airport_id)
        } // FOR (base_ids)
    }
}