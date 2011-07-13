package edu.brown.benchmark.airline;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;

import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;

import junit.framework.TestCase;

public class TestBenchmarkProfile extends TestCase {
    
    private final int num_airports = 10;
    private final int num_customers[] = new int[this.num_airports];
    private final int max_num_customers = 4;
    private final Random rand = new Random(0);
    private final BenchmarkProfile profile = new BenchmarkProfile();
    private final HashSet<CustomerId> customer_ids = new HashSet<CustomerId>();

    private final HashSet<FlightId> flight_ids = new HashSet<FlightId>();
    private final long num_flights = 10l;
    private final Date flight_start_date = new Date(1262630005000l); // Monday 01.04.2010 13:33:25
    private final int flight_past_days = 7;
    private final int flight_future_days = 14;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.profile.setFlightPastDays(this.flight_past_days);
        this.profile.setFlightStartDate(this.flight_start_date);
        
        for (long airport_id = 0; airport_id < this.num_airports; airport_id++) {
            this.num_customers[(int)airport_id] = this.rand.nextInt(max_num_customers) + 1;
            for (int customer_id = 0; customer_id < this.num_customers[(int)airport_id]; customer_id++) {
                this.profile.incrementAirportCustomerCount(airport_id);
                this.customer_ids.add(new CustomerId(customer_id, airport_id));
            } // FOR
//            System.err.println(airport_id + ": " + this.num_customers[(int)airport_id] + " customers");
        } // FOR
//        System.err.println("------------------------");
        
        //
        // Add a bunch of FlightIds
        //
        int count = 0;
        for (long depart_airport_id = 0; depart_airport_id < this.num_airports; depart_airport_id++) {
            for (long arrive_airport_id = 0; arrive_airport_id < this.num_airports; arrive_airport_id++) {
                if (depart_airport_id == arrive_airport_id) continue;
                int time_offset = this.rand.nextInt(86400000 * this.flight_future_days);
                Date flight_date = new Date(this.flight_start_date.getTime() + time_offset);
                FlightId id = new FlightId(count++, depart_airport_id, arrive_airport_id, this.flight_start_date, flight_date);
                this.profile.addFlightId(id);
                this.flight_ids.add(id);
                if (count >= this.num_flights) break;
            } // FOR
            if (count >= this.num_flights) break;
        } // FOR
        assertEquals(this.num_flights, this.flight_ids.size());
    }
    
    /**
     * testIncrementAirportCustomerCount
     */
    public void testIncrementAirportCustomerCount() {
        for (long airport_id = 0; airport_id < this.num_airports; airport_id++) {
            Long cnt = this.profile.getCustomerIdCount(airport_id);
            assertNotNull(cnt);
            assertEquals((long)this.num_customers[(int)airport_id], (long)cnt);
        } // FOR
    }
    
    /**
     * testCustomerIdIterable
     */
    public void testCustomerIdIterable() {
        Map<Long, AtomicInteger> airport_counts = new HashMap<Long, AtomicInteger>();
        for (long airport_id = 0; airport_id < this.num_airports; airport_id++) {
            airport_counts.put(airport_id, new AtomicInteger(0));
        } // FOR
        
//        int idx = 0;
        for (CustomerId customer_id : this.profile.getCustomerIds()) {
            long airport_id = customer_id.getDepartAirportId();
            airport_counts.get(airport_id).incrementAndGet();
//            System.err.println("[" + (idx++) + "]: " + customer_id);
            assertTrue(this.customer_ids.contains(customer_id));
        } // FOR
        assertFalse(airport_counts.isEmpty());
        
        for (long airport_id = 0; airport_id < this.num_airports; airport_id++) {
//            System.err.println(airport_id + ": " + airport_counts.get(airport_id));
            assertTrue(airport_counts.containsKey(airport_id));
            assertEquals(this.num_customers[(int)airport_id], airport_counts.get(airport_id).get());
        } // FOR
    }
    
    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String jsonString = this.profile.toJSONString();
        for (FlightId flight_id : this.flight_ids) {
            String encoded = Long.toString(flight_id.encode());
            assertTrue(jsonString.contains(encoded));
        } // FOR
    }
    
    /**
     * testFromJSONString
     */
    public void testFromJSONString() throws Exception {
        String jsonString = this.profile.toJSONString();
        JSONObject jsonObject = new JSONObject(jsonString);
        
        BenchmarkProfile clone = new BenchmarkProfile();
        clone.fromJSON(jsonObject, null);
        
        assertEquals(this.profile.getCustomerIdCount(), clone.getCustomerIdCount());
        assertEquals(this.profile.getFlightIdCount(), clone.getFlightIdCount());
        assertEquals(this.profile.getFlightStartDate(), clone.getFlightStartDate());
        
        for (FlightId clone_id : clone.getFlightIds()) {
            assert(this.flight_ids.contains(clone_id)) : "Unknown flight id " + clone_id;
        } // FOR
        
    }

}
