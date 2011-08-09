package edu.brown.benchmark.airline;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class BenchmarkProfile implements JSONSerializable {
    protected static final Logger LOG = Logger.getLogger(AirlineBaseClient.class.getName());
    
    public enum Members {
        AIRPORT_MAX_CUSTOMER_ID,
        FLIGHT_START_DATE,
        FLIGHT_UPCOMING_DATE,
        FLIGHT_PAST_DAYS,
        FLIGHT_FUTURE_DAYS,
        SCALE_FACTOR,
        SEATS_REMAINING,
        RESERVATION_COUNT,
        RESERVATION_UPCOMING_OFFSET,
    };
    
    /**
     * Data Scale Factor
     * Range: ???
     */
    public double scale_factor;
    
    /**
     * For each airport id, store the last id of the customer that uses this airport
     * as their local airport. The customer ids will be stored as follows in the dbms:
     * <16-bit AirportId><48-bit CustomerId>
     */
    public final Map<Long, Long> airport_max_customer_id = new HashMap<Long, Long>();
        
    /**
     * The date when flights total data set begins
     */
    public Date flight_start_date;
    
    /**
     * The date for when the flights are considered upcoming and are eligible for reservations
     */
    public Date flight_upcoming_date;
    
    /**
     * The number of days in the past that our flight data set includes.
     */
    public long flight_past_days;
    
    /**
     * The number of days in the future (from the flight_upcoming_date) that our flight data set includes
     */
    public long flight_future_days;
    
    /**
     * Store a list of FlightIds (encoded) and the number of seats
     * remaining for a particular flight.
     */
    public final ListOrderedMap<Long, Short> seats_remaining = new ListOrderedMap<Long, Short>();
    
    /**
     * The offset of when upcoming flights begin in the seats_remaining list
     */
    public Long flight_upcoming_offset = null;
    
    /**
     * The offset of when reservations for upcoming flights begin
     */
    public Long reservation_upcoming_offset = null;
    
    /**
     * The number of records loaded for each table
     */
    public final Map<String, Long> num_records = new HashMap<String, Long>();

    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Constructor - Keep your pimp hand strong!
     */
    public BenchmarkProfile() {
        // Nothing to see here...
    }
    
    /**
     * Get the scale factor value for this benchmark profile
     * @return
     */
    public double getScaleFactor() {
        return (this.scale_factor);
    }
    
    /**
     * Set the scale factor for this benchmark profile
     * @param scaleFactor
     */
    public void setScaleFactor(double scaleFactor) {
        assert(scaleFactor > 0) : "Invalid scale factor '" + scaleFactor + "'";
        this.scale_factor = scaleFactor;
    }
    
    /**
     * The number of reservations preloaded for this benchmark run
     * @return
     */
    public Long getRecordCount(String table_name) {
        return (this.num_records.get(table_name));
    }
    
    /**
     * Set the number of preloaded reservations
     * @param numReservations
     */
    public void setRecordCount(String table_name, long count) {
        this.num_records.put(table_name, count);
    }

    /**
     * The offset of when upcoming reservation ids begin
     * @return
     */
    public Long getReservationUpcomingOffset() {
        return (this.reservation_upcoming_offset);
    }
    
    /**
     * Set the number of upcoming reservation offset
     * @param numReservations
     */
    public void setReservationUpcomingOffset(long offset) {
        this.reservation_upcoming_offset = offset;
    }

    // -----------------------------------------------------------------
    // FLIGHT DATES
    // -----------------------------------------------------------------

    /**
     * The date in which the flight data set begins
     * @return
     */
    public Date getFlightStartDate() {
        return this.flight_start_date;
    }
    /**
     * 
     * @param start_date
     */
    public void setFlightStartDate(Date start_date) {
        this.flight_start_date = start_date;
    }

    /**
     * The date in which the flight data set begins
     * @return
     */
    public Date getFlightUpcomingDate() {
        return (this.flight_upcoming_date);
    }
    /**
     * 
     * @param start_date
     */
    public void setFlightUpcomingDate(Date upcoming_date) {
        this.flight_upcoming_date = upcoming_date;
    }
    
    /**
     * The date in which upcoming flights begin
     * @return
     */
    public long getFlightPastDays() {
        return (this.flight_past_days);
    }
    /**
     * 
     * @param flight_start_date
     */
    public void setFlightPastDays(long flight_past_days) {
        this.flight_past_days = flight_past_days;
    }
    
    /**
     * The date in which upcoming flights begin
     * @return
     */
    public long getFlightFutureDays() {
        return (this.flight_future_days);
    }
    /**
     * 
     * @param flight_start_date
     */
    public void setFlightFutureDays(long flight_future_days) {
        this.flight_future_days = flight_future_days;
    }
    
    // -----------------------------------------------------------------
    // CUSTOMER IDS
    // -----------------------------------------------------------------

    private class CustomerIdIterable implements Iterable<CustomerId> {
        private final ListOrderedSet<Long> airport_ids = new ListOrderedSet<Long>();
        private Long last_airport_id = null;
        private Long last_id = null;
        private Long last_max_id = null;
        
        public CustomerIdIterable(long...airport_ids) {
            for (long id : airport_ids) {
                this.airport_ids.add(id);
            } // FOR
        }
        
        public CustomerIdIterable(Collection<Long> airport_ids) {
            this.airport_ids.addAll(airport_ids);
        }
        
        @Override
        public Iterator<CustomerId> iterator() {
            return new Iterator<CustomerId>() {
                @Override
                public boolean hasNext() {
                    return (!CustomerIdIterable.this.airport_ids.isEmpty() || (last_id != null && last_id < last_max_id));
                }
                @Override
                public CustomerId next() {
                    if (last_airport_id == null) {
                        last_airport_id = airport_ids.remove(0);
                        last_id = 0l;
                        last_max_id = BenchmarkProfile.this.airport_max_customer_id.get(last_airport_id);
                    }
                    CustomerId next_id = new CustomerId(last_id, last_airport_id);
                    if (++last_id == last_max_id) last_airport_id = null;
                    return next_id;
                }
                @Override
                public void remove() {
                    // Not implemented
                }
            };
        }
    } // END CLASS
    
    public Iterable<CustomerId> getCustomerIds() {
        return (new CustomerIdIterable(this.airport_max_customer_id.keySet()));
    }
    public Iterable<CustomerId> getCustomerIds(Collection<Long> airport_ids) {
        return (new CustomerIdIterable(airport_ids));
    }
    public Iterable<CustomerId> getCustomerIds(long...airport_ids) {
        return (new CustomerIdIterable(airport_ids));
    }
    
    public long incrementAirportCustomerCount(long airport_id) {
        Long count = this.airport_max_customer_id.get(airport_id);
        if (count == null) count = 0l;
        count++;
        this.airport_max_customer_id.put(airport_id, count);
        return (count);
    }
    public Long getCustomerIdCount(long airport_id) {
        return (this.airport_max_customer_id.get(airport_id));
    }
    public long getCustomerIdCount() {
        long total = 0;
        for (long max_id : this.airport_max_customer_id.values()) {
            total += max_id;
        }
        return (total);
    }
    
    /**
     * Return the number of airports that are part of this profile
     * @return
     */
    public int getAirportCount() {
        return (this.airport_max_customer_id.size());
    }

    // -----------------------------------------------------------------
    // FLIGHT IDS
    // -----------------------------------------------------------------
    
    public Iterable<FlightId> getFlightIds() {
        return (new Iterable<FlightId>() {
            @Override
            public Iterator<FlightId> iterator() {
                return (new Iterator<FlightId>() {
                    private int idx = 0;
                    private final int cnt = BenchmarkProfile.this.seats_remaining.size();
                    
                    @Override
                    public boolean hasNext() {
                        return (idx < this.cnt);
                    }
                    @Override
                    public FlightId next() {
                        long encoded = BenchmarkProfile.this.seats_remaining.get(this.idx++);
                        return (new FlightId(encoded));
                    }
                    @Override
                    public void remove() {
                        // Not implemented
                    }
                });
            }
        });
    }

    /**
     * 
     * @param flight_id
     */
    public void addFlightId(FlightId flight_id) {
        assert(flight_id != null);
        this.seats_remaining.put(flight_id.encode(), (short)AirlineConstants.NUM_SEATS_PER_FLIGHT);
        if (this.flight_upcoming_offset == null &&
            this.flight_upcoming_date.compareTo(flight_id.getDepartDate(this.flight_start_date)) < 0) {
            this.flight_upcoming_offset = new Long(this.seats_remaining.size() - 1);
        }
    }
    
    /**
     * Return the number of unique flight ids
     * @return
     */
    public long getFlightIdCount() {
        return (this.seats_remaining.size());
    }
    
    /**
     * Return the index offset of when future flights 
     * @return
     */
    public long getFlightIdStartingOffset() {
        return (this.flight_upcoming_offset);
    }
    
    /**
     * Return flight 
     * @param index
     * @return
     */
    public FlightId getFlightId(int index) {
        assert(index >= 0);
        assert(index <= this.getFlightIdCount());
        return (new FlightId(this.seats_remaining.get(index)));
    }

    /**
     * Return the number of seats remaining for a flight
     * @param flight_id
     * @return
     */
    public int getFlightRemainingSeats(FlightId flight_id) {
        return ((int)this.seats_remaining.get(flight_id.encode()));
    }
    
    /**
     * Decrement the number of available seats for a flight and return
     * the total amount remaining
     */
    public int decrementFlightSeat(FlightId flight_id) {
        long id = flight_id.encode();
        Short seats = this.seats_remaining.get(id);
        assert(seats != null) : "Missing seat count for " + flight_id;
        return ((int)this.seats_remaining.put(id, (short)(seats - 1)));
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, BenchmarkProfile.class, BenchmarkProfile.Members.values());
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, BenchmarkProfile.class, BenchmarkProfile.Members.values());
    }
}
