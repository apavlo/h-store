package edu.brown.benchmark.airline;

import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.*;
import org.voltdb.VoltType;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.airline.util.*;
import edu.brown.utils.FileUtil;

public class BenchmarkProfile implements JSONString {
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
    private long scale_factor;
    
    /**
     * For each airport id, store the last id of the customer that uses this airport
     * as their local airport. The customer ids will be stored as follows in the dbms:
     * <16-bit AirportId><48-bit CustomerId>
     */
    private final Map<Long, Long> airport_max_customer_id = new HashMap<Long, Long>();
        
    /**
     * The date when flights total data set begins
     */
    private Date flight_start_date;
    
    /**
     * The date for when the flights are considered upcoming and are eligible for reservations
     */
    private Date flight_upcoming_date;
    
    /**
     * The number of days in the past that our flight data set includes.
     */
    private long flight_past_days;
    
    /**
     * The number of days in the future (from the flight_upcoming_date) that our flight data set includes
     */
    private long flight_future_days;
    
    /**
     * Store a list of FlightIds (encoded) and the number of seats
     * remaining for a particular flight.
     */
    private final ListOrderedMap<Long, Short> seats_remaining = new ListOrderedMap<Long, Short>();
    
    /**
     * The offset of when upcoming flights begin in the seats_remaining list
     */
    private Long flight_upcoming_offset = null;
    
    /**
     * The offset of when reservations for upcoming flights begin
     */
    private Long reservation_upcoming_offset = null;
    
    /**
     * The number of records loaded for each table
     */
    private final Map<String, Long> num_records = new HashMap<String, Long>();

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
    public long getScaleFactor() {
        return (this.scale_factor);
    }
    
    /**
     * Set the scale factor for this benchmark profile
     * @param scale_factor
     */
    public void setScaleFactor(long scale_factor) {
        assert(this.scale_factor > 0);
        this.scale_factor = scale_factor;
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
    public long getAirportCount() {
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
        assert(seats != null);
        return ((int)this.seats_remaining.put(id, (short)(seats - 1)));
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    public void save(String path) throws Exception {
        LOG.debug("Writing out '" + this.getClass().getSimpleName() + "' to " + path + "'");
        String json = this.toJSONString();
        JSONObject jsonObject = new JSONObject(json);
        FileOutputStream out = new FileOutputStream(path);
        out.write(jsonObject.toString(2).getBytes());
        out.close();
        return;
    }
    
    public void load(String path) throws Exception {
        LOG.debug("Reading in '" + this.getClass().getSimpleName() + "' from " + path + "'");
        String contents = FileUtil.readFile(path);
        if (contents.isEmpty()) {
            throw new Exception("The file '" + path + "' is empty");
        }
        JSONObject jsonObject = new JSONObject(contents);
        this.fromJSONObject(jsonObject);
    }
    
    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            this.toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return stringer.toString();
    }
    
    @SuppressWarnings("unchecked")
    public void toJSONString(JSONStringer stringer) throws JSONException {
        Class<?> profileClass = this.getClass();
        for (Members element : Members.values()) {
            try {
                Field field = profileClass.getDeclaredField(element.toString().toLowerCase());
                Object value = field.get(this);
                
                if (value instanceof List) {
                    stringer.key(element.name()).array();
                    for (Object list_value : (List<?>)value) {
                        stringer.value(list_value);
                    } // FOR
                    stringer.endArray();
                } else if (value instanceof Map) {
                    stringer.key(element.name()).object();
                    for (Entry<?, ?> e : ((Map<?, ?>)value).entrySet()) {
                        stringer.key(e.getKey().toString()).value(e.getValue());
                    } // FOR
                    stringer.endObject();
                } else {
                    stringer.key(element.name()).value(field.get(this)); 
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
    }
    
    @SuppressWarnings("unchecked")
    public void fromJSONObject(JSONObject object) throws JSONException {
        Class<?> profileClass = this.getClass();
        for (Members element : Members.values()) {
            try {
                String field_name = element.toString().toLowerCase();
                Field field = profileClass.getDeclaredField(field_name);
                Class<?> field_class = field.getType();
                Object value = field.get(this);
                
                if (value instanceof List) {
                    LOG.debug("[LIST] " + element.name() + " ==> " + value.getClass());
                    JSONArray jsonArray = object.getJSONArray(element.name());
                    List list = (List)value;
                    Type type = field.getGenericType();
                    assert(type instanceof ParameterizedType);
                    ParameterizedType pType = (ParameterizedType) type;
                    VoltType volt_type = VoltType.typeFromClass((Class<?>) pType.getActualTypeArguments()[0]);

                    for (int i = 0, cnt = jsonArray.length(); i < cnt; i++) {
                        Object json_value = jsonArray.get(i);
                        list.add(VoltTypeUtil.getObjectFromString(volt_type, json_value.toString()));
                    } // FOR
                    System.out.println(list.toString());
                    
                } else if (value instanceof Map) {
                    LOG.debug("[MAP] " + element.name() + " ==> " + value.getClass());
                    JSONObject jsonObject = object.getJSONObject(element.name());
                    Map map = (Map)value;
                    
                    Type type = field.getGenericType();
                    assert(type instanceof ParameterizedType);
                    ParameterizedType pType = (ParameterizedType) type;
                    VoltType key_type = VoltType.typeFromClass((Class<?>) pType.getActualTypeArguments()[0]);
                    VoltType val_type = VoltType.typeFromClass((Class<?>) pType.getActualTypeArguments()[1]);
                    
                    Iterator<String> keys_it = jsonObject.keys();
                    while (keys_it.hasNext()) {
                        String key = keys_it.next();
                        Object key_obj = VoltTypeUtil.getObjectFromString(key_type, key);
                        Object val_obj = VoltTypeUtil.getObjectFromString(val_type, jsonObject.getString(key));
                        LOG.debug(element + ": " + key_obj + " -> " + val_obj);
                        map.put(key_obj, val_obj);
                    } // WHILE
                    
                } else {
                    Object json_value = object.get(element.name());
                    LOG.debug(element + " -> " + json_value + " -> " + field.getType());
                    VoltType volt_type = VoltType.typeFromClass(field_class);
                    field.set(this, VoltTypeUtil.getObjectFromString(volt_type, json_value.toString()));
                }
            } catch (Exception ex) {
                LOG.error("Unable to deserialize field '" + element + "'");
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
    }    
}
