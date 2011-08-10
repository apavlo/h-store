/**
 * 
 */
package edu.brown.benchmark.airline;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.benchmark.airline.util.HistogramUtil;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreConf;

/**
 * @author pavlo
 *
 */
public abstract class AirlineBaseClient extends BenchmarkComponent implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(AirlineBaseClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    
    /**
     * Tuple Code to Tuple Id Mapping
     * For some tables, we want to store a unique code that can be used to map
     * to the id of a tuple. Any table that has a foreign key reference to this table
     * will use the unique code in the input data tables instead of the id. Thus, we need
     * to keep a table of how to map these codes to the ids when loading.
     */
    private static final String CODE_TO_ID_COLUMNS[][] = {
        {"CO_CODE_3",       "CO_ID"}, // COUNTRY
        {"AP_CODE",         "AP_ID"}, // AIRPORT
        {"AL_IATA_CODE",    "AL_ID"}, // AIRLINE
    };
    
    // ----------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------
    
    /** Data Directory */
    private transient final File airline_data_dir;
   
    /** TODO */
    private transient final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();
    
    /**
     * Each AirportCode will have a histogram of the number of flights 
     * that depart from that airport to all the other airports
     */
    protected transient final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();
    
    /** The airports that we actually care about */
    protected final Set<String> airport_codes = new HashSet<String>();
    
    /**
     * Mapping from Airports to their geolocation coordinates
     * AirportCode -> <Latitude, Longitude>
     */
    protected final ListOrderedMap<String, Pair<Double, Double>> airport_locations = new ListOrderedMap<String, Pair<Double,Double>>();
    
    /**
     * Foreign Key Mappings
     * Column Name -> Xref Mapper
     */
    protected transient final Map<String, String> fkey_value_xref = new HashMap<String, String>();

    /**
     * Key -> Id Mappings
     */
    protected transient final Map<String, String> code_columns = new HashMap<String, String>();
    protected transient final Map<String, Map<String, Long>> code_id_xref = new HashMap<String, Map<String, Long>>();
    
    /**
     * Specialized random number generator
     */
    protected transient final AbstractRandomGenerator m_rng;
    
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

    /**
     * AirportCode -> Set<AirportCode, Distance>
     * Only store the records for those airports in HISTOGRAM_FLIGHTS_PER_AIRPORT
     */
    public final Map<String, Map<String, Short>> airport_distances = new HashMap<String, Map<String, Short>>();

    
    // ----------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------
    
    /**
     * @param args
     */
    public AirlineBaseClient(String[] args) {
        super(args);
        
        String data_dir = null;
        String profile_file = null;
        int seed = 0;
        String randGenClassName = RandomGenerator.class.getName();
        String randGenProfilePath = null;
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Benchmark Profile File
            if (key.equalsIgnoreCase("benchmarkprofile")) {
                profile_file = value;
            // Random Generator Seed
            } else if (key.equalsIgnoreCase("randomseed")) {
                seed = Integer.parseInt(value);
            // Random Generator Class
            } else if (key.equalsIgnoreCase("randomgenerator")) {
                randGenClassName = value;
            // Random Generator Profile File
            } else if (key.equalsIgnoreCase("randomprofile")) {
                randGenProfilePath = value;
            // Data Directory
            // Parameter that points to where we can find the initial data files
            } else if (key.equalsIgnoreCase("datadir")) {
                data_dir = value;
            }
        } // FOR
        if (data_dir == null) {
            throw new RuntimeException("Unable to start benchmark. Missing 'datadir' parameter"); 
        }
        this.airline_data_dir = new File(data_dir);
        if (this.airline_data_dir.exists() == false) {
            throw new RuntimeException("Unable to start benchmark. The data directory '" + this.airline_data_dir.getAbsolutePath() + "' does not exist");
        }
        
        // Load our good old friend Mister HStoreConf
        HStoreConf hstore_conf = this.getHStoreConf();
        
        // Create BenchmarkProfile
        this.setScaleFactor(hstore_conf.client.scalefactor);
        if (profile_file != null) {
            try {
                this.load(profile_file, null);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load benchmark profile '" + profile_file + "'", ex);
            }
        }
        
        // Load Random Generator
        AbstractRandomGenerator rng = null;
        try {
            rng = AbstractRandomGenerator.factory(randGenClassName, seed);
            if (randGenProfilePath != null) rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        this.m_rng = rng;

        // Tuple Code to Tuple Id Mapping
        for (String xref[] : CODE_TO_ID_COLUMNS) {
            assert(xref.length == 2);
            this.code_columns.put(xref[0], xref[1]);
            this.code_id_xref.put(xref[1], new HashMap<String, Long>());
            if (debug.get()) LOG.debug(String.format("Added mapping from Code Column '%s' to Id Column '%s'", xref[0], xref[1]));
        } // FOR
        
        // Foreign Key Code to Ids Mapping
        // In this data structure, the key will be the name of the dependent column
        // and the value will be the name of the foreign key parent column
        // We then use this in conjunction with the Key->Id mapping to turn a code into
        // a foreign key column id. For example, if the child table AIRPORT has a column with a foreign
        // key reference to COUNTRY.CO_ID, then the data file for AIRPORT will have a value
        // 'USA' in the AP_CO_ID column. We can use mapping to get the id number for 'USA'.
        // Long winded and kind of screwy, but hey what else are you going to do?
        for (Table catalog_tbl : CatalogUtil.getDatabase(this.getCatalog()).getTables()) {
            for (Column catalog_col : catalog_tbl.getColumns()) {
                Column catalog_fkey_col = CatalogUtil.getForeignKeyParent(catalog_col);
                if (catalog_fkey_col != null && this.code_id_xref.containsKey(catalog_fkey_col.getName())) {
                    this.fkey_value_xref.put(catalog_col.getName(), catalog_fkey_col.getName());
                    if (debug.get()) LOG.debug(String.format("Added ForeignKey mapping from %s to %s", catalog_col.fullName(), catalog_fkey_col.fullName()));
                }
            } // FOR
        } // FOR
    }
    
    // ----------------------------------------------------------------
    // DATA ACCESS METHODS
    // ----------------------------------------------------------------
    
    public File getAirlineDataDir() {
        return this.airline_data_dir;
    }
    
    private Map<String, Long> getCodeXref(String col_name) {
        Map<String, Long> m = this.code_id_xref.get(col_name);
        assert(m != null) : "Invalid code xref mapping column '" + col_name + "'";
        assert(m.isEmpty() == false) : "Empty code xref mapping for column '" + col_name + "'";
        return (m);
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
    
    // ----------------------------------------------------------------
    // HISTOGRAM METHODS
    // ----------------------------------------------------------------
    
    /**
     * Return the histogram for the given name
     * @param name
     * @return
     */
    public Histogram<String> getHistogram(String name) {
        Histogram<String> h = this.histograms.get(name);
        assert(h != null) : "Invalid histogram '" + name + "'";
        return (h);
    }
    
    public Histogram<String> getTotalAirportFlightsHistogram() {
        // TODO
        return (null);
    }
    
    /**
     * 
     * @param airport_code
     * @return
     */
    public Histogram<String> getFightsPerAirportHistogram(String airport_code) {
        return (this.airport_histograms.get(airport_code));
    }
    
    public int getHistogramCount() {
        return (this.histograms.size());
    }
    
    /**
     * Load all the histograms used in the benchmark
     */
    protected void loadHistograms() {
        LOG.info(String.format("Loading in %d histograms from files stored in '%s'",
                               AirlineConstants.HISTOGRAM_DATA_FILES.length, this.airline_data_dir));
        
        // Now load in the histograms that we will need for generating the flight data
        for (String histogramName : AirlineConstants.HISTOGRAM_DATA_FILES) {
            if (this.histograms.containsKey(histogramName)) {
                LOG.warn("Already loaded histogram '" + histogramName + "'. Skipping...");
                continue;
            }
            if (debug.get()) LOG.debug("Loading in histogram data file for '" + histogramName + "'");
            Histogram<String> hist = null;
            
            try {
                // The Flights_Per_Airport histogram is actually a serialized map that has a histogram
                // of the departing flights from each airport to all the others
                if (histogramName.equals(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT)) {
                    Map<String, Histogram<String>> m = HistogramUtil.loadAirportFlights(airline_data_dir);
                    assert(m != null);
                    if (debug.get()) LOG.debug(String.format("Loaded %d airport flight histograms", m.size()));
                    
                    // Store the airport codes information
                    this.airport_histograms.putAll(m);
                    this.airport_codes.addAll(m.keySet());
                    
                    // We then need to flatten all of the histograms in this map into a single histogram
                    // that just counts the number of departing flights per airport. We will use this
                    // to get the distribution of where Customers are located
                    hist = new Histogram<String>();
                    for (Entry<String, Histogram<String>> e : m.entrySet()) {
                        hist.put(e.getKey(), e.getValue().getSampleCount());
                    } // FOR
                    
                // All other histograms are just serialized and can be loaded directly
                } else {
                    hist = HistogramUtil.loadHistogram(histogramName, this.airline_data_dir, true);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load histogram '" + histogramName + "'", ex);
            }
            assert(hist != null);
            this.histograms.put(histogramName, hist);
            if (debug.get()) LOG.debug(String.format("Loaded histogram '%s' [sampleCount=%d, valueCount=%d]",
                                                     histogramName, hist.getSampleCount(), hist.getValueCount()));
        } // FOR

    }
    
    // ----------------------------------------------------------------
    // RANDOM GENERATION METHODS
    // ----------------------------------------------------------------
    
    /**
     * Return a random airport id
     * @return
     */
    public long getRandomAirportId() {
        return (m_rng.nextInt((int)this.getAirportCount()));
    }
    
    /**
     * Return a random flight
     * @return
     */
    public FlightId getRandomFlightId() {
        int start_idx = (int)this.getFlightIdStartingOffset();
        int end_idx = (int)this.getFlightIdCount();
        int idx = this.m_rng.number(start_idx, end_idx - 1);
        FlightId id = this.getFlightId(idx);
        assert(id == null) : "Invalid FlightId offset '" + idx + "'";
        return (id);
    }
    
    /**
     * Return a random customer id based at the given airport_id 
     * @param airport_id
     * @return
     */
    public CustomerId getRandomCustomerId(long airport_id) {
        int max_id = this.getCustomerIdCount(airport_id).intValue();
        int base_id = this.m_rng.nextInt(max_id);
        return (new CustomerId(base_id, airport_id));
    }
    
    /**
     * Return a random customer id based out of any airport 
     * @return
     */
    public CustomerId getRandomCustomerId() {
        Long airport_id = null;
        int num_airports = this.getAirportCount();
        if (trace.get()) LOG.trace(String.format("Selecting a random airport with customers [numAirports=%d]", num_airports));
        while (airport_id == null) {
            airport_id = (long)this.m_rng.number(1, num_airports);
            Long cnt = this.getCustomerIdCount(airport_id); 
            if (cnt != null) {
                if (trace.get()) LOG.trace(String.format("Selected airport '%s' [numCustomers=%d]", this.getAirportCode(airport_id), cnt));
                break;
            }
            airport_id = null;
        } // WHILE
        return (this.getRandomCustomerId(airport_id));
    }
    
    /**
     * Return a random airline id
     * @return
     */
    public long getRandomAirlineId() {
        return (m_rng.nextInt(this.getRecordCount(AirlineConstants.TABLENAME_AIRLINE).intValue()));
    }
    
    /**
     * Return a random reservation id
     * @return
     */
    public long getRandomReservationId() {
        // FIXME
        return (0l);
    }

    /**
     * Return a random date in the future (after the start of upcoming flights)
     * @return
     */
    public Date getRandomUpcomingDate() {
        Date upcoming_start_date = this.getFlightUpcomingDate();
        int offset = m_rng.nextInt((int)this.getFlightFutureDays());
        return (new Date(upcoming_start_date.getTime() + (offset * AirlineConstants.MILISECONDS_PER_DAY)));
    }
    
    // ----------------------------------------------------------------
    // AIRPORT METHODS
    // ----------------------------------------------------------------
    
    /**
     * Return all the airport ids that we known about
     * @return
     */
    public Collection<Long> getAirportIds() {
        Map<String, Long> m = this.getCodeXref("AP_ID");
        return (m.values());
    }
    
    public Long getAirportId(String airport_code) {
        Map<String, Long> m = this.getCodeXref("AP_ID");
        return (m.get(airport_code));
    }
    
    public String getAirportCode(long airport_id) {
        Map<String, Long> m = this.getCodeXref("AP_ID");
        for (Entry<String, Long> e : m.entrySet()) {
            if (e.getValue() == airport_id) return (e.getKey());
        }
        return (null);
    }
    
    public Collection<String> getAirportCodes() {
        /// return (this.code_id_xref.get("AP_ID").keySet());
        return (this.airport_codes);
    }
    
    public Histogram<String> getAirportCustomerHistogram() {
        Histogram<String> h = new Histogram<String>();
        if (debug.get()) LOG.debug("Generating Airport-CustomerCount histogram [numAirports=" + this.getAirportCount() + "]");
        for (Entry<Long, Long> e : this.airport_max_customer_id.entrySet()) {
            long airport_id = e.getKey();
            h.put(this.getAirportCode(airport_id), e.getValue());
        } // FOR
        return (h);
    }
    
    /**
     * Return the number of airports that are part of this profile
     * @return
     */
    public int getAirportCount() {
        return (this.airport_codes.size());
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
    // FLIGHT IDS
    // -----------------------------------------------------------------
    
    public Iterable<FlightId> getFlightIds() {
        return (new Iterable<FlightId>() {
            @Override
            public Iterator<FlightId> iterator() {
                return (new Iterator<FlightId>() {
                    private int idx = 0;
                    private final int cnt = AirlineBaseClient.this.seats_remaining.size();
                    
                    @Override
                    public boolean hasNext() {
                        return (idx < this.cnt);
                    }
                    @Override
                    public FlightId next() {
                        long encoded = AirlineBaseClient.this.seats_remaining.get(this.idx++);
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
        assert(this.flight_start_date != null);
        assert(this.flight_upcoming_date != null);
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
        assert(seats >= 0) : "Invalid seat count for " + flight_id;
        return ((int)this.seats_remaining.put(id, (short)(seats - 1)));
    }
    
    // ----------------------------------------------------------------
    // AIRLINE METHODS
    // ----------------------------------------------------------------
    
    public Collection<Long> getAirlineIds() {
        Map<String, Long> m = this.getCodeXref("AL_ID");
        return (m.values());
    }
    
    public Collection<String> getAirlineCodes() {
        Map<String, Long> m = this.getCodeXref("AL_ID");
        return (m.keySet());
    }
    
    // ----------------------------------------------------------------
    // DISTANCE METHODS
    // ----------------------------------------------------------------
    
    public void setDistance(String airport0, String airport1, double distance) {
        short short_distance = (short)Math.round(distance);
        for (String a[] : new String[][] { {airport0, airport1}, { airport1, airport0 }}) {
            if (!this.airport_distances.containsKey(a[0])) {
                this.airport_distances.put(a[0], new HashMap<String, Short>());
            }
            this.airport_distances.get(a[0]).put(a[1], short_distance);
        } // FOR
    }
    
    public Integer getDistance(String airport0, String airport1) {
        assert(this.airport_distances.containsKey(airport0)) : "No distance entries for '" + airport0 + "'";
        assert(this.airport_distances.get(airport0).containsKey(airport1)) : "No distance entries from '" + airport0 + "' to '" + airport1 + "'";
        return ((int)this.airport_distances.get(airport0).get(airport1));
    }

    /**
     * For the current depart+arrive airport destinations, calculate the estimated
     * flight time and then add the to the departure time in order to come up with the
     * expected arrival time.
     * @param depart_airport
     * @param arrive_airport
     * @param depart_time
     * @return
     */
    public Date calculateArrivalTime(String depart_airport, String arrive_airport, Date depart_time) {
        Integer distance = this.getDistance(depart_airport, arrive_airport);
        assert(distance != null) : String.format("The calculated distance between '%s' and '%s' is null", depart_airport, arrive_airport);
        long flight_time = Math.round(distance / AirlineConstants.FLIGHT_TRAVEL_RATE) *  3600000;
        return (new Date((long)(depart_time.getTime() + flight_time)));
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
                        last_max_id = AirlineBaseClient.this.airport_max_customer_id.get(last_airport_id);
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
    
    public synchronized long incrementAirportCustomerCount(long airport_id) {
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
    
    // ----------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------
    
    public String toString() {
        String ret = "";
        
        ret += "Airport Codes\n";
        for (Entry<String, Long> e : this.code_id_xref.get("AP_ID").entrySet()) {
            ret += e.getKey() + " [" + e.getValue() + "]\n";
        }
        
        return (ret);
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
        JSONUtil.fieldsToJSON(stringer, this, AirlineBaseClient.class, JSONUtil.getSerializableFields(this.getClass()));
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, AirlineBaseClient.class, false, JSONUtil.getSerializableFields(this.getClass()));
    }
}
