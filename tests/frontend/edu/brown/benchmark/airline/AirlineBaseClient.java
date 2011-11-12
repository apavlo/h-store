/**
 * 
 */
package edu.brown.benchmark.airline;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.benchmark.airline.util.HistogramUtil;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;
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
    // SERIALIZABLE DATA MEMBERS
    // ----------------------------------------------------------------
    
    /**
     * Data Scale Factor
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
    public TimestampType flight_start_date;
    /**
     * The date for when the flights are considered upcoming and are eligible for reservations
     */
    public TimestampType flight_upcoming_date;
    /**
     * The number of days in the past that our flight data set includes.
     */
    public long flight_past_days;
    /**
     * The number of days in the future (from the flight_upcoming_date) that our flight data set includes
     */
    public long flight_future_days;
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
     * We want to maintain a small cache of FlightIds so that the AirlineClient
     * has something to work with. We obviously don't want to store the entire set here
     */
    private final LinkedList<FlightId> cached_flight_ids = new LinkedList<FlightId>();

    /** TODO */
    public final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();
    
    /**
     * Each AirportCode will have a histogram of the number of flights 
     * that depart from that airport to all the other airports
     */
    public final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();

    /**
     * Key -> Id Mappings
     */
    public final Map<String, String> code_columns = new HashMap<String, String>();
    public final Map<String, Map<String, Long>> code_id_xref = new HashMap<String, Map<String, Long>>();
    
    /**
     * Foreign Key Mappings
     * Column Name -> Xref Mapper
     */
    public final Map<String, String> fkey_value_xref = new HashMap<String, String>();
    
    // ----------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------
    
    /** Data Directory */
    private transient final File airline_data_dir;
    
    /**
     * Specialized random number generator
     */
    protected transient final AbstractRandomGenerator rng;
    
    protected transient File profile_file = null;
    
    // ----------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------
    
    /**
     * Constructor
     * @param args
     */
    public AirlineBaseClient(String[] args) {
        super(args);
        
        String data_dir = null;
        int seed = 0;
        String randGenClassName = RandomGenerator.class.getName();
        String randGenProfilePath = null;
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Benchmark Profile File
            if (key.equalsIgnoreCase("profile")) {
                this.profile_file = new File(value);
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
            throw new RuntimeException("Unable to start benchmark. Missing 'datadir' parameter\n" + StringUtil.formatMaps(m_extraParams)); 
        }
        this.airline_data_dir = new File(data_dir);
        if (this.airline_data_dir.exists() == false) {
            throw new RuntimeException("Unable to start benchmark. The data directory '" + this.airline_data_dir.getAbsolutePath() + "' does not exist");
        }
        
        // Load our good old friend Mister HStoreConf
        HStoreConf hstore_conf = this.getHStoreConf();
        
        // Create BenchmarkProfile
        this.setScaleFactor(hstore_conf.client.scalefactor);
        if (this.profile_file != null) {
            try {
                this.load(this.profile_file.getAbsolutePath(), null);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load benchmark profile '" + this.profile_file + "'", ex);
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
        this.rng = rng;

        // Tuple Code to Tuple Id Mapping
        for (String xref[] : CODE_TO_ID_COLUMNS) {
            assert(xref.length == 2);
            if (this.code_columns.containsKey(xref[0]) == false) {
                this.code_columns.put(xref[0], xref[1]);
                this.code_id_xref.put(xref[1], new HashMap<String, Long>());
                if (debug.get()) LOG.debug(String.format("Added mapping from Code Column '%s' to Id Column '%s'", xref[0], xref[1]));
            }
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

    /**
     * 
     * @param airport_code
     * @return
     */
    public Histogram<String> getFightsPerAirportHistogram(String airport_code) {
        return (this.airport_histograms.get(airport_code));
    }
    
    /**
     * Returns the number of histograms that we have loaded
     * Does not include the airport_histograms
     * @return
     */
    public int getHistogramCount() {
        return (this.histograms.size());
    }
    
    /**
     * Load all the histograms used in the benchmark
     */
    protected void loadHistograms() {
        if (debug.get()) LOG.debug(String.format("Loading in %d histograms from files stored in '%s'",
                                                 AirlineConstants.HISTOGRAM_DATA_FILES.length, this.airline_data_dir));
        
        // Now load in the histograms that we will need for generating the flight data
        for (String histogramName : AirlineConstants.HISTOGRAM_DATA_FILES) {
            if (this.histograms.containsKey(histogramName)) {
                if (debug.get()) LOG.warn("Already loaded histogram '" + histogramName + "'. Skipping...");
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
        return (rng.number(1, (int)this.getAirportCount()));
    }
    
    public long getRandomOtherAirport(long airport_id) {
        String code = this.getAirportCode(airport_id);
        Histogram<String> h = this.airport_histograms.get(code);
        assert(h != null);
        FlatHistogram<String> f = new FlatHistogram<String>(rng, h);
        String other = f.nextValue();
        return this.getAirportId(other);
    }
    
    /**
     * Return a random customer id based at the given airport_id 
     * @param airport_id
     * @return
     */
    public CustomerId getRandomCustomerId(long airport_id) {
        Long cnt = this.getCustomerIdCount(airport_id);
        if (cnt != null) {
            int max_id = cnt.intValue();
            int base_id = rng.nextInt(max_id);
            return (new CustomerId(base_id, airport_id));
        }
        return (null);
    }
    
    /**
     * Return a random customer id based out of any airport 
     * @return
     */
    public CustomerId getRandomCustomerId() {
        Long airport_id = null;
        int num_airports = this.airport_max_customer_id.size();
        if (trace.get()) LOG.trace(String.format("Selecting a random airport with customers [numAirports=%d]", num_airports));
        while (airport_id == null) {
            airport_id = (long)this.rng.number(1, num_airports);
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
        return (rng.nextInt(this.getRecordCount(AirlineConstants.TABLENAME_AIRLINE).intValue()));
    }

    /**
     * Return a random date in the future (after the start of upcoming flights)
     * @return
     */
    public TimestampType getRandomUpcomingDate() {
        TimestampType upcoming_start_date = this.flight_upcoming_date;
        int offset = rng.nextInt((int)this.getFlightFutureDays());
        return (new TimestampType(upcoming_start_date.getTime() + (offset * AirlineConstants.MICROSECONDS_PER_DAY)));
    }
    
    /**
     * Return a random FlightId from our set of cached ids
     * @return
     */
    public FlightId getRandomFlightId() {
        assert(this.cached_flight_ids.isEmpty() == false);
        FlightId ret = null;
        
        // Grab a random FlightId from our cache and push it back to the end
        // of the list. That way the order of the cache goes from MRU -> LRU
        synchronized (this.cached_flight_ids) {
            int idx = rng.nextInt(this.cached_flight_ids.size());
            ret = this.cached_flight_ids.remove(idx);
            this.cached_flight_ids.addFirst(ret);
        } // SYNCH
        return (ret);
    }
    
    // ----------------------------------------------------------------
    // AIRPORT METHODS
    // ----------------------------------------------------------------
    
    /**
     * Return all the airport ids that we know about
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
        return (this.getCodeXref("AP_ID").keySet());
    }
    
    /**
     * Return the number of airports that are part of this profile
     * @return
     */
    public int getAirportCount() {
        return (this.getAirportCodes().size());
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
    
    public Collection<String> getAirportsWithFlights() {
        return this.airport_histograms.keySet();
    }
    
    public boolean hasFlights(String airport_code) {
        Histogram<String> h = this.getFightsPerAirportHistogram(airport_code);
        if (h != null) {
            return (h.getSampleCount() > 0);
        }
        return (false);
    }
    
    // -----------------------------------------------------------------
    // FLIGHTS
    // -----------------------------------------------------------------
    
    /**
     * Add a new FlightId for this benchmark instance
     * This method will decide whether to store the id or not in its cache
     * @return True if the FlightId was added to the cache
     */
    public boolean addFlightId(FlightId flight_id) {
        boolean added = false;
        synchronized (this.cached_flight_ids) {
            // If we have room, shove it right in
            // We'll throw it in the back because we know it hasn't been used yet
            if (this.cached_flight_ids.size() < AirlineConstants.CACHED_FLIGHT_ID_SIZE) {
                this.cached_flight_ids.addLast(flight_id);
                added = true;
            
            // Otherwise, we can will randomly decide whether to pop one out
            } else if (rng.nextBoolean()) {
                this.cached_flight_ids.pop();
                this.cached_flight_ids.addLast(flight_id);
                added = true;
            }
        } // SYNCH
        return (added);
    }
    
    public long getFlightIdCount() {
        return (this.cached_flight_ids.size());
    }
    
    // -----------------------------------------------------------------
    // FLIGHT DATES
    // -----------------------------------------------------------------

    /**
     * The date in which the flight data set begins
     * @return
     */
    public TimestampType getFlightStartDate() {
        return this.flight_start_date;
    }
    /**
     * 
     * @param start_date
     */
    public void setFlightStartDate(TimestampType start_date) {
        this.flight_start_date = start_date;
    }

    /**
     * The date in which the flight data set begins
     * @return
     */
    public TimestampType getFlightUpcomingDate() {
        return (this.flight_upcoming_date);
    }
    /**
     * 
     * @param startDate
     */
    public void setFlightUpcomingDate(TimestampType upcoming_date) {
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
    
    public Long getAirlineId(String airline_code) {
        Map<String, Long> m = this.getCodeXref("AL_ID");
        return (m.get(airline_code));
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
    
    public long getNextReservationId() {
        long base_id = -1;
        synchronized (this.num_records) {
            base_id = this.num_records.get(AirlineConstants.TABLENAME_RESERVATION);
            this.num_records.put(AirlineConstants.TABLENAME_RESERVATION, base_id+1);
        } // SYNCH
        // Offset it by the client id so that we can ensure it's unique
        return (this.getClientId() | base_id<<48);
    }
    
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
        JSONUtil.fieldsToJSON(stringer, this, AirlineBaseClient.class, JSONUtil.getSerializableFields(this.getClass(), "cached_flight_ids"));
        
        // CACHED FIELD IDS
        stringer.key("CACHED_FLIGHT_IDS").array();
        for (FlightId f_id : this.cached_flight_ids) {
            stringer.value(f_id.encode());
        } // FOR
        stringer.endArray();
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, AirlineBaseClient.class, false, JSONUtil.getSerializableFields(this.getClass(), "cached_flight_ids"));
        
        // CACHED FIELD IDS
        JSONArray json_arr = json_object.getJSONArray("CACHED_FLIGHT_IDS");
        this.cached_flight_ids.clear();
        for (int i = 0, cnt = json_arr.length(); i < cnt; i++) {
            FlightId f_id = new FlightId(json_arr.getLong(i));
            this.cached_flight_ids.add(f_id);
        } // FOR
    }
}
