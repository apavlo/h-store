/**
 * 
 */
package edu.brown.benchmark.airline;

import java.io.File;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.benchmark.airline.util.HistogramUtil;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreConf;

/**
 * @author pavlo
 *
 */
public abstract class AirlineBaseClient extends BenchmarkComponent {
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
    
    /** Benchmark Profile */
    protected BenchmarkProfile profile;
    
    /** Data Directory */
    private final File airline_data_dir;
   
    /** TODO */
    protected final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();
    
    /**
     * Each AirportCode will have a histogram of the number of flights 
     * that depart from that airport to all the other airports
     */
    protected final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();
    
    /** The airports that we actually care about */
    protected final Set<String> airport_codes = new HashSet<String>();
    
    /**
     * Mapping from Airports to their geolocation coordinates
     * AirportCode -> <Latitude, Longitude>
     */
    protected final ListOrderedMap<String, Pair<Double, Double>> airport_locations = new ListOrderedMap<String, Pair<Double,Double>>();
    
    /**
     * AirportCode -> Set<AirportCode, Distance>
     * Only store the records for those airports in HISTOGRAM_FLIGHTS_PER_AIRPORT
     */
    protected final Map<String, Map<String, Short>> airport_distances = new HashMap<String, Map<String, Short>>();
    
    /**
     * Foreign Key Mappings
     * Column Name -> Xref Mapper
     */
    protected final Map<String, String> fkey_value_xref = new HashMap<String, String>();

    /**
     * Key -> Id Mappings
     */
    protected final Map<String, String> code_columns = new HashMap<String, String>();
    protected final Map<String, Map<String, Long>> code_id_xref = new HashMap<String, Map<String, Long>>();
    
    /**
     * Specialized random number generator
     */
    protected final AbstractRandomGenerator m_rng;
    
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
            if (key.equals("benchmarkprofile")) {
                profile_file = value;
            // Random Generator Seed
            } else if (key.equals("randomseed")) {
                seed = Integer.parseInt(value);
            // Random Generator Class
            } else if (key.equals("randomgenerator")) {
                randGenClassName = value;
            // Random Generator Profile File
            } else if (key.equals("randomprofile")) {
                randGenProfilePath = value;
            // Data Directory
            // Parameter that points to where we can find the initial data files
            } else if (key.equals("datadir")) {
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
        this.profile = new BenchmarkProfile();
        this.profile.setScaleFactor(hstore_conf.client.scalefactor);
        if (profile_file != null) {
            try {
                this.profile.load(profile_file, null);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load benchmark profile file '" + profile_file + "'", ex);
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
    
    /**
     * Load all the histograms used in the benchmark
     */
    protected void loadHistograms() {
        LOG.info(String.format("Loading in %d histograms from files stored in '%s'",
                               AirlineConstants.HISTOGRAM_DATA_FILES.length, this.airline_data_dir));
        
        // Now load in the histograms that we will need for generating the flight data
        try {
            for (String histogram_name : AirlineConstants.HISTOGRAM_DATA_FILES) {
                if (debug.get()) LOG.debug("Loading in histogram data file for '" + histogram_name + "'");
                Histogram<String> hist = null;
                
                // The Flights_Per_Airport histogram is actually a serialized map that has a histogram
                // of the departing flights from each airport to all the others
                if (histogram_name.equals(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT)) {
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
                    hist = HistogramUtil.loadHistogram(histogram_name, this.airline_data_dir, true);
                }
                assert(hist != null);
                this.histograms.put(histogram_name, hist);
                if (debug.get()) LOG.debug(String.format("Loaded histogram '%s' [sampleCount=%d, valueCount=%d]",
                                                         histogram_name, hist.getSampleCount(), hist.getValueCount()));
            } // FOR
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load data files for histograms", ex);
        }
    }
    
    // ----------------------------------------------------------------
    // RANDOM GENERATION METHODS
    // ----------------------------------------------------------------
    
    /**
     * Return a random airport id
     * @return
     */
    public long getRandomAirportId() {
        return (m_rng.nextInt((int)this.profile.getAirportCount()));
    }
    
    /**
     * Return a random flight
     * @return
     */
    public FlightId getRandomFlightId() {
        int start_idx = (int)this.profile.getFlightIdStartingOffset();
        int end_idx = (int)this.profile.getFlightIdCount();
        int idx = this.m_rng.number(start_idx, end_idx - 1);
        FlightId id = this.profile.getFlightId(idx);
        assert(id == null) : "Invalid FlightId offset '" + idx + "'";
        return (id);
    }
    
    /**
     * Return a random customer id based at the given airport_id 
     * @param airport_id
     * @return
     */
    public CustomerId getRandomCustomerId(long airport_id) {
        int max_id = this.profile.getCustomerIdCount(airport_id).intValue();
        int base_id = this.m_rng.nextInt(max_id);
        return (new CustomerId(base_id, airport_id));
    }
    
    /**
     * Return a random customer id based out of any airport 
     * @return
     */
    public CustomerId getRandomCustomerId() {
        Long airport_id = null;
        while (airport_id == null) {
            airport_id = (long)this.m_rng.number(1, this.profile.getAirportCount());
            if (this.profile.getCustomerIdCount(airport_id) != null) break;
            airport_id = null;
        } // WHILE
        return (this.getRandomCustomerId(airport_id));
    }
    
    /**
     * Return a random airline id
     * @return
     */
    public long getRandomAirlineId() {
        return (m_rng.nextInt(this.profile.getRecordCount(AirlineConstants.TABLENAME_AIRLINE).intValue()));
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
        Date upcoming_start_date = this.profile.getFlightUpcomingDate();
        int offset = m_rng.nextInt((int)this.profile.getFlightFutureDays());
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
}
