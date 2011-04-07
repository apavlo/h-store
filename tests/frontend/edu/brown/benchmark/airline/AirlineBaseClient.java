/**
 * 
 */
package edu.brown.benchmark.airline;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltDB;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.JarReader;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.benchmark.airline.util.HistogramUtil;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.statistics.Histogram;

/**
 * @author pavlo
 *
 */
public abstract class AirlineBaseClient extends ClientMain {
    protected static final Logger LOG = Logger.getLogger(AirlineBaseClient.class);
    
    protected int m_days_past = 7;
    protected int m_days_future = 14;
    
    /**
     * Benchmark Profile
     */
    protected BenchmarkProfile m_profile;
    
    protected final String AIRLINE_DATA_DIR;
   
    protected final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();
    
    protected final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();
    
    /**
     * The airports that we actually care about
     */
    protected final Set<String> airport_codes = new HashSet<String>();
    
    /**
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
    
    /**
     * Base catalog objects that we can reference to figure out how to access Volt
     */
    protected final Catalog catalog;
    protected final Database catalog_db;

    /**
     * @param args
     */
    public AirlineBaseClient(String[] args) {
        super(args);
        
        Integer scale_factor = null;
        String profile_file = null;
        
        int seed = 0;
        String randGenClassName = RandomGenerator.class.getName();
        String randGenProfilePath = null;
        
        //
        // Data Path
        //
        if (!m_extraParams.containsKey(AirlineConstants.AIRLINE_DATA_PARAM)) {
            LOG.error("Unable to start benchmark. Missing '" + AirlineConstants.AIRLINE_DATA_PARAM + "' parameter");
            System.exit(1);
        }
        this.AIRLINE_DATA_DIR = m_extraParams.get(AirlineConstants.AIRLINE_DATA_PARAM);
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Scale Factor
            if (key.equals("scalefactor")) {
                scale_factor = Integer.parseInt(value);
            // Benchmark Profile File
            } else if (key.equals("benchmarkprofile")) {
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
            }
        } // FOR
        
        //
        // Create BenchmarkProfile
        //
        this.m_profile = new BenchmarkProfile();
        if (profile_file != null) {
            try {
                this.m_profile.load(profile_file);
            } catch (Exception ex) {
                LOG.error("Failed to load benchmark profile file '" + profile_file + "'", ex);
                System.exit(1);
            }
        }
        if (scale_factor != null) this.m_profile.setScaleFactor(scale_factor);
        
        //
        // Load Random Generator
        //
        AbstractRandomGenerator rng = null;
        try {
            rng = AbstractRandomGenerator.factory(randGenClassName, seed);
            if (randGenProfilePath != null) rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        this.m_rng = rng;
        
        // Load pre-compiled Catalog
        Catalog _catalog = null;
        try {
            _catalog = this.getCatalog();
        } catch (Exception ex) {
            LOG.error("Failed to retrieve already compiled catalog", ex);
            System.exit(1);
        }
        this.catalog = _catalog;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog);
        
        // Tuple Code to Tuple Id Mapping
        // For some tables, we want to store a unique code that can be used to map
        // to the id of a tuple. Any table that has a foreign key reference to this table
        // will use the unique code in the input data tables instead of the id. Thus, we need
        // to keep a table of how to map these codes to the ids when loading.
        String code_to_id_columns[][] = {
                {"CO_CODE_3",       "CO_ID"}, // COUNTRY
                {"AP_CODE",         "AP_ID"}, // AIRPORT
//                {"AP_POSTAL_CODE",  "AP_ID"}, // AIRPORT
                {"AL_IATA_CODE",    "AL_ID"}, // AIRLINE
        };
        for (String xref[] : code_to_id_columns) {
            this.code_columns.put(xref[0], xref[1]);
            this.code_id_xref.put(xref[1], new HashMap<String, Long>());
        } // FOR
        
        // Foreign Key Code to Ids Mapping
        // In this data structure, the key will be the name of the dependent column
        // and the value will be the name of the foreign key parent column
        // We then use this in conjunction with the Key->Id mapping to turn a code into
        // a foreign key column id. For example, if the child table AIRPORT has a column with a foreign
        // key reference to COUNTRY.CO_ID, then the data file for AIRPORT will have a value
        // 'USA' in the AP_CO_ID column. We can use mapping to get the id number for 'USA'.
        // Long winded and kind of screwy, but hey what else are you going to do?
        for (Table catalog_tbl : this.catalog_db.getTables()) {
            for (Column catalog_col : catalog_tbl.getColumns()) {
                Column catalog_fkey_col = CatalogUtil.getForeignKeyParent(catalog_col);
                if (catalog_fkey_col != null && this.code_id_xref.containsKey(catalog_fkey_col.getName())) {
                    this.fkey_value_xref.put(catalog_col.getName(), catalog_fkey_col.getName());
                }
            } // FOR
        } // FOR
    }
    
    /**
     * Return the histogram for the given name
     * @param name
     * @return
     */
    public Histogram<String> getHistogram(String name) {
        return (this.histograms.get(name));
    }
    
    /**
     * Return all the airport ids that we known about
     * @return
     */
    public Collection<Long> getAirportIds() {
        assert(this.code_id_xref.containsKey("AP_ID"));
        return (this.code_id_xref.get("AP_ID").values());
    }
    
    public long getAirportId(String airport_code) {
        assert(this.code_id_xref.containsKey("AP_ID"));
        return (this.code_id_xref.get("AP_ID").get(airport_code));
    }
    
    public String debug() {
        String ret = "";
        
        ret += "Airport Codes\n";
        for (Entry<String, Long> e : this.code_id_xref.get("AP_ID").entrySet()) {
            ret += e.getKey() + " [" + e.getValue() + "]\n";
        }
        
        return (ret);
    }
    
    public String getAirportCode(long airport_id) {
        for (Entry<String, Long> e : this.code_id_xref.get("AP_ID").entrySet()) {
            if (e.getValue() == airport_id) return (e.getKey());
        }
        return (null);
    }
    
    /**
     * Load all the histograms used in the benchmark
     */
    protected void loadHistograms() {
        // Now load in the histograms that we will need for generating the flight data
        try {
            for (String histogram_name : AirlineConstants.HISTOGRAM_DATA_FILES) {
                LOG.debug("Loading in histogam data file for '" + histogram_name + "'");
                
                if (histogram_name.equals(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT)) {
                    Map<String, Histogram<String>> m = HistogramUtil.loadAirportFlights(AIRLINE_DATA_DIR);
                    assert(m != null);
                    
                    // Store the airport codes set
                    this.airport_histograms.putAll(m);
                    this.airport_codes.addAll(m.keySet());
                    
                } else {
                    Histogram<String> histogram = HistogramUtil.loadHistogram(histogram_name, AIRLINE_DATA_DIR, true);
                    assert(histogram != null);
                    this.histograms.put(histogram_name, histogram);
                }

//                if (!histogram_name.equals(AirlineConstants.HISTOGRAM_POPULATIONS)) {
//                    System.out.println(histogram_name);
//                    System.out.println(histogram);
//                    System.out.println("--------------------------------------------");
//                }
                
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to load data files for histograms", ex);
            System.exit(1);
        }
    }
    
    protected Set<String> getAirportCodes() {
        /// return (this.code_id_xref.get("AP_ID").keySet());
        return (this.airport_codes);
    }
    
    protected Pair<String, String> getAirportCodes(String code_pair) {
        String codes[] = code_pair.split("-");
        return (Pair.of(codes[0], codes[1]));
    }
    
    protected void setDistance(String airport0, String airport1, double distance) {
        short short_distance = (short)Math.round(distance);
        for (String a[] : new String[][] { {airport0, airport1}, { airport1, airport0 }}) {
            if (!this.airport_distances.containsKey(a[0])) {
                this.airport_distances.put(a[0], new HashMap<String, Short>());
            }
            this.airport_distances.get(a[0]).put(a[1], short_distance);
        } // FOR
    }
    
    protected Integer getDistance(String airport0, String airport1) {
        assert(this.airport_distances.containsKey(airport0)) : "No distance entries for '" + airport0 + "'";
        assert(this.airport_distances.get(airport0).containsKey(airport1)) : "No distance entries from '" + airport0 + "' to '" + airport1 + "'";
        return ((int)this.airport_distances.get(airport0).get(airport1));
    }
    
    /**
     * Return a random flight
     * @return
     */
    protected FlightId getRandomFlightId() {
        int start_idx = (int)this.m_profile.getFlightIdStartingOffset();
        int end_idx = (int)this.m_profile.getFlightIdCount();
        int idx = this.m_rng.number(start_idx, end_idx - 1);
        FlightId id = this.m_profile.getFlightId(idx);
        assert(id == null) : "Invalid FlightId offset '" + idx + "'";
        return (id);
    }
    
    /**
     * Return a random customer id based at the given airport_id 
     * @param airport_id
     * @return
     */
    protected CustomerId getRandomCustomerId(long airport_id) {
        int max_id = this.m_profile.getCustomerIdCount(airport_id).intValue();
        int base_id = this.m_rng.nextInt(max_id);
        return (new CustomerId(base_id, airport_id));
    }
    
    /**
     * Return a random airport id
     * @return
     */
    protected long getRandomAirportId() {
        return (m_rng.nextInt((int)this.m_profile.getAirportCount()));
    }
    
    /**
     * Return a random airline id
     * @return
     */
    protected long getRandomAirlineId() {
        return (m_rng.nextInt(this.m_profile.getRecordCount(AirlineConstants.TABLENAME_AIRLINE).intValue()));
    }
    
    public long getRandomReservationId() {
        return (0l);
    }

    /**
     * Return a random date in the future (after the start of upcoming flights)
     * @return
     */
    public Date getRandomUpcomingDate() {
        Date upcoming_start_date = this.m_profile.getFlightUpcomingDate();
        int offset = m_rng.nextInt((int)this.m_profile.getFlightFutureDays());
        return (new Date(upcoming_start_date.getTime() + (offset * AirlineConstants.MILISECONDS_PER_DAY)));
    }
}
