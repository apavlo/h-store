package edu.brown.benchmark.airline;

import java.io.File;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.airline.util.*;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.TableDataIterable;

public class AirlineLoader extends AirlineBaseClient {
    
    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        org.voltdb.benchmark.ClientMain.main(AirlineLoader.class, args, true);
    }
    
    /**
     * Constructor
     * @param args
     */
    public AirlineLoader(String[] args) {
        super(args);
    }
    
    @Override
    public String[] getTransactionDisplayNames() {
        return new String[] {};
    }
    
    public void test() {
        this.runLoop();
    }

    @Override
    public void runLoop() {
        LOG.info("Begin to load tables...");
                
        //
        // Load Histograms
        //
        LOG.info("Loading data files for histograms");
        this.loadHistograms();
        
        //
        // Load the first tables from data files
        //
        LOG.info("Loading data files for fixed-sized tables");
        try {
            for (String table_name : AirlineConstants.TABLE_DATA_FILES) {
                Table catalog_tbl = catalog_db.getTables().get(table_name);
                assert(catalog_tbl != null);
                Iterable<Object[]> iterable = this.getFixedIterable(catalog_tbl);
                this.loadTable(catalog_tbl, iterable, 100000);
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to load data files for fixed-sized tables", ex);
            System.exit(1);
        }
        
        //
        // Once we have those mofos, let's go get make our flight data tables
        //
        LOG.info("Loading data files for scaling tables");
        try {
            for (String table_name : AirlineConstants.TABLE_SCALING) {
                Table catalog_tbl = catalog_db.getTables().get(table_name);
                assert(catalog_tbl != null);
                Iterable<Object[]> iterable = this.getScalingIterable(catalog_tbl); 
                this.loadTable(catalog_tbl, iterable, 100000);
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to load data files for fixed-sized tables", ex);
            System.exit(1);
        }
        
        //
        // Save the benchmark profile out to disk
        //
        try {
            this.m_profile.save("airline.profile");
        } catch (Exception ex) {
            LOG.error("Unable to save benchmark profile", ex);
            System.exit(1);
        }

        LOG.info("Airline loader done.");
    }

    /**
     * Debug
     * @param catalog_tbl
     * @param tuple
     * @param row_idx
     * @return
     */
    private String dumpTuple(Table catalog_tbl, Object tuple[], int row_idx) {
        String ret = catalog_tbl.getName() + " #" + row_idx + "\n";
        for (int i = 0; i < tuple.length; i++) {
            Column catalog_col = catalog_tbl.getColumns().get(i);
            ret += "[" + i + "]: " + tuple[i] + " - " + catalog_col.getName() + " - [" +
                   (tuple[i] != null ? tuple[i].getClass().getSimpleName() : null) +
                   "->" + VoltType.get((byte)catalog_col.getType()).toString() + "]\n";
        } // FOR
        ret += "\n";
        return (ret);
    }
    
    /**
     * 
     * @param catalog_tbl
     */
    public void loadTable(Table catalog_tbl, Iterable<Object[]> iterable, int batch_size) {
        LOG.info("Loading records for table " + catalog_tbl.getName() + " in batches of " + batch_size);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);

        Map<Integer, Integer> code_2_id = new HashMap<Integer, Integer>();
        Map<Integer, Map<String, Long>> mapping_columns = new HashMap<Integer, Map<String, Long>>();
        for (Column catalog_col : catalog_tbl.getColumns()) {
            String col_name = catalog_col.getName();
            int col_code_idx = catalog_col.getIndex();
            
            //
            // Code Column -> Id Column Mapping
            // Check to see whether this table has columns that we need to map their
            // code values to tuple ids
            //
            String col_id_name = this.code_columns.get(col_name); 
            if (col_id_name != null) {
                Column catalog_id_col = catalog_tbl.getColumns().get(col_id_name);
                assert(catalog_id_col != null) : "The id column " + catalog_tbl.getName() + "." + col_id_name + " is missing"; 
                int col_id_idx = catalog_id_col.getIndex();
                code_2_id.put(col_code_idx, col_id_idx);
            }
            
            //
            // Foreign Key Column to Code->Id Mapping
            // If this columns references a foreign key that is used in the Code->Id mapping
            // that we generating above, then we need to know when we should change the 
            // column value from a code to the id stored in our lookup table
            //
            if (this.fkey_value_xref.containsKey(col_name)) {
                String col_fkey_name = this.fkey_value_xref.get(col_name);
                mapping_columns.put(col_code_idx, this.code_id_xref.get(col_fkey_name));
            }
        } // FOR
        
        //
        // Special Case: Airport Locations
        //
        boolean is_airport = catalog_tbl.getName().equals(AirlineConstants.TABLENAME_AIRPORT);
        boolean debug = false; // catalog_tbl.getName().equals(AirlineConstants.TABLENAME_FLIGHT);
        int row_idx = -1;
        try {
            for (Object tuple[] : iterable) {
                assert(tuple[0] != null) : "The primary key for " + catalog_tbl.getName() + " is null";
                row_idx++;
                if (debug) System.out.println(this.dumpTuple(catalog_tbl, tuple, row_idx));
                
                // 
                // Code Column -> Id Column
                //
                for (int col_code_idx : code_2_id.keySet()) {
                    assert(tuple[col_code_idx] != null) : "The code column at '" + col_code_idx + "' is null for " + catalog_tbl + " id=" + tuple[0];
                    String code = tuple[col_code_idx].toString().trim();
                    String col_name = catalog_tbl.getColumns().get(code_2_id.get(col_code_idx)).getName();
                    long id = (Long)tuple[code_2_id.get(col_code_idx)];
                    if (debug) LOG.info("Mapping " + catalog_tbl.getName() + "." + catalog_tbl.getColumns().get(col_code_idx).getName() + " '" + code + "' -> " + catalog_tbl.getName() + "." + col_name + " '" + id + "'");
                    this.code_id_xref.get(col_name).put(code, id);
                } // FOR
                
                //
                // Foreign Key Code -> Foreign Key Id
                //
                for (int col_code_idx : mapping_columns.keySet()) {
                    Column catalog_col = catalog_tbl.getColumns().get(col_code_idx);
                    assert(tuple[col_code_idx] != null || catalog_col.getNullable()) : "The code column at '" + col_code_idx + "' is null for " + catalog_tbl.getName() + " id=" + tuple[0];
                    if (tuple[col_code_idx] != null) {
                        String code = tuple[col_code_idx].toString();
                        tuple[col_code_idx] = mapping_columns.get(col_code_idx).get(code);
                        if (debug) LOG.info("Mapped " + catalog_tbl.getName() + "." + catalog_tbl.getColumns().get(col_code_idx).getName() + " '" + code + "' -> '" + tuple[col_code_idx] + "'");
                    }
                } // FOR
                
                //
                // Airport: Store Locations
                //
                if (is_airport) {
                    int col_code_idx = catalog_tbl.getColumns().get("AP_CODE").getIndex();
                    int col_lat_idx = catalog_tbl.getColumns().get("AP_LATITUDE").getIndex();
                    int col_lon_idx = catalog_tbl.getColumns().get("AP_LONGITUDE").getIndex();
                    this.airport_locations.put(tuple[col_code_idx].toString(), Pair.of((Double)tuple[col_lat_idx], (Double)tuple[col_lon_idx]));
                }

                try {
                    vt.addRow(tuple);
                } catch (Exception ex) {
                    LOG.error("Failed tuple: " + this.dumpTuple(catalog_tbl, tuple, row_idx));
                    throw ex;
                }
                if (row_idx % batch_size == 0) {
                    LOG.info("Storing batch of " + batch_size + " tuples for " + catalog_tbl.getName() + " [total=" + row_idx + "]");
                    // if (debug) System.out.println(vt.toString());
                    // this.loadVoltTable(catalog_tbl.getName(), vt);
                    vt.clearRowData();
                }
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to load table " + catalog_tbl.getName(), ex);
            System.exit(1);
        }
        //
        // Record the number of tuples that we loaded for this table in the profile 
        //
        this.m_profile.setRecordCount(catalog_tbl.getName(), row_idx + 1);
        
        // if (vt.getRowCount() > 0) this.loadVoltTable(catalog_tbl.getName(), vt);
        LOG.info("Finished loading " + (row_idx + 1) + " tuples for " + catalog_tbl.getName());
        return;
    }
    
    /**
     * 
     * @param tablename
     * @param table
     */
    @SuppressWarnings("unused")
    private void loadVoltTable(String tablename, VoltTable table) {
        //Client.SyncCallback cb = new Client.SyncCallback();
        try {
            String target_proc = "@LoadMultipartitionTable";
            LOG.debug("Loading VoltTable for " + tablename + " using " + target_proc);
            m_voltClient.callProcedure(target_proc, tablename, table); 
            //m_voltClient.callProcedure(cb, target_proc, tablename, table);
            //cb.waitForResponse();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    // ----------------------------------------------------------------
    // FIXED-SIZE TABLE DATA GENERATION
    // ----------------------------------------------------------------
    
    /**
     * 
     * @param catalog_tbl
     * @return
     * @throws Exception
     */
    private Iterable<Object[]> getFixedIterable(Table catalog_tbl) throws Exception {
        String filename = AIRLINE_DATA_DIR + File.separator + "table." + catalog_tbl.getName().toLowerCase() + ".csv";
        if (!(new File(filename)).exists()) filename += ".gz";
        TableDataIterable iterable = new FixedDataIterable(catalog_tbl, filename);
        return (iterable);
    }
    
    /**
     * Wrapper around TableDataIterable that will populate additional random fields
     */
    protected class FixedDataIterable extends TableDataIterable {
        private final Set<Integer> rnd_string = new HashSet<Integer>();
        private final Map<Integer, Integer> rnd_string_min = new HashMap<Integer, Integer>();
        private final Map<Integer, Integer> rnd_string_max = new HashMap<Integer, Integer>();
        private final Set<Integer> rnd_integer = new HashSet<Integer>();
        
        public FixedDataIterable(Table catalog_tbl, String filename) throws Exception {
            super(catalog_tbl, filename, true);
            
            //
            // Figure out which columns are random integers and strings
            //
            for (Column catalog_col : catalog_tbl.getColumns()) {
                String col_name = catalog_col.getName();
                int col_idx = catalog_col.getIndex();
                if (col_name.contains("_SATTR")) {
                    this.rnd_string.add(col_idx);
                    this.rnd_string_min.put(col_idx, m_rng.nextInt(catalog_col.getSize()-1));
                    this.rnd_string_max.put(col_idx, catalog_col.getSize());
                } else if (col_name.contains("_IATTR")) {
                    this.rnd_integer.add(catalog_col.getIndex());
                }
            } // FOR
        }
        
        @Override
        public Iterator<Object[]> iterator() {
            // This is nasty old boy!
            return (new TableDataIterable.TableIterator() {
                
                @Override
                public Object[] next() {
                    Object[] tuple = super.next();
                    
                    // Random String (*_SATTR##)
                    for (int col_idx : rnd_string) {
                        int min_length = rnd_string_min.get(col_idx);
                        int max_length = rnd_string_max.get(col_idx);
                        tuple[col_idx] = m_rng.astring(min_length, max_length);
                    } // FOR
                    // Random Integer (*_IATTR##)
                    for (int col_idx : rnd_integer) {
                        tuple[col_idx] = m_rng.nextLong();
                    } // FOR

                    return (tuple);
                }
            });
        }
    } // END CLASS

    // ----------------------------------------------------------------
    // SCALING TABLE DATA GENERATION
    // ----------------------------------------------------------------
    
    /**
     * Return an iterable that spits out tuples for scaling tables
     * @param catalog_tbl the target table that we need an iterable for
     */
    private Iterable<Object[]> getScalingIterable(Table catalog_tbl) {
        String name = catalog_tbl.getName().toUpperCase();
        ScalingDataIterable it = null;
        
        // Customers
        if (name.equals(AirlineConstants.TABLENAME_CUSTOMER)) {
            long total = AirlineConstants.NUM_CUSTOMERS / this.m_profile.getScaleFactor();
            it = new CustomerIterable(catalog_tbl, total);
        // FrequentFlier
        } else if (name.equals(AirlineConstants.TABLENAME_FREQUENT_FLYER)) {
            long total = AirlineConstants.NUM_CUSTOMERS / this.m_profile.getScaleFactor();
            it = new FrequentFlyerIterable(catalog_tbl, total, AirlineConstants.MAX_FREQUENTFLYER_PER_CUSTOMER);
        // Airport Distance
        } else if (name.equals(AirlineConstants.TABLENAME_AIRPORT_DISTANCE)) {
            int max_distance = AirlineConstants.DISTANCES[AirlineConstants.DISTANCES.length - 1];
            it = new AirportDistanceIterable(catalog_tbl, max_distance);
        // Flights
        } else if (name.equals(AirlineConstants.TABLENAME_FLIGHT)) {
            it = new FlightIterable(catalog_tbl, this.m_days_past, this.m_days_future);
        // Reservations
        } else if (name.equals(AirlineConstants.TABLENAME_RESERVATION)) {
            long total = AirlineConstants.NUM_FLIGHTS_PER_DAY / this.m_profile.getScaleFactor();
            it = new ReservationIterable(catalog_tbl, total);
        } else {
            assert(false) : "Unexpected table '" + name + "' in getScalingIterable()";
        }
        assert(it != null) : "The ScalingIterable for '" + name + "' is null!";
        return (it);
    }

    /**
     * Base Iterable implementation for scaling tables 
     * Sub-classes implement the specialValue() method to generate values of a specific
     * type instead of just using the random data generators 
     */
    protected abstract class ScalingDataIterable implements Iterable<Object[]> {
        private final Table catalog_tbl;
        private final boolean special[];
        private final Object[] data;
        private final VoltType types[];
        protected long total;
        private long last_id = 0;
        
        /**
         * Constructor
         * @param catalog_tbl
         * @param table_file
         * @throws Exception
         */
        public ScalingDataIterable(Table catalog_tbl, long total) {
            this(catalog_tbl, total, new int[0]);
        }
        
        /**
         * 
         * @param catalog_tbl
         * @param total
         * @param special_columns
         * @throws Exception
         */
        public ScalingDataIterable(Table catalog_tbl, long total, int special_columns[]) {
            this.catalog_tbl = catalog_tbl;
            this.total = total;
            this.data = new Object[this.catalog_tbl.getColumns().size()];
            this.special = new boolean[this.catalog_tbl.getColumns().size()];
            
            for (int i = 0; i < this.special.length; i++) {
                this.special[i] = false;
            }
            for (int idx : special_columns) {
                this.special[idx] = true;
            }
            
            //
            // Cache the types
            //
            this.types = new VoltType[catalog_tbl.getColumns().size()];
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.types[catalog_col.getIndex()] = VoltType.get((byte)catalog_col.getType());
            } // FOR
        }
        
        /**
         * Generate a special value for this particular column index
         * @param idx
         * @return
         */
        protected abstract Object specialValue(long id, int column_idx);
        
        protected boolean hasNext() {
            return (last_id < total);
        }
        
        /**
         * Generate the iterator
         */
        public Iterator<Object[]> iterator() {
            Iterator<Object[]> it = new Iterator<Object[]>() {
                @Override
                public boolean hasNext() {
                    return (ScalingDataIterable.this.hasNext());
                }
                
                @Override
                public Object[] next() {
                    for (int i = 0; i < data.length; i++) {
                        Column catalog_col = catalog_tbl.getColumns().get(i);
                        assert(catalog_col != null) : "The column at position " + i + " for " + catalog_tbl + " is null";
                        
                        // Special Value Column
                        if (special[i]) {
                            data[i] = specialValue(last_id, i);

                        // Id column (always first unless overridden in special) 
                        } else if (i == 0) {
                            data[i] = new Long(last_id);

                        // Strings
                        } else if (types[i] == VoltType.STRING) {
                            int size = catalog_col.getSize();
                            data[i] = m_rng.astring(m_rng.nextInt(size - 1), size);
                        
                        // Ints/Longs
                        } else {
                            data[i] = m_rng.number(0, 1<<30);
                        }
                    } // FOR
                    last_id++;
                    return (data);
                }
                
                @Override
                public void remove() {
                    // Not Implemented
                }
            };
            return (it);
        }
    } // END CLASS
    
    // ----------------------------------------------------------------
    // CUSTOMERS
    // ----------------------------------------------------------------
    protected class CustomerIterable extends ScalingDataIterable {
        private final RandomDistribution.FlatHistogram<String> rand;
        private String airport_code = null;
        
        public CustomerIterable(Table catalog_tbl, long total) {
            super(catalog_tbl, total, new int[]{ 0, 1 });
            
            // Use the population histogram to select where people are located
            Histogram<String> histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_POPULATION_PER_AIRPORT);  
            this.rand = new RandomDistribution.FlatHistogram<String>(m_rng, histogram);
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (0): {
                    this.airport_code = this.rand.nextValue().toString();
                    long airport_id = AirlineLoader.this.getAirportId(this.airport_code);
                    long next_customer_id = AirlineLoader.this.m_profile.incrementAirportCustomerCount(airport_id);
                    value = new CustomerId(next_customer_id, airport_id).encode();
                    break;
                }
                // LOCAL AIRPORT
                case (1): {
                    assert(this.airport_code != null);
                    value = this.airport_code;
                    break;
                }
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    }
    
    // ----------------------------------------------------------------
    // FREQUENT_FLYER
    // ----------------------------------------------------------------
    protected class FrequentFlyerIterable extends ScalingDataIterable {
        private final Iterator<CustomerId> customer_id_iterator;
        private final short ff_per_customer[];
        private final RandomDistribution.FlatHistogram<String> airline_rand;
        private int customer_idx = 0;
        private CustomerId last_customer_id = null;
        private Set<Object> customer_airlines = new HashSet<Object>();
        private Set<String> all_airlines = new HashSet<String>();
        
        public FrequentFlyerIterable(Table catalog_tbl, long total, int max_per_customer) {
            super(catalog_tbl, total, new int[]{ 0, 1 });
            
            this.customer_id_iterator = AirlineLoader.this.m_profile.getCustomerIds().iterator();
            this.last_customer_id = this.customer_id_iterator.next();

            // Flights per Airline
            Histogram<String> histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRLINE);
            this.all_airlines = histogram.values();
            this.airline_rand = new RandomDistribution.FlatHistogram<String>(m_rng, histogram);
            
            // Loop through for the total customers and figure out how many entries we 
            // should have for each one. This will be our new total;
            this.ff_per_customer = new short[(int)total];
            RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(m_rng, 0, this.all_airlines.size(), 1.1);
            long new_total = 0; 
            total = AirlineLoader.this.m_profile.getCustomerIdCount();
            for (int i = 0; i < total; i++) {
                this.ff_per_customer[i] = (short)zipf.nextInt();
                new_total += this.ff_per_customer[i]; 
            } // FOR
            this.total = new_total;
            LOG.info("Constructing " + this.total + " FrequentFlyer tuples...");
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (0): {
                    while (this.customer_idx < this.ff_per_customer.length && this.ff_per_customer[this.customer_idx] <= 0) {
                        this.customer_idx++;
                        this.customer_airlines.clear();
                        assert(this.customer_id_iterator.hasNext());
                        this.last_customer_id = this.customer_id_iterator.next();
                    } // WHILE
                    this.ff_per_customer[this.customer_idx]--;
                    value = this.last_customer_id.encode();
                    break;
                }
                // AIRLINE ID
                case (1): {
                    assert(this.customer_airlines.size() < this.all_airlines.size());
                    do {
                        value = this.airline_rand.nextValue();
                    } while (this.customer_airlines.contains(value));
                    this.customer_airlines.add(value);
                    break;
                }
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    }
    
    // ----------------------------------------------------------------
    // AIRPORT_DISTANCE
    // ----------------------------------------------------------------
    protected class AirportDistanceIterable extends ScalingDataIterable {
        private final int max_distance;
        private final int num_airports;
        private final Set<String> record_airports;
        
        private int outer_ctr = 0;
        private String outer_airport;
        private Pair<Double, Double> outer_location;
        
        private Integer last_inner_ctr = null;
        private String inner_airport;
        private Pair<Double, Double> inner_location;
        private double distance;
        
        /**
         * Constructor
         * @param catalog_tbl
         * @param max_distance
         */
        public AirportDistanceIterable(Table catalog_tbl, int max_distance) {
            super(catalog_tbl, Long.MAX_VALUE, new int[] { 0, 1, 2 }); // total work Around
            this.max_distance = max_distance;
            this.num_airports = airport_locations.size();
            this.record_airports = getAirportCodes();
        }
        
        /**
         * Find the next two airports that are within our max_distance limit
         * We keep track of where we were in the inner loop using last_inner_ctr
         */
        @Override
        protected boolean hasNext() {
            for ( ; this.outer_ctr < (this.num_airports - 1); this.outer_ctr++) {
                this.outer_airport = airport_locations.get(this.outer_ctr);
                this.outer_location = airport_locations.getValue(this.outer_ctr);
                
                int inner_ctr = (this.last_inner_ctr != null ? this.last_inner_ctr : this.outer_ctr + 1);
                this.last_inner_ctr = null;
                for ( ; inner_ctr < this.num_airports; inner_ctr++) {
                    assert(this.outer_ctr != inner_ctr);
                    this.inner_airport = airport_locations.get(inner_ctr);
                    this.inner_location = airport_locations.getValue(inner_ctr);
                    this.distance = DistanceUtil.distance(this.outer_location, this.inner_location);

                    // Store the distance between the airports locally if either one is in our
                    // flights-per-airport data set
                    if (this.record_airports.contains(this.outer_airport) &&
                        this.record_airports.contains(this.inner_airport)) {
                        AirlineLoader.this.setDistance(this.outer_airport, this.inner_airport, this.distance);
                    }

                    // Stop here if these two airports are within range
                    if (this.distance > 0 && this.distance <= this.max_distance) {
                        // System.err.println(this.outer_airport + "->" + this.inner_airport + ": " + distance);
                        this.last_inner_ctr = inner_ctr + 1;
                        return (true);
                    }
                } // FOR
            } // FOR
            return (false);
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // OUTER AIRPORT
                case (0):
                    value = this.outer_airport;
                    break;
                // INNER AIRPORT
                case (1):
                    value = this.inner_airport;
                    break;
                // DISTANCE
                case (2):
                    value = this.distance;
                    break;
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    }
    
    // ----------------------------------------------------------------
    // FLIGHTS
    // ----------------------------------------------------------------
    protected class FlightIterable extends ScalingDataIterable {
        private final RandomDistribution.FlatHistogram<String> airlines;
        private final RandomDistribution.FlatHistogram<String> airports;
        private final RandomDistribution.FlatHistogram<String> flight_times;
        private final Pattern time_pattern = Pattern.compile("([\\d]{2,2}):([\\d]{2,2})");
        private final ListOrderedMap<Date, Integer> flights_per_day = new ListOrderedMap<Date, Integer>();
        private final Map<Long, AtomicInteger> airport_flight_ids = new HashMap<Long, AtomicInteger>();
        
        
        private int day_idx = 0;
        private Date today;
        private Date start_date;
        
        private FlightId flight_id;
        private String depart_airport;
        private String arrive_airport;
        private String airline;
        private Date depart_time;
        private Date arrive_time;
        private int status;
        
        public FlightIterable(Table catalog_tbl, int days_past, int days_future) {
            super(catalog_tbl, Long.MAX_VALUE, new int[]{ 0, 1, 2, 3, 4, 5, 6, 7 });
            assert(days_past >= 0);
            assert(days_future >= 0);
            
            // Histograms that we need:
            // (1) # of flights per airport
            // (2) # of flights per airline
            // (3) # of flights per time of day
            Histogram<String> histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRLINE);  
            this.airlines = new RandomDistribution.FlatHistogram<String>(m_rng, histogram);
            
            histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);  
            this.airports = new RandomDistribution.FlatHistogram<String>(m_rng, histogram);
            for (long airport_id : AirlineLoader.this.getAirportIds()) {
                this.airport_flight_ids.put(airport_id, new AtomicInteger(0));
            } // FOR
            
            histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_FLIGHT_DEPART_TIMES);  
            this.flight_times = new RandomDistribution.FlatHistogram<String>(m_rng, histogram);
            
            // Figure out how many flights that we want for each day
            Calendar cal = Calendar.getInstance();
            int year = cal.get(Calendar.YEAR);
            int month= cal.get(Calendar.MONTH);
            int day = cal.get(Calendar.DAY_OF_MONTH);
            
            cal.clear();
            cal.set(year, month, day);
            this.today = cal.getTime();
            
            this.total = 0;
            int num_flights_low = (int)Math.round(AirlineConstants.NUM_FLIGHTS_PER_DAY * 0.75);
            int num_flights_high = (int)Math.round(AirlineConstants.NUM_FLIGHTS_PER_DAY * 1.25);
            
            RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(m_rng, num_flights_low, num_flights_high);
            boolean first = true;
            for (long _date = this.today.getTime() - (days_past * AirlineConstants.MILISECONDS_PER_DAY); _date < this.today.getTime(); _date += AirlineConstants.MILISECONDS_PER_DAY) {
                Date date = new Date(_date);
                if (first) {
                    this.start_date = date;
                    AirlineLoader.this.m_profile.setFlightStartDate(date);
                    first = false;
                }
                int num_flights = gaussian.nextInt();
                this.flights_per_day.put(date, num_flights);
                this.total += num_flights;
            } // FOR
            
            // This is for upcoming flights that we want to be able to schedule
            // new reservations for in the benchmark
            AirlineLoader.this.m_profile.setFlightUpcomingDate(this.today);
            for (long _date = this.today.getTime(), last_date = this.today.getTime() + (days_future * AirlineConstants.MILISECONDS_PER_DAY); _date <= last_date; _date += AirlineConstants.MILISECONDS_PER_DAY) {
                Date date = new Date(_date);
                int num_flights = gaussian.nextInt();
                this.flights_per_day.put(date, num_flights);
                this.total += num_flights;
                // System.err.println(new Date(date).toString() + " -> " + num_flights);
            } // FOR
            
            // Update profile
            AirlineLoader.this.m_profile.setFlightPastDays(days_past);
            AirlineLoader.this.m_profile.setFlightFutureDays(days_future);
        }
        
        /**
         * Convert a time string "HH:MM" to a Date object
         * @param code
         * @return
         */
        private Date convertTimeString(Date base_date, String code) {
            Matcher m = this.time_pattern.matcher(code);
            assert(m.find()) : "Invalid time code '" + code + "'";
            int hour = Integer.valueOf(m.group(1));
            int minute = Integer.valueOf(m.group(2));
            return (new Date(base_date.getTime() + (hour * 3600000) + (minute * 60000)));
        }

        /**
         * For the current depart+arrive airport destinations, calculate the estimated
         * flight time and then add the to the departure time in order to come up with the
         * expected arrival time.
         * @return
         */
        private Date calculateArrivalTime() {
            Integer distance = AirlineLoader.this.getDistance(this.depart_airport, this.arrive_airport);
            assert(distance != null) : "The calculated distance between '" + this.depart_airport + "' and '" + this.arrive_airport + "' is null";
            long flight_time = Math.round(distance / AirlineConstants.FLIGHT_TRAVEL_RATE) *  3600000;
            return (new Date((long)(this.depart_time.getTime() + flight_time)));
        }
        
        /**
         * Select all the data elements for the current tuple
         * @param date
         */
        private void populate(Date date) {
            // Depart/Arrive Airports
            Pair<String, String> pair = getAirportCodes(this.airports.nextValue().toString());
            this.depart_airport = pair.getFirst();
            this.arrive_airport = pair.getSecond();

            // Depart/Arrive Times
            this.depart_time = this.convertTimeString(date, this.flight_times.nextValue().toString());
            this.arrive_time = this.calculateArrivalTime();

            // Airline
            this.airline = this.airlines.nextValue().toString();
            
            // Status
            this.status = 0; // TODO
            
            this.flights_per_day.put(date, this.flights_per_day.get(date) - 1);
            return;
        }
        
        /** 
         * Give each seat a 70%-90% probability of being occupied
         */
        boolean seatIsOccupied() {
            return (m_rng.number(1,100) < m_rng.number(70,90));
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // FLIGHT ID
                case (0): {
                    // Figure out what date we are currently on
                    Integer remaining = null;
                    Date date; 
                    do {
                        if (remaining != null) this.day_idx++;
                        date = this.flights_per_day.get(this.day_idx);
                        remaining = this.flights_per_day.getValue(this.day_idx);
                    } while (remaining <= 0 && this.day_idx + 1 < this.flights_per_day.size());
                    this.populate(date);
                    
                    // Generate a composite FlightId
                    long depart_airport_id = AirlineLoader.this.getAirportId(this.depart_airport);
                    long arrive_airport_id = AirlineLoader.this.getAirportId(this.arrive_airport);
                    long base_id = this.airport_flight_ids.get(depart_airport_id).getAndIncrement(); 
                    this.flight_id = new FlightId(base_id, depart_airport_id, arrive_airport_id, this.start_date, this.depart_time);
                    AirlineLoader.this.m_profile.addFlightId(this.flight_id);
                    value = this.flight_id.encode();
                    break;
                }
                // AIRLINE ID
                case (1): {
                    value = this.airline;
                    break;
                }
                // DEPART AIRPORT
                case (2): {
                    value = this.depart_airport;
                    break;
                }
                // DEPART TIME
                case (3): {
                    value = this.depart_time;
                    break;
                }
                // ARRIVE AIRPORT
                case (4): {
                    value = this.arrive_airport;
                    break;
                }
                // ARRIVE TIME
                case (5): {
                    value = this.arrive_time;
                    break;
                }
                // FLIGHT STATUS
                case (6): {
                    value = this.status;
                    break;
                }
                // SEATS REMAINING
                case (7): {
                    // We have to figure this out ahead of time since we need to populate the tuple now
                    for (int seatnum = 0; seatnum < AirlineConstants.NUM_SEATS_PER_FLIGHT; seatnum++) {
                        if (!this.seatIsOccupied()) continue;
                        AirlineLoader.this.m_profile.decrementFlightSeat(this.flight_id);
                    } // FOR
                    value = new Long(AirlineLoader.this.m_profile.getFlightRemainingSeats(this.flight_id));
                    break;
                }
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    }
    
    // ----------------------------------------------------------------
    // RESERVATIONS
    // ----------------------------------------------------------------
    protected class ReservationIterable extends ScalingDataIterable {
        /**
         * For each airport id, store a list of ReturnFlight objects that represent customers
         * that need return flights back to their home airport
         * ArriveAirportId -> ReturnFlights
         */
        private final Map<Long, TreeSet<ReturnFlight>> airport_returns = new HashMap<Long, TreeSet<ReturnFlight>>();
        
        /**
         * When this flag is true, then the data generation thread is finished
         */
        private boolean gen_thread_done = false;
        
        /**
         * Locks
         */
        private Semaphore gen_latch = new Semaphore(0);
        private Semaphore it_latch = new Semaphore(0);
        
        /**
         * Reservation details generated by the thread
         */
        private Long customer_id;
        private FlightId flight_id;
        private int seatnum = 1;
        
        /**
         * We use a Gaussian distribution for determining how long a customer will stay at their
         * destination before needing to return to their original airport
         */
        private final RandomDistribution.Gaussian rand_returns = new RandomDistribution.Gaussian(m_rng, 1, AirlineConstants.MAX_RETURN_FLIGHT_DAYS);
        
        /**
         * Constructor
         * @param catalog_tbl
         * @param total
         */
        public ReservationIterable(Table catalog_tbl, long total) {
            // Special Columns: R_C_ID, R_F_ID, R_F_AL_ID, R_SEAT
            super(catalog_tbl, total, new int[] { 1, 2, 3, 4 });
            
            for (long airport_id : AirlineLoader.this.getAirportIds()) {
                // Return Flights per airport
                this.airport_returns.put(airport_id, new TreeSet<ReturnFlight>());
            } // FOR
            
            // Data Generation Thread
            new Thread() { 
                public void run() {
                    LOG.debug("Data generation thread started");
                    
                    // Loop through the flights and generate reservations
                    for (FlightId flight_id : AirlineLoader.this.m_profile.getFlightIds()) {
                        LOG.debug("Flight Id: " + flight_id);
                        long depart_airport_id = flight_id.getDepartAirportId();
                        long arrive_airport_id = flight_id.getArriveAirportId();
                        Date flight_date = flight_id.getDepartDate(AirlineLoader.this.m_profile.getFlightStartDate());
                        
                        Set<CustomerId> flight_customer_ids = new HashSet<CustomerId>();
                        List<ReturnFlight> returning_customers = ReservationIterable.this.getReturningCustomers(flight_id);
                        LOG.debug("Returning Customers[" + flight_id + "]: " + returning_customers);
                        int booked_seats = AirlineConstants.NUM_SEATS_PER_FLIGHT - AirlineLoader.this.m_profile.getFlightRemainingSeats(flight_id);
                        for (int seatnum = 0; seatnum < booked_seats; seatnum++) {
                            CustomerId customer_id;
                            AirlineLoader.this.m_profile.decrementFlightSeat(flight_id);
                            
                            // Always book returning customers first
                            if (!returning_customers.isEmpty()) {
                                ReturnFlight return_flight = returning_customers.remove(0); 
                                customer_id = return_flight.getCustomerId();
                                LOG.debug("Booked return flight: " + return_flight + " [remaining=" + returning_customers.size() + "]");
                                    
                            // New Outbound Reservation
                            } else {
                                do {
                                    // TODO: Need a way to include random customers that aren't 
                                    //       considered local to the depart airport
                                    customer_id = AirlineLoader.this.getRandomCustomerId(depart_airport_id);
                                } while (flight_customer_ids.contains(customer_id));

                                // Randomly decide when this customer will return (if at all)
                                int rand = m_rng.nextInt(100);
                                if (rand <= AirlineConstants.PROB_SINGLE_FLIGHT_RESERVATION) {
                                    // Do nothing for now...
                                    
                                // Create a ReturnFlight object to record that this customer needs a flight
                                // back to their original depart airport
                                } else {
                                    int return_days = rand_returns.nextInt();
                                    ReturnFlight return_flight = new ReturnFlight(customer_id, depart_airport_id, flight_date, return_days);
                                    ReservationIterable.this.airport_returns.get(arrive_airport_id).add(return_flight);
                                }
                            }
                            assert(!flight_customer_ids.contains(customer_id));
                            flight_customer_ids.add(customer_id);
                    
                            try {
                                LOG.debug("New reservation ready. Acquiring gen_latch");
                                ReservationIterable.this.gen_latch.acquire();
                                LOG.debug("Got gen_latch!");
                            } catch (InterruptedException ex) {
                                ex.printStackTrace();
                                return;
                            }
                            ReservationIterable.this.customer_id = customer_id.encode();
                            ReservationIterable.this.flight_id = flight_id;
                            ReservationIterable.this.seatnum = seatnum;
                            LOG.debug("Releasing it_latch!");
                            ReservationIterable.this.it_latch.release();
                        } // FOR (seats)
                    } // FOR (flights)
                    LOG.info("Data generation thread is finished");
                    ReservationIterable.this.gen_thread_done = true;
                } // run
            }.start();
        }
        
        
        /**
         * Return a list of the customers that need to return to their original
         * location on this particular flight.
         * @param flight_id
         * @return
         */
        private List<ReturnFlight> getReturningCustomers(FlightId flight_id) {
            Date flight_date = flight_id.getDepartDate(AirlineLoader.this.m_profile.getFlightStartDate());
            List<ReturnFlight> found = new ArrayList<ReturnFlight>();
            TreeSet<ReturnFlight> returns = this.airport_returns.get(flight_id.getDepartAirportId());
            if (!returns.isEmpty()) {
                for (ReturnFlight return_flight : returns) {
                    if (return_flight.getReturnDate().compareTo(flight_date) > 0) break;
                    if (return_flight.getReturnAirportId() == flight_id.getArriveAirportId()) {
                        found.add(return_flight);
                    }
                } // FOR
                if (!found.isEmpty()) returns.removeAll(found);
            }
            return (found);
        }
        
        @Override
        protected boolean hasNext() {
            // Release the data generation thread in order to populate our global data structures
            LOG.debug("hasNext() called");
            if (!this.gen_thread_done) {
                LOG.debug("Releasing gen_latch!");
                this.gen_latch.release();
                try {
                    LOG.debug("Acquring it_latch");
                    this.it_latch.acquire();
                    LOG.debug("Got it_latch!! Suck it!");
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
                return (true);
            }
            return (false);
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (1): {
                    value = this.customer_id;
                    break;
                }
                // FLIGHT ID
                case (2): {
                    value = this.flight_id.encode();
                    if (AirlineLoader.this.m_profile.getReservationUpcomingOffset() == null &&
                        this.flight_id.isUpcoming(AirlineLoader.this.m_profile.getFlightStartDate(), m_profile.getFlightPastDays())) {
                        AirlineLoader.this.m_profile.setReservationUpcomingOffset(id);
                    }
                    break;
                }
                // SEAT
                case (3): {
                    value = this.seatnum;
                    break;
                }
                // STATUS
                case (4): {
                    break;
                }
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    } // END CLASS
    
    @Override
    public String getApplicationName() {
        return "Airline Benchmark";
    }

    @Override
    public String getSubApplicationName() {
        return "Loader";
    }
}

