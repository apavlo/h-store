/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.airline;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.CustomerIdIterable;
import edu.brown.benchmark.airline.util.DistanceUtil;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.benchmark.airline.util.ReturnFlight;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableDataIterable;

public class AirlineLoader extends AirlineBaseClient {
    private static final Logger LOG = Logger.getLogger(AirlineLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Mapping from Airports to their geolocation coordinates
     * AirportCode -> <Latitude, Longitude>
     */
    private final ListOrderedMap<String, Pair<Double, Double>> airport_locations = new ListOrderedMap<String, Pair<Double,Double>>();

    /**
     * AirportCode -> Set<AirportCode, Distance>
     * Only store the records for those airports in HISTOGRAM_FLIGHTS_PER_AIRPORT
     */
    private final Map<String, Map<String, Short>> airport_distances = new HashMap<String, Map<String, Short>>();

    /**
     * Store a list of FlightIds and the number of seats
     * remaining for a particular flight.
     */
    private final ListOrderedMap<FlightId, Short> seats_remaining = new ListOrderedMap<FlightId, Short>();

    /**
     * Counter for the number of tables that we have finished loading
     */
    private final transient AtomicInteger finished = new AtomicInteger(0);
    
    // -----------------------------------------------------------------
    // FLIGHT IDS
    // -----------------------------------------------------------------
    
    public Iterable<FlightId> getFlightIds() {
        return (new Iterable<FlightId>() {
            @Override
            public Iterator<FlightId> iterator() {
                return (new Iterator<FlightId>() {
                    private int idx = 0;
                    private final int cnt = seats_remaining.size();
                    
                    @Override
                    public boolean hasNext() {
                        return (idx < this.cnt);
                    }
                    @Override
                    public FlightId next() {
                        return (seats_remaining.get(this.idx++));
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
    @Override
    public boolean addFlightId(FlightId flight_id) {
        assert(flight_id != null);
        assert(this.profile.flight_start_date != null);
        assert(this.profile.flight_upcoming_date != null);
        super.addFlightId(flight_id);
        this.seats_remaining.put(flight_id, (short)AirlineConstants.NUM_SEATS_PER_FLIGHT);
        
        // XXX
        if (this.profile.flight_upcoming_offset == null &&
            this.profile.flight_upcoming_date.compareTo(flight_id.getDepartDate(this.profile.flight_start_date)) < 0) {
            this.profile.flight_upcoming_offset = (long)(this.seats_remaining.size() - 1);
        }
        return (true);
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
        return (this.profile.flight_upcoming_offset);
    }
    
    /**
     * Return flight 
     * @param index
     * @return
     */
    public FlightId getFlightId(int index) {
        assert(index >= 0);
        assert(index <= this.getFlightIdCount());
        return (this.seats_remaining.get(index));
    }

    /**
     * Return the number of seats remaining for a flight
     * @param flight_id
     * @return
     */
    public int getFlightRemainingSeats(FlightId flight_id) {
        return ((int)this.seats_remaining.get(flight_id));
    }
    
    /**
     * Decrement the number of available seats for a flight and return
     * the total amount remaining
     */
    public int decrementFlightSeat(FlightId flight_id) {
        Short seats = this.seats_remaining.get(flight_id);
        assert(seats != null) : "Missing seat count for " + flight_id;
        assert(seats >= 0) : "Invalid seat count for " + flight_id;
        return ((int)this.seats_remaining.put(flight_id, (short)(seats - 1)));
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
    public TimestampType calculateArrivalTime(String depart_airport, String arrive_airport, TimestampType depart_time) {
        Integer distance = this.getDistance(depart_airport, arrive_airport);
        assert(distance != null) : String.format("The calculated distance between '%s' and '%s' is null", depart_airport, arrive_airport);
        long flight_time = Math.round(distance / AirlineConstants.FLIGHT_TRAVEL_RATE) * 3600000000l; // 60 sec * 60 min * 1,000,000
        return (new TimestampType(depart_time.getTime() + flight_time));
    }
    
    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        edu.brown.benchmark.BenchmarkComponent.main(AirlineLoader.class, args, true);
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

    @Override
    public void runLoop() {
        LOG.info("Begin to load tables...");
        final Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        
        // Load Histograms
        LOG.info("Loading data files for histograms");
        this.loadHistograms();
        
        // Load the first tables from data files
        LOG.info("Loading data files for fixed-sized tables");
        this.loadFixedTables(catalog_db);
        
        // Once we have those mofos, let's go get make our flight data tables
        LOG.info("Loading data files for scaling tables");
        this.loadScalingTables(catalog_db);
        
        // Save the benchmark profile out to disk so that we can send it
        // to all of the clients
        this.profile.saveProfile(this);

        LOG.debug("Airline loader done.");
    }
    
    /**
     * The fixed tables are those that are generated from the static data files
     * The number of tuples in these tables will not change based on the scale factor.
     * @param catalog_db
     */
    protected void loadFixedTables(Database catalog_db) {
        for (String table_name : AirlineConstants.TABLE_DATA_FILES) {
            try {    
                Table catalog_tbl = catalog_db.getTables().get(table_name);
                assert(catalog_tbl != null);
                Iterable<Object[]> iterable = this.getFixedIterable(catalog_tbl);
                this.loadTable(catalog_tbl, iterable, 5000);
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to load data files for fixed-sized table '" + table_name + "'", ex);
            }
        } // FOR
    }
    
    /**
     * The scaling tables are things that we will scale the number of tuples based
     * on the given scaling factor at runtime 
     * @param catalog_db
     */
    protected void loadScalingTables(Database catalog_db) {
        for (String table_name : AirlineConstants.TABLE_SCALING) {
            try {
                Table catalog_tbl = catalog_db.getTables().get(table_name);
                assert(catalog_tbl != null);
                Iterable<Object[]> iterable = this.getScalingIterable(catalog_tbl); 
                this.loadTable(catalog_tbl, iterable, 5000);
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to load data files for scaling-sized table '" + table_name + "'", ex);
            }
        } // FOR
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
        // Special Case: Airport Locations
        final boolean is_airport = catalog_tbl.getName().equals(AirlineConstants.TABLENAME_AIRPORT);
        final Database catalog_db = CatalogUtil.getDatabase(catalog_tbl.getCatalog()); 
        
        if (debug.get()) LOG.debug(String.format("Generating new records for table %s [batchSize=%d]", catalog_tbl.getName(), batch_size));
        final VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        final CatalogMap<Column> columns = catalog_tbl.getColumns();

        // Check whether we have any special mappings that we need to maintain
        Map<Integer, Integer> code_2_id = new HashMap<Integer, Integer>();
        Map<Integer, Map<String, Long>> mapping_columns = new HashMap<Integer, Map<String, Long>>();
        for (Column catalog_col : columns) {
            String col_name = catalog_col.getName();
            int col_code_idx = catalog_col.getIndex();
            
            // Code Column -> Id Column Mapping
            // Check to see whether this table has columns that we need to map their
            // code values to tuple ids
            String col_id_name = this.code_columns.get(col_name); 
            if (col_id_name != null) {
                Column catalog_id_col = columns.get(col_id_name);
                assert(catalog_id_col != null) : "The id column " + catalog_tbl.getName() + "." + col_id_name + " is missing"; 
                int col_id_idx = catalog_id_col.getIndex();
                code_2_id.put(col_code_idx, col_id_idx);
            }
            
            // Foreign Key Column to Code->Id Mapping
            // If this columns references a foreign key that is used in the Code->Id mapping
            // that we generating above, then we need to know when we should change the 
            // column value from a code to the id stored in our lookup table
            if (this.fkey_value_xref.containsKey(col_name)) {
                String col_fkey_name = this.fkey_value_xref.get(col_name);
                mapping_columns.put(col_code_idx, this.profile.code_id_xref.get(col_fkey_name));
            }
        } // FOR
        
        int row_idx = 0;
        try {
            for (Object tuple[] : iterable) {
                assert(tuple[0] != null) : "The primary key for " + catalog_tbl.getName() + " is null";
                if (trace.get()) LOG.trace(this.dumpTuple(catalog_tbl, tuple, row_idx));
                
                // AIRPORT 
                if (is_airport) {
                    // Skip any airport that does not have flights
                    if (hasFlights((String)tuple[columns.get("AP_CODE").getIndex()]) == false) continue;
                    
                    // Update the row # so that it matches what we're actually loading
                    int col_id_idx = columns.get("AP_ID").getIndex();
                    tuple[col_id_idx] = (long)(row_idx + 1);
                    
                    // Store Locations
                    int col_code_idx = columns.get("AP_CODE").getIndex();
                    int col_lat_idx = columns.get("AP_LATITUDE").getIndex();
                    int col_lon_idx = columns.get("AP_LONGITUDE").getIndex();
                    Pair<Double, Double> coords = Pair.of((Double)tuple[col_lat_idx], (Double)tuple[col_lon_idx]);
                    this.airport_locations.put(tuple[col_code_idx].toString(), coords);
                }
                
                // Code Column -> Id Column
                for (int col_code_idx : code_2_id.keySet()) {
                    assert(tuple[col_code_idx] != null) : "The code column at '" + col_code_idx + "' is null for " + catalog_tbl + "\n" + Arrays.toString(tuple);
                    String code = tuple[col_code_idx].toString().trim();
                    if (code.length() > 0) {
                        Column from_column = columns.get(col_code_idx);
                        assert(from_column != null);
                        Column to_column = columns.get(code_2_id.get(col_code_idx)); 
                        assert(to_column != null) : String.format("Invalid column %s.%s", catalog_tbl.getName(), code_2_id.get(col_code_idx));  
                        long id = (Long)tuple[code_2_id.get(col_code_idx)];
                        if (trace.get()) LOG.trace(String.format("Mapping %s '%s' -> %s '%d'", from_column.fullName(), code, to_column.fullName(), id));
                        this.profile.code_id_xref.get(to_column.getName()).put(code, id);
                    }
                } // FOR
                
                // Foreign Key Code -> Foreign Key Id
                for (int col_code_idx : mapping_columns.keySet()) {
                    Column catalog_col = columns.get(col_code_idx);
                    assert(tuple[col_code_idx] != null || catalog_col.getNullable()) : "The code column at '" + col_code_idx + "' is null for " + catalog_tbl.getName() + " id=" + tuple[0];
                    if (tuple[col_code_idx] != null) {
                        String code = tuple[col_code_idx].toString();
                        tuple[col_code_idx] = mapping_columns.get(col_code_idx).get(code);
                        if (trace.get()) {
                            Column catalog_fkey_col = CatalogUtil.getForeignKeyParent(catalog_col);
                            LOG.trace(String.format("Mapped %s '%s' -> %s '%s'", catalog_col.fullName(), code, catalog_fkey_col.fullName(), tuple[col_code_idx]));
                        }
                    }
                } // FOR
                
                vt.addRow(tuple);
                if (row_idx > 0 && (row_idx+1) % batch_size == 0) {
                    // if (trace.get()) LOG.trace("Storing batch of " + batch_size + " tuples for " + catalog_tbl.getName() + " [total=" + row_idx + "]");
                    // if (debug) System.out.println(vt.toString());
                    this.loadVoltTable(catalog_tbl.getName(), vt);
                    vt.clearRowData();
                }
                row_idx++;
            } // FOR
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load table " + catalog_tbl.getName(), ex);
        }
        if (vt.getRowCount() > 0) {
            this.loadVoltTable(catalog_tbl.getName(), vt);
        }
        if (is_airport) assert(this.getAirportCount() == row_idx) : String.format("%d != %d", this.getAirportCount(), row_idx);
        
        // Record the number of tuples that we loaded for this table in the profile 
        this.setRecordCount(catalog_tbl.getName(), row_idx + 1);
        
        LOG.info(String.format("Finished loading all %d tuples for %s [%d / %d]",
                                row_idx, catalog_tbl.getName(),
                                this.finished.incrementAndGet(), catalog_db.getTables().size()));
        return;
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
    protected Iterable<Object[]> getFixedIterable(Table catalog_tbl) throws Exception {
        File f = new File(getAirlineDataDir().getAbsolutePath() + File.separator + "table." + catalog_tbl.getName().toLowerCase() + ".csv");
        if (f.exists() == false) f = new File(f.getAbsolutePath() + ".gz");
        TableDataIterable iterable = new FixedDataIterable(catalog_tbl, f);
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
        
        public FixedDataIterable(Table catalog_tbl, File filename) throws Exception {
            super(catalog_tbl, filename, true, true);
            
            // Figure out which columns are random integers and strings
            for (Column catalog_col : catalog_tbl.getColumns()) {
                String col_name = catalog_col.getName();
                int col_idx = catalog_col.getIndex();
                if (col_name.contains("_SATTR")) {
                    this.rnd_string.add(col_idx);
                    this.rnd_string_min.put(col_idx, rng.nextInt(catalog_col.getSize()-1));
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
                        tuple[col_idx] = rng.astring(min_length, max_length);
                    } // FOR
                    // Random Integer (*_IATTR##)
                    for (int col_idx : rnd_integer) {
                        tuple[col_idx] = rng.nextLong();
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
    protected Iterable<Object[]> getScalingIterable(Table catalog_tbl) {
        String name = catalog_tbl.getName().toUpperCase();
        ScalingDataIterable it = null;
        double scaleFactor = this.getScaleFactor(); 
        
        // Customers
        if (name.equals(AirlineConstants.TABLENAME_CUSTOMER)) {
            long total = Math.round(AirlineConstants.NUM_CUSTOMERS / scaleFactor);
            it = new CustomerIterable(catalog_tbl, total);
        // FrequentFlier
        } else if (name.equals(AirlineConstants.TABLENAME_FREQUENT_FLYER)) {
            long total = Math.round(AirlineConstants.NUM_CUSTOMERS / scaleFactor);
            int per_customer = (int)Math.ceil(AirlineConstants.MAX_FREQUENTFLYER_PER_CUSTOMER / scaleFactor);
            it = new FrequentFlyerIterable(catalog_tbl, total, per_customer);
        // Airport Distance
        } else if (name.equals(AirlineConstants.TABLENAME_AIRPORT_DISTANCE)) {
            int max_distance = Integer.MAX_VALUE; // AirlineConstants.DISTANCES[AirlineConstants.DISTANCES.length - 1];
            it = new AirportDistanceIterable(catalog_tbl, max_distance);
        // Flights
        } else if (name.equals(AirlineConstants.TABLENAME_FLIGHT)) {
            it = new FlightIterable(catalog_tbl, AirlineConstants.DAYS_PAST, AirlineConstants.DAYS_FUTURE);
        // Reservations
        } else if (name.equals(AirlineConstants.TABLENAME_RESERVATION)) {
            long total = Math.round(AirlineConstants.NUM_FLIGHTS_PER_DAY / scaleFactor);
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
         * @param special_columns - The offsets of the columns that we will invoke specialValue() to get their values
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
            
            // Cache the types
            this.types = new VoltType[catalog_tbl.getColumns().size()];
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.types[catalog_col.getIndex()] = VoltType.get(catalog_col.getType());
            } // FOR
        }
        
        /**
         * Generate a special value for this particular column index
         * @param idx
         * @return
         */
        protected abstract Object specialValue(long id, int column_idx);
        
        /**
         * Simple callback when the ScalingDataIterable is finished
         */
        protected void callbackFinished() {
            // Nothing...
        }
        
        protected boolean hasNext() {
            boolean has_next = (last_id < total);
            if (has_next == false) this.callbackFinished();
            return (has_next);
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
                            data[i] = rng.astring(rng.nextInt(size - 1), size);
                        
                        // Ints/Longs
                        } else {
                            data[i] = rng.number(0, 1<<30);
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
        private final RandomDistribution.Flat randBalance;
        private String airport_code = null;
        private CustomerId last_id = null;
        
        public CustomerIterable(Table catalog_tbl, long total) {
            super(catalog_tbl, total, new int[]{ 0, 1, 2 });
            
            // Use the flights per airport histogram to select where people are located
            Histogram<String> histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);  
            this.rand = new RandomDistribution.FlatHistogram<String>(rng, histogram);
            if (debug.get()) this.rand.enableHistory();
            
            this.randBalance = new RandomDistribution.Flat(rng, 1000, 10000);
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (0): {
                    // HACK: The flights_per_airport histogram may not match up exactly with the airport
                    // data files, so we'll just spin until we get a good one
                    Long airport_id = null;
                    while (airport_id == null) {
                        this.airport_code = this.rand.nextValue();
                        airport_id = AirlineLoader.this.getAirportId(this.airport_code);
                    } // WHILE
                    long next_customer_id = AirlineLoader.this.incrementAirportCustomerCount(airport_id);
                    this.last_id = new CustomerId(next_customer_id, airport_id);
                    if (trace.get()) LOG.trace("NEW CUSTOMER: " + this.last_id.encode() + " / " + this.last_id);
                    value = this.last_id.encode();
                    if (trace.get()) LOG.trace(value + " => " + this.airport_code + " [" + getCustomerIdCount(airport_id) + "]");
                    break;
                }
                // CUSTOMER ID STR
                case (1): {
                    assert(this.last_id != null);
                    value = Long.toString(this.last_id.encode());
                    this.last_id = null;
                    break;
                }
                // LOCAL AIRPORT
                case (2): {
                    assert(this.airport_code != null);
                    value = this.airport_code;
                    break;
                }
                // BALANCE
                case (3): {
                    value = MathUtil.roundToDecimals(this.randBalance.nextLong(), 2);
                    break;
                }
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
        
        @Override
        protected void callbackFinished() {
            if (trace.get()) {
                Histogram<String> h = this.rand.getHistogramHistory();
                LOG.trace(String.format("Customer Local Airports Histogram [valueCount=%d, sampleCount=%d]\n%s",
                                        h.getValueCount(), h.getSampleCount(), h.toString()));
            }
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
        private Collection<String> customer_airlines = new HashSet<String>();
        private final Collection<String> all_airlines;
        
        public FrequentFlyerIterable(Table catalog_tbl, long num_customers, int max_per_customer) {
            super(catalog_tbl, num_customers, new int[]{ 0, 1 });
            
            this.customer_id_iterator = new CustomerIdIterable(profile.airport_max_customer_id).iterator();
            this.last_customer_id = this.customer_id_iterator.next();

            // Flights per Airline
            this.all_airlines = getAirlineCodes();
            Histogram<String> histogram = new Histogram<String>();
            histogram.putAll(this.all_airlines);
            // Embed a Gaussian distribution
            RandomDistribution.Gaussian gauss_rng = new RandomDistribution.Gaussian(rng, 0, this.all_airlines.size());
            this.airline_rand = new RandomDistribution.FlatHistogram<String>(gauss_rng, histogram);
            if (trace.get()) this.airline_rand.enableHistory();
            
            // Loop through for the total customers and figure out how many entries we 
            // should have for each one. This will be our new total;
            RandomDistribution.Zipf ff_zipf = new RandomDistribution.Zipf(rng, 0, this.all_airlines.size(), 1.1);
            long new_total = 0; 
            long total = AirlineLoader.this.getCustomerIdCount();
            if (debug.get()) LOG.debug("Num of Customers: " + total);
            this.ff_per_customer = new short[(int)total];
            for (int i = 0; i < total; i++) {
                this.ff_per_customer[i] = (short)ff_zipf.nextInt();
                if (this.ff_per_customer[i] > max_per_customer) this.ff_per_customer[i] = (short)max_per_customer;
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
                        if (trace.get()) LOG.trace("CUSTOMER IDX: " + this.customer_idx);
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
                    this.customer_airlines.add((String)value);
                    if (trace.get()) LOG.trace(this.last_customer_id + " => " + value);
                    break;
                }
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
        
        @Override
        protected void callbackFinished() {
            if (trace.get()) {
                Histogram<String> h = this.airline_rand.getHistogramHistory();
                LOG.trace(String.format("Airline Flights Histogram [valueCount=%d, sampleCount=%d]\n%s",
                                        h.getValueCount(), h.getSampleCount(), h.toString()));
            }
        }
    }
    
    // ----------------------------------------------------------------
    // AIRPORT_DISTANCE
    // ----------------------------------------------------------------
    protected class AirportDistanceIterable extends ScalingDataIterable {
        private final int max_distance;
        private final int num_airports;
        private final Collection<String> record_airports;
        
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
            super(catalog_tbl, Long.MAX_VALUE, new int[] { 0, 1, 2 }); // total work around ????
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
                if (hasFlights(this.outer_airport) == false) continue;
                
                int inner_ctr = (this.last_inner_ctr != null ? this.last_inner_ctr : this.outer_ctr + 1);
                this.last_inner_ctr = null;
                for ( ; inner_ctr < this.num_airports; inner_ctr++) {
                    assert(this.outer_ctr != inner_ctr);
                    this.inner_airport = airport_locations.get(inner_ctr);
                    this.inner_location = airport_locations.getValue(inner_ctr);
                    if (hasFlights(this.inner_airport) == false) continue;
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
        private final Map<String, RandomDistribution.FlatHistogram<String>> flights_per_airport = new HashMap<String, RandomDistribution.FlatHistogram<String>>();
        private final RandomDistribution.FlatHistogram<String> flight_times;
        private final RandomDistribution.Flat prices = new RandomDistribution.Flat(rng, 100, 1000);
        
        private final Pattern time_pattern = Pattern.compile("([\\d]{2,2}):([\\d]{2,2})");
        private final ListOrderedMap<TimestampType, Integer> flights_per_day = new ListOrderedMap<TimestampType, Integer>();
//        private final Map<Long, AtomicInteger> airport_flight_ids = new HashMap<Long, AtomicInteger>();
        
        private int day_idx = 0;
        private TimestampType today;
        private TimestampType start_date;
        
        private FlightId flight_id;
        private String depart_airport;
        private String arrive_airport;
        private String airline_code;
        private Long airline_id;
        private TimestampType depart_time;
        private TimestampType arrive_time;
        private int status;
        
        public FlightIterable(Table catalog_tbl, int days_past, int days_future) {
            super(catalog_tbl, Long.MAX_VALUE, new int[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
            assert(days_past >= 0);
            assert(days_future >= 0);
            
            // Flights per Airline
            Collection<String> all_airlines = getAirlineCodes();
            Histogram<String> histogram = new Histogram<String>();
            histogram.putAll(all_airlines);
            // Embed a Gaussian distribution
            RandomDistribution.Gaussian gauss_rng = new RandomDistribution.Gaussian(rng, 0, all_airlines.size());
            this.airlines = new RandomDistribution.FlatHistogram<String>(gauss_rng, histogram);
            
            // Flights Per Airport
            histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);  
            this.airports = new RandomDistribution.FlatHistogram<String>(rng, histogram);
            for (String airport_code : histogram.values()) {
                histogram = AirlineLoader.this.getFightsPerAirportHistogram(airport_code);
                assert(histogram != null) : "Unexpected departure airport code '" + airport_code + "'";
                this.flights_per_airport.put(airport_code, new RandomDistribution.FlatHistogram<String>(rng, histogram));
            } // FOR
            
            // Flights Per Departure Time
            histogram = AirlineLoader.this.getHistogram(AirlineConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES);  
            this.flight_times = new RandomDistribution.FlatHistogram<String>(rng, histogram);
            
            // Figure out how many flights that we want for each day
            this.today = new TimestampType();
            
            this.total = 0;
            int num_flights_low = (int)Math.round(AirlineConstants.NUM_FLIGHTS_PER_DAY * 0.75);
            int num_flights_high = (int)Math.round(AirlineConstants.NUM_FLIGHTS_PER_DAY * 1.25);
            
            RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(rng, num_flights_low, num_flights_high);
            boolean first = true;
            for (long _date = this.today.getTime() - (days_past * AirlineConstants.MICROSECONDS_PER_DAY);
                 _date < this.today.getTime(); _date += AirlineConstants.MICROSECONDS_PER_DAY) {
                TimestampType date = new TimestampType(_date);
                if (first) {
                    this.start_date = date;
                    AirlineLoader.this.setFlightStartDate(date);
                    first = false;
                }
                int num_flights = (int)Math.ceil(gaussian.nextInt() / getScaleFactor());
                this.flights_per_day.put(date, num_flights);
                this.total += num_flights;
            } // FOR
            
            // This is for upcoming flights that we want to be able to schedule
            // new reservations for in the benchmark
            AirlineLoader.this.setFlightUpcomingDate(this.today);
            for (long _date = this.today.getTime(), last_date = this.today.getTime() + (days_future * AirlineConstants.MICROSECONDS_PER_DAY);
                 _date <= last_date; _date += AirlineConstants.MICROSECONDS_PER_DAY) {
                TimestampType date = new TimestampType(_date);
                int num_flights = (int)Math.ceil(gaussian.nextInt()  / getScaleFactor());
                this.flights_per_day.put(date, num_flights);
                this.total += num_flights;
                // System.err.println(new Date(date).toString() + " -> " + num_flights);
            } // FOR
            
            // Update profile
            AirlineLoader.this.setFlightPastDays(days_past);
            AirlineLoader.this.setFlightFutureDays(days_future);
        }
        
        /**
         * Convert a time string "HH:MM" to a Date object
         * @param code
         * @return
         */
        private TimestampType convertTimeString(TimestampType base_date, String code) {
            Matcher m = this.time_pattern.matcher(code);
            assert(m.find()) : "Invalid time code '" + code + "'";
            int hour = Integer.valueOf(m.group(1));
            int minute = Integer.valueOf(m.group(2));
            long offset = (hour * 60 * AirlineConstants.MICROSECONDS_PER_MINUTE) + (minute * AirlineConstants.MICROSECONDS_PER_MINUTE);
            return (new TimestampType(base_date.getTime() + offset));
        }
        
        /**
         * Select all the data elements for the current tuple
         * @param date
         */
        private void populate(TimestampType date) {
            // Depart/Arrive Airports
            this.depart_airport = this.airports.nextValue();
            this.arrive_airport = this.flights_per_airport.get(this.depart_airport).nextValue();

            // Depart/Arrive Times
            this.depart_time = this.convertTimeString(date, this.flight_times.nextValue().toString());
            this.arrive_time = AirlineLoader.this.calculateArrivalTime(this.depart_airport, this.arrive_airport, this.depart_time);

            // Airline
            this.airline_code = this.airlines.nextValue();
            this.airline_id = getAirlineId(this.airline_code);
            
            // Status
            this.status = 0; // TODO
            
            this.flights_per_day.put(date, this.flights_per_day.get(date) - 1);
            return;
        }
        
        /** 
         * Give each seat a 70%-90% probability of being occupied
         */
        boolean seatIsOccupied() {
            return (rng.number(1,100) < rng.number(AirlineConstants.PROB_SEAT_OCCUPIED_MIN, AirlineConstants.PROB_SEAT_OCCUPIED_MAX));
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // FLIGHT ID
                case (0): {
                    // Figure out what date we are currently on
                    Integer remaining = null;
                    TimestampType date; 
                    do {
                        if (remaining != null) this.day_idx++;
                        date = this.flights_per_day.get(this.day_idx);
                        remaining = this.flights_per_day.getValue(this.day_idx);
                    } while (remaining <= 0 && this.day_idx + 1 < this.flights_per_day.size());
                    this.populate(date);
                    
                    // Generate a composite FlightId
                    long depart_airport_id = AirlineLoader.this.getAirportId(this.depart_airport);
                    long arrive_airport_id = AirlineLoader.this.getAirportId(this.arrive_airport);
                    this.flight_id = new FlightId(this.airline_id, depart_airport_id, arrive_airport_id, this.start_date, this.depart_time);
                    AirlineLoader.this.addFlightId(this.flight_id);
                    value = this.flight_id.encode();
                    break;
                }
                // AIRLINE ID
                case (1): {
                    value = this.airline_code;
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
                // BASE PRICE
                case (7): {
                    value = MathUtil.roundToDecimals(this.prices.nextLong(), 2);
                    break;
                }
                // SEATS TOTAL
                case (8): {
                    value = AirlineConstants.NUM_SEATS_PER_FLIGHT;
                    break;
                }
                // SEATS REMAINING
                case (9): {
                    // We have to figure this out ahead of time since we need to populate the tuple now
                    for (int seatnum = 0; seatnum < AirlineConstants.NUM_SEATS_PER_FLIGHT; seatnum++) {
                        if (!this.seatIsOccupied()) continue;
                        AirlineLoader.this.decrementFlightSeat(this.flight_id);
                    } // FOR
                    value = new Long(AirlineLoader.this.getFlightRemainingSeats(this.flight_id));
                    if (debug.get()) LOG.debug(this.flight_id.encode() + " SEATS REMAINING: " + value);
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
        private final RandomDistribution.Flat prices = new RandomDistribution.Flat(rng, 100, 1000);
        
        /**
         * For each airport id, store a list of ReturnFlight objects that represent customers
         * that need return flights back to their home airport
         * ArriveAirportId -> ReturnFlights
         */
        private final Map<Long, TreeSet<ReturnFlight>> airport_returns = new HashMap<Long, TreeSet<ReturnFlight>>();
        
        /**
         * When this flag is true, then the data generation thread is finished
         */
        private boolean done = false;
        
        private Throwable error = null;
        
        /**
         * We use a Gaussian distribution for determining how long a customer will stay at their
         * destination before needing to return to their original airport
         */
        private final RandomDistribution.Gaussian rand_returns = new RandomDistribution.Gaussian(rng, 1, AirlineConstants.MAX_RETURN_FLIGHT_DAYS);
        
        private final LinkedBlockingDeque<Object[]> queue = new LinkedBlockingDeque<Object[]>(100);
        private Object current[] = null;
        
        /**
         * Constructor
         * @param catalog_tbl
         * @param total
         */
        public ReservationIterable(Table catalog_tbl, long total) {
            // Special Columns: R_C_ID, R_F_ID, R_F_AL_ID, R_SEAT, R_PRICE
            super(catalog_tbl, total, new int[] { 1, 2, 3, 4 });
            
            for (long airport_id : AirlineLoader.this.getAirportIds()) {
                // Return Flights per airport
                this.airport_returns.put(airport_id, new TreeSet<ReturnFlight>());
            } // FOR
            
            // Data Generation Thread
            // Ok, hang on tight. We are going to fork off a separate thread to generate our
            // tuples because it's easier than trying to pick up where we left off every time
            // That means that when hasNext() is called, it will block and poke this thread to start
            // running. Once this thread has generate a new tuple, it will block itself and then
            // poke the hasNext() thread. This is sort of like a hacky version of Python's yield
            new Thread() { 
                public void run() {
                    try {
                        ReservationIterable.this.generateData();
                    } catch (Throwable ex) {
//                        System.err.println("Airport Customers:\n" + getAirportCustomerHistogram());
                        ReservationIterable.this.error = ex;
                    } finally {
                        if (debug.get()) LOG.debug("Reservation generation thread is finished");
                        ReservationIterable.this.done = true;
                    }
                } // run
            }.start();
        }
        
        private void generateData() throws Exception {
            if (debug.get()) LOG.debug("Reservation data generation thread started");
            
            Collection<CustomerId> flight_customer_ids = new HashSet<CustomerId>();
            Collection<ReturnFlight> returning_customers = new ListOrderedSet<ReturnFlight>();
            
            // Loop through the flights and generate reservations
            for (FlightId flight_id : AirlineLoader.this.getFlightIds()) {
                long depart_airport_id = flight_id.getDepartAirportId();
                long arrive_airport_id = flight_id.getArriveAirportId();
                TimestampType depart_time = flight_id.getDepartDate(AirlineLoader.this.getFlightStartDate());
                TimestampType arrive_time = AirlineLoader.this.calculateArrivalTime(getAirportCode(depart_airport_id), getAirportCode(arrive_airport_id), depart_time);
                flight_customer_ids.clear();
                
                // For each flight figure out which customers are returning
                this.getReturningCustomers(returning_customers, flight_id);
                int booked_seats = AirlineConstants.NUM_SEATS_PER_FLIGHT - AirlineLoader.this.getFlightRemainingSeats(flight_id);

                if (trace.get()) {
                    Map<String, Object> m = new ListOrderedMap<String, Object>();
                    m.put("Flight Id", flight_id + " / " + flight_id.encode());
                    m.put("Departure", String.format("%s / %s", getAirportCode(depart_airport_id), depart_time));
                    m.put("Arrival", String.format("%s / %s", getAirportCode(arrive_airport_id), arrive_time));
                    m.put("Booked Seats", booked_seats);
                    m.put(String.format("Returning Customers[%d]", returning_customers.size()), StringUtil.join("\n", returning_customers));
                    LOG.trace("Flight Information\n" + StringUtil.formatMaps(m));
                }
                
                for (int seatnum = 0; seatnum < booked_seats; seatnum++) {
                    CustomerId customer_id = null;
                    Long airport_customer_cnt = AirlineLoader.this.getCustomerIdCount(depart_airport_id);
                    boolean local_customer = airport_customer_cnt != null && (flight_customer_ids.size() < airport_customer_cnt.intValue());
                    int tries = 2000;
                    ReturnFlight return_flight = null;
                    while (tries > 0) {
                        return_flight = null;
                        
                        // Always book returning customers first
                        if (returning_customers.isEmpty() == false) {
                            return_flight = CollectionUtil.pop(returning_customers); 
                            customer_id = return_flight.getCustomerId();
                        }
                        // New Outbound Reservation
                        // Prefer to use a customer based out of the local airport
                        else if (local_customer) {
                            customer_id = AirlineLoader.this.getRandomCustomerId(depart_airport_id);
                        }
                        // New Outbound Reservation
                        // We'll take anybody!
                        else {
                            customer_id = AirlineLoader.this.getRandomCustomerId();
                        }
                        if (flight_customer_ids.contains(customer_id) == false) break;
                        tries--;
                    } // WHILE
                    assert(tries > 0) : String.format("Safety check! [local=%s]", local_customer);

                    // If this is return flight, then there's nothing extra that we need to do
                    if (return_flight != null) {
                        if (trace.get()) LOG.trace("Booked return flight: " + return_flight + " [remaining=" + returning_customers.size() + "]");
                    
                    // If it's a new outbound flight, then we will randomly decide when this customer will return (if at all)
                    } else {
                        int rand = rng.nextInt(100);
                        if (rand <= AirlineConstants.PROB_SINGLE_FLIGHT_RESERVATION) {
                            // Do nothing for now...
                            
                        // Create a ReturnFlight object to record that this customer needs a flight
                        // back to their original depart airport
                        } else {
                            int return_days = rand_returns.nextInt();
                            return_flight = new ReturnFlight(customer_id, depart_airport_id, depart_time, return_days);
                            this.airport_returns.get(arrive_airport_id).add(return_flight);
                        }
                    }
                    assert(customer_id != null) : "Null customer id on " + flight_id;
                    assert(flight_customer_ids.contains(customer_id) == false) : flight_id + " already contains " + customer_id; 
                    flight_customer_ids.add(customer_id);
                    
                    if (trace.get()) LOG.trace(String.format("New reservation ready. Adding to queue! [queueSize=%d]", this.queue.size()));
                    this.queue.put(new Object[]{ customer_id, flight_id, seatnum });
                } // FOR (seats)
            } // FOR (flights)
            LOG.info("Reservation data generation thread is finished");
        }
        
        /**
         * Return a list of the customers that need to return to their original
         * location on this particular flight.
         * @param flight_id
         * @return
         */
        private void getReturningCustomers(Collection<ReturnFlight> returning_customers, FlightId flight_id) {
            TimestampType flight_date = flight_id.getDepartDate(AirlineLoader.this.getFlightStartDate());
            returning_customers.clear();
            Set<ReturnFlight> returns = this.airport_returns.get(flight_id.getDepartAirportId());
            if (!returns.isEmpty()) {
                for (ReturnFlight return_flight : returns) {
                    if (return_flight.getReturnDate().compareTo(flight_date) > 0) break;
                    if (return_flight.getReturnAirportId() == flight_id.getArriveAirportId()) {
                        returning_customers.add(return_flight);
                    }
                } // FOR
                if (!returning_customers.isEmpty()) returns.removeAll(returning_customers);
            }
        }
        
        @Override
        protected boolean hasNext() {
            if (trace.get()) LOG.trace("hasNext() called");
            this.current = null;
            while (this.done == false || this.queue.isEmpty() == false) {
                if (this.error != null)
                    throw new RuntimeException("Failed to generate Reservation records", this.error);
                
                try {
                    this.current = this.queue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption!", ex);
                }
                if (this.current != null) return (true);
                if (trace.get()) LOG.trace("There were no new reservations. Let's try again!");
            } // WHILE
            return (false);
        }
        
        @Override
        protected Object specialValue(long id, int columnIdx) {
            assert(this.current != null);
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (1): {
                    value = ((CustomerId)this.current[0]).encode();
                    break;
                }
                // FLIGHT ID
                case (2): {
                    FlightId flight_id = (FlightId)this.current[1];
                    value = flight_id.encode();
                    if (AirlineLoader.this.getReservationUpcomingOffset() == null &&
                        flight_id.isUpcoming(AirlineLoader.this.getFlightStartDate(), AirlineLoader.this.getFlightPastDays())) {
                        AirlineLoader.this.setReservationUpcomingOffset(id);
                    }
                    break;
                }
                // SEAT
                case (3): {
                    value = this.current[2];
                    break;
                }
                // PRICE
                case (4): {
                    value = MathUtil.roundToDecimals(this.prices.nextInt(), 2);
                    break;
                }
                // BAD MOJO!
                default:
                    assert(false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    } // END CLASS
}

