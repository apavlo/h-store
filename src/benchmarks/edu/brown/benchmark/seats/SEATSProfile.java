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
package edu.brown.benchmark.seats;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.seats.procedures.LoadConfig;
import edu.brown.benchmark.seats.util.CustomerId;
import edu.brown.benchmark.seats.util.FlightId;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public class SEATSProfile {
    private static final Logger LOG = Logger.getLogger(SEATSProfile.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------
    // PERSISTENT DATA MEMBERS
    // ----------------------------------------------------------------
    
    /**
     * Data Scale Factor
     */
    protected double scale_factor;
    /**
     * For each airport id, store the last id of the customer that uses this airport
     * as their local airport. The customer ids will be stored as follows in the dbms:
     * <16-bit AirportId><48-bit CustomerId>
     */
    protected final ObjectHistogram<Long> airport_max_customer_id = new ObjectHistogram<Long>();
    /**
     * The date when flights total data set begins
     */
    protected TimestampType flight_start_date = new TimestampType();
    /**
     * The date for when the flights are considered upcoming and are eligible for reservations
     */
    protected TimestampType flight_upcoming_date;
    /**
     * The number of days in the past that our flight data set includes.
     */
    protected long flight_past_days;
    /**
     * The number of days in the future (from the flight_upcoming_date) that our flight data set includes
     */
    protected long flight_future_days;
    /**
     * The offset of when upcoming flights begin in the seats_remaining list
     */
    protected Long flight_upcoming_offset = null;
    /**
     * The offset of when reservations for upcoming flights begin
     */
    protected Long reservation_upcoming_offset = null;
    /**
     * The number of reservations initially created.
     */
    protected long num_reservations = 0l;

    /**
     * TODO
     **/
    protected final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();
    
    /**
     * Each AirportCode will have a histogram of the number of flights 
     * that depart from that airport to all the other airports
     */
    protected final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();

    protected final Map<String, Map<String, Long>> code_id_xref = new HashMap<String, Map<String, Long>>();

    // ----------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------

    /**
     * TableName -> TableCatalog
     */
    protected transient final Catalog catalog;
    
    /**
     * We want to maintain a small cache of FlightIds so that the SEATSClient
     * has something to work with. We obviously don't want to store the entire set here
     */    
    protected transient final LinkedList<FlightId> cached_flight_ids = new LinkedList<FlightId>();
    
    /**
     * Key -> Id Mappings
     */
    protected transient final Map<String, String> code_columns = new HashMap<String, String>();
    
    /**
     * Foreign Key Mappings
     * Column Name -> Xref Mapper
     */
    protected transient final Map<String, String> fkey_value_xref = new HashMap<String, String>();
    
    /**
     * Specialized random number generator
     */
    protected transient final RandomGenerator rng;
    
    /**
     * Depart Airport Code -> Arrive Airport Code
     * Random number generators based on the flight distributions 
     */
    private final Map<String, FlatHistogram<String>> airport_distributions = new HashMap<String, FlatHistogram<String>>();
    
    // ----------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------
    
    public SEATSProfile(Catalog catalog, RandomGenerator rng) {
        this.catalog = catalog;
        this.rng = rng;
        
        // Tuple Code to Tuple Id Mapping
        for (String xref[] : SEATSConstants.CODE_TO_ID_COLUMNS) {
            assert(xref.length == 3);
            String tableName = xref[0];
            String codeCol = xref[1];
            String idCol = xref[2];
            
            if (this.code_columns.containsKey(codeCol) == false) {
                this.code_columns.put(codeCol, idCol);
                this.code_id_xref.put(idCol, new HashMap<String, Long>());
                if (debug.get()) LOG.debug(String.format("Added %s mapping from Code Column '%s' to Id Column '%s'", tableName, codeCol, idCol));
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
        for (Table catalog_tbl : CatalogUtil.getDatabase(catalog).getTables()) {
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
    // SAVE / LOAD PROFILE
    // ----------------------------------------------------------------
    
    /**
     * Save the profile information into the database 
     */
    protected final void saveProfile(BenchmarkComponent baseClient) {
        Database catalog_db = CatalogUtil.getDatabase(baseClient.getCatalog());
        
        // CONFIG_PROFILE
        Table catalog_tbl = catalog_db.getTables().get(SEATSConstants.TABLENAME_CONFIG_PROFILE);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assert(vt != null);
        vt.addRow(
            this.scale_factor,                  // CFP_SCALE_FACTOR
            this.airport_max_customer_id.toJSONString(), // CFP_AIPORT_MAX_CUSTOMER
            this.flight_start_date,             // CFP_FLIGHT_START
            this.flight_upcoming_date,          // CFP_FLIGHT_UPCOMING
            this.flight_past_days,              // CFP_FLIGHT_PAST_DAYS
            this.flight_future_days,            // CFP_FLIGHT_FUTURE_DAYS
            this.flight_upcoming_offset,        // CFP_FLIGHT_OFFSET
            this.reservation_upcoming_offset,   // CFP_RESERVATION_OFFSET
            this.num_reservations,              // CFP_NUM_RESERVATIONS
            JSONUtil.toJSONString(this.code_id_xref) // CFP_CODE_ID_XREF
        );
        if (debug.get()) LOG.debug("Saving profile information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);
        
        // CONFIG_HISTOGRAMS
        catalog_tbl = catalog_db.getTables().get(SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS);
        vt = CatalogUtil.getVoltTable(catalog_tbl);
        assert(vt != null);
        
        for (Entry<String, Histogram<String>> e : this.airport_histograms.entrySet()) {
            vt.addRow(
                e.getKey(),                     // CFH_NAME
                e.getValue().toJSONString(),    // CFH_DATA
                1                               // CFH_IS_AIRPORT
            );
        } // FOR
        if (debug.get()) LOG.debug("Saving airport histogram information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);
        
        for (Entry<String, Histogram<String>> e : this.histograms.entrySet()) {
            vt.addRow(
                e.getKey(),                     // CFH_NAME
                e.getValue().toJSONString(),    // CFH_DATA
                0                               // CFH_IS_AIRPORT
            );
        } // FOR
        if (debug.get()) LOG.debug("Saving benchmark histogram information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);

        return;
    }
    
    protected static void clearCachedProfile() {
        cachedProfile = null;
    }
    
    private SEATSProfile copy(SEATSProfile other) {
        this.scale_factor = other.scale_factor;
        this.airport_max_customer_id.put(other.airport_max_customer_id);
        this.flight_start_date = other.flight_start_date;
        this.flight_upcoming_date = other.flight_upcoming_date;
        this.flight_past_days = other.flight_past_days;
        this.flight_future_days = other.flight_future_days;
        this.flight_upcoming_offset = other.flight_upcoming_offset;
        this.reservation_upcoming_offset = other.reservation_upcoming_offset;
        this.num_reservations = other.num_reservations;
        this.code_id_xref.putAll(other.code_id_xref);
        this.cached_flight_ids.addAll(other.cached_flight_ids);
        this.airport_histograms.putAll(other.airport_histograms);
        this.histograms.putAll(other.histograms);
        return (this);
    }
    
    /**
     * Load the profile information stored in the database
     */
    private static SEATSProfile cachedProfile;
    protected final void loadProfile(Client client) {
        synchronized (SEATSProfile.class) {
            // Check whether we have a cached Profile we can copy from
            if (cachedProfile != null) {
                if (debug.get()) LOG.debug("Using cached SEATSProfile");
                this.copy(cachedProfile);
                return;
            }
            
            // Otherwise we have to go fetch everything again
            if (debug.get()) LOG.debug("Loading SEATSProfile for the first time");
            ClientResponse response = null;
            try {
                response = client.callProcedure(LoadConfig.class.getSimpleName());
            } catch (Exception ex) {
                throw new RuntimeException("Failed retrieve data from " + SEATSConstants.TABLENAME_CONFIG_PROFILE, ex);
            }
            assert(response != null);
            assert(response.getStatus() == Status.OK) : "Unexpected " + response;
    
            VoltTable results[] = response.getResults();
            int result_idx = 0;
            
            // CONFIG_PROFILE
            this.loadConfigProfile(results[result_idx++]); 
            
            // CONFIG_HISTOGRAMS
            this.loadConfigHistograms(results[result_idx++]);
            
            // CODE XREFS
            for (int i = 0; i < SEATSConstants.CODE_TO_ID_COLUMNS.length; i++) {
                String codeCol = SEATSConstants.CODE_TO_ID_COLUMNS[i][1];
                String idCol = SEATSConstants.CODE_TO_ID_COLUMNS[i][2];
                this.loadCodeXref(results[result_idx++], codeCol, idCol);
            } // FOR
            
            // CACHED FLIGHT IDS
            this.loadCachedFlights(results[result_idx++]);
            
            if (debug.get())
                LOG.debug("Loaded profile:\n" + this.toString());
            if (LOG.isTraceEnabled())
                LOG.trace("Airport Max Customer Id:\n" + this.airport_max_customer_id);
            
            cachedProfile = new SEATSProfile(this.catalog, this.rng).copy(this);
        } // SYNCH
    }
    
    private final void loadConfigProfile(VoltTable vt) {
        boolean adv = vt.advanceRow();
        assert(adv) : "No data in " + SEATSConstants.TABLENAME_CONFIG_PROFILE + ". " +
        		      "Did you forget to load the database first?";
        int col = 0;
        this.scale_factor = vt.getDouble(col++);
        JSONUtil.fromJSONString(this.airport_max_customer_id, vt.getString(col++));
        this.flight_start_date = vt.getTimestampAsTimestamp(col++);
        this.flight_upcoming_date = vt.getTimestampAsTimestamp(col++);
        this.flight_past_days = vt.getLong(col++);
        this.flight_future_days = vt.getLong(col++);
        this.flight_upcoming_offset = vt.getLong(col++);
        this.reservation_upcoming_offset = vt.getLong(col++);
        this.num_reservations = vt.getLong(col++);
        if (debug.get())
            LOG.debug(String.format("Loaded %s data", SEATSConstants.TABLENAME_CONFIG_PROFILE));
    }
    
    private final void loadConfigHistograms(VoltTable vt) {
        while (vt.advanceRow()) {
            int col = 0;
            String name = vt.getString(col++);
            ObjectHistogram<String> h = JSONUtil.fromJSONString(new ObjectHistogram<String>(), vt.getString(col++));
            boolean is_airline = (vt.getLong(col++) == 1);
            
            if (is_airline) {
                this.airport_histograms.put(name, h);
                if (trace.get()) 
                    LOG.trace(String.format("Loaded %d records for %s airport histogram", h.getValueCount(), name));
            } else {
                this.histograms.put(name, h);
                if (trace.get())
                    LOG.trace(String.format("Loaded %d records for %s histogram", h.getValueCount(), name));
            }
        } // WHILE
        if (debug.get())
            LOG.debug(String.format("Loaded %s data", SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS));
    }
    
    private final void loadCodeXref(VoltTable vt, String codeCol, String idCol) {
        Map<String, Long> m = this.code_id_xref.get(idCol);
        while (vt.advanceRow()) {
            long id = vt.getLong(0);
            String code = vt.getString(1);
            m.put(code, id);
        } // WHILE
        if (debug.get()) LOG.debug(String.format("Loaded %d xrefs for %s -> %s", m.size(), codeCol, idCol));
    }
    
    private final void loadCachedFlights(VoltTable vt) {
        while (vt.advanceRow()) {
            long f_id = vt.getLong(0);
            FlightId flight_id = new FlightId(f_id);
            this.cached_flight_ids.add(flight_id);
        } // WHILE
        if (debug.get())
            LOG.debug(String.format("Loaded %d cached FlightIds", this.cached_flight_ids.size()));
    }
    
    // ----------------------------------------------------------------
    // DATA ACCESS METHODS
    // ----------------------------------------------------------------
    
    private Map<String, Long> getCodeXref(String col_name) {
        Map<String, Long> m = this.code_id_xref.get(col_name);
        assert(m != null) : "Invalid code xref mapping column '" + col_name + "'";
        assert(m.isEmpty() == false) :
            "Empty code xref mapping for column '" + col_name + "'\n" + StringUtil.formatMaps(this.code_id_xref); 
        return (m);
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
            if (this.cached_flight_ids.size() < SEATSConstants.CACHE_LIMIT_FLIGHT_IDS) {
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
        FlatHistogram<String> f = this.airport_distributions.get(code);
        if (f == null) {
            synchronized (this.airport_distributions) {
                f = this.airport_distributions.get(code);
                if (f == null) {
                    Histogram<String> h = this.airport_histograms.get(code);
                    assert(h != null);
                    f = new FlatHistogram<String>(rng, h);
                    this.airport_distributions.put(code, f);
                }
            } // SYCH
        }
        assert(f != null);
        String other = f.nextValue();
        return this.getAirportId(other);
    }
    
    /**
     * Return a random customer id based at the given airport_id 
     * @param airport_id
     * @return
     */
    public CustomerId getRandomCustomerId(Long airport_id) {
        Long cnt = this.getCustomerIdCount(airport_id);
        if (cnt != null) {
            int base_id = rng.nextInt(cnt.intValue());
            return (new CustomerId(base_id, airport_id));
        }
        return (null);
    }
    
    /**
     * Return a random customer id based out of any airport 
     * @return
     */
    public CustomerId getRandomCustomerId() {
        int num_airports = this.airport_max_customer_id.getValueCount();
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("Selecting a random airport with customers [numAirports=%d]", num_airports));
        CustomerId c_id = null;
        while (c_id == null) {
            Long airport_id = (long)this.rng.number(1, num_airports);
            c_id = this.getRandomCustomerId(airport_id);
        } // WHILE
        return (c_id);
    }
    
    /**
     * Return a random date in the future (after the start of upcoming flights)
     * @return
     */
    public TimestampType getRandomUpcomingDate() {
        TimestampType upcoming_start_date = this.flight_upcoming_date;
        int offset = rng.nextInt((int)this.getFlightFutureDays());
        return (new TimestampType(upcoming_start_date.getTime() + (offset * SEATSConstants.MICROSECONDS_PER_DAY)));
    }
    
    /**
     * Return a random FlightId from our set of cached ids
     * @return
     */
    public FlightId getRandomFlightId() {
        assert(this.cached_flight_ids.isEmpty() == false);
        if (LOG.isTraceEnabled())
            LOG.trace("Attempting to get a random FlightId");
        int idx = rng.nextInt(this.cached_flight_ids.size());
        FlightId flight_id = this.cached_flight_ids.get(idx);
        if (LOG.isTraceEnabled())
            LOG.trace("Got random " + flight_id);
        return (flight_id);
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
    
    public int incrementAirportCustomerCount(long airport_id) {
        int next_id = (int)this.airport_max_customer_id.get(airport_id, 0); 
        this.airport_max_customer_id.put(airport_id);
        return (next_id);
    }
    public Long getCustomerIdCount(Long airport_id) {
        return (this.airport_max_customer_id.get(airport_id));
    }
    public long getCustomerIdCount() {
        return (this.airport_max_customer_id.getSampleCount());
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
    
    public ObjectHistogram<String> getAirportCustomerHistogram() {
        ObjectHistogram<String> h = new ObjectHistogram<String>();
        if (debug.get()) LOG.debug("Generating Airport-CustomerCount histogram [numAirports=" + this.getAirportCount() + "]");
        for (Long airport_id : this.airport_max_customer_id.values()) {
            String airport_code = this.getAirportCode(airport_id);
            int count = this.airport_max_customer_id.get(airport_id).intValue();
            h.put(airport_code, count);
        } // FOR
        return (h);
    }
    
    /**
     * Get the list airport codes that have flights
     * @return
     */
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
    
    public long getNextReservationId(int id) {
        // Offset it by the client id so that we can ensure it's unique
        return (id | this.num_reservations++<<48);
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Scale Factor", this.scale_factor);
        m.put("# of Reservations", this.num_reservations);
        m.put("Flight Start Date", this.flight_start_date);
        m.put("Flight Upcoming Date", this.flight_upcoming_date);
        m.put("Flight Past Days", this.flight_past_days);
        m.put("Flight Future Days", this.flight_future_days);
        m.put("Flight Upcoming Offset", this.flight_upcoming_offset);
        m.put("Reservation Upcoming Offset", this.reservation_upcoming_offset);
        return (StringUtil.formatMaps(m));
    }
}
