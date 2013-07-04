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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.seats.procedures.LoadConfig;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public class SEATSProfile {
    private static final Logger LOG = Logger.getLogger(SEATSProfile.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
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
     * The number of FLIGHT rows created.
     */
    protected long num_flights = 0l;
    /**
     * The number of CUSTOMER rows
     */
    protected long num_customers = 0l;
    /**
     * The number of RESERVATION rows
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
    protected transient final CatalogContext catalogContext;
    
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
    protected transient final AbstractRandomGenerator rng;
    
    /**
     * Depart Airport Code -> Arrive Airport Code
     * Random number generators based on the flight distributions 
     */
    private final Map<String, FlatHistogram<String>> airport_distributions = new HashMap<String, FlatHistogram<String>>();
    
    // ----------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------
    
    public SEATSProfile(CatalogContext catalogContext, AbstractRandomGenerator rng) {
        this.catalogContext = catalogContext;
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
                if (debug.val) LOG.debug(String.format("Added %s mapping from Code Column '%s' to Id Column '%s'", tableName, codeCol, idCol));
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
        for (Table catalog_tbl : catalogContext.database.getTables()) {
            for (Column catalog_col : catalog_tbl.getColumns()) {
                Column catalog_fkey_col = CatalogUtil.getForeignKeyParent(catalog_col);
                if (catalog_fkey_col != null && this.code_id_xref.containsKey(catalog_fkey_col.getName())) {
                    this.fkey_value_xref.put(catalog_col.getName(), catalog_fkey_col.getName());
                    if (debug.val) LOG.debug(String.format("Added ForeignKey mapping from %s to %s", catalog_col.fullName(), catalog_fkey_col.fullName()));
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
        
        // CONFIG_PROFILE
        Table catalog_tbl = catalogContext.database.getTables().get(SEATSConstants.TABLENAME_CONFIG_PROFILE);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assert(vt != null);
        vt.addRow(
            this.scale_factor,                  // CFP_SCALE_FACTOR
            this.airport_max_customer_id.toJSONString(), // CFP_AIPORT_MAX_CUSTOMER
            this.flight_start_date,             // CFP_FLIGHT_START
            this.flight_upcoming_date,          // CFP_FLIGHT_UPCOMING
            this.flight_past_days,              // CFP_FLIGHT_PAST_DAYS
            this.flight_future_days,            // CFP_FLIGHT_FUTURE_DAYS
            this.num_flights,                   // CFP_NUM_FLIGHTS
            this.num_customers,                 // CFP_NUM_CUSTOMERS
            this.num_reservations,              // CFP_NUM_RESERVATIONS
            JSONUtil.toJSONString(this.code_id_xref) // CFP_CODE_ID_XREF
        );
        if (debug.val)
            LOG.debug(String.format("Saving profile information into %s\n%s", catalog_tbl, this));
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);
        
        // CONFIG_HISTOGRAMS
        catalog_tbl = catalogContext.database.getTables().get(SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS);
        vt = CatalogUtil.getVoltTable(catalog_tbl);
        assert(vt != null);
        
        for (Entry<String, Histogram<String>> e : this.airport_histograms.entrySet()) {
            vt.addRow(
                e.getKey(),                     // CFH_NAME
                e.getValue().toJSONString(),    // CFH_DATA
                1                               // CFH_IS_AIRPORT
            );
        } // FOR
        if (debug.val) LOG.debug("Saving airport histogram information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);
        
        for (Entry<String, Histogram<String>> e : this.histograms.entrySet()) {
            vt.addRow(
                e.getKey(),                     // CFH_NAME
                e.getValue().toJSONString(),    // CFH_DATA
                0                               // CFH_IS_AIRPORT
            );
        } // FOR
        if (debug.val) LOG.debug("Saving benchmark histogram information into " + catalog_tbl);
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
        this.num_flights = other.num_flights;
        this.num_customers = other.num_customers;
        this.num_reservations = other.num_reservations;
        this.code_id_xref.putAll(other.code_id_xref);
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
                if (debug.val) LOG.debug("Using cached SEATSProfile");
                this.copy(cachedProfile);
                return;
            }
            
            // Otherwise we have to go fetch everything again
            if (debug.val) LOG.debug("Loading SEATSProfile for the first time");
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
            // this.loadCachedFlights(results[result_idx++]);
            
            if (debug.val)
                LOG.debug("Loaded profile:\n" + this.toString());
            if (trace.val)
                LOG.trace("Airport Max Customer Id:\n" + this.airport_max_customer_id);
            
            cachedProfile = new SEATSProfile(this.catalogContext, this.rng).copy(this);
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
        this.num_flights = vt.getLong(col++);
        this.num_customers = vt.getLong(col++);
        this.num_reservations = vt.getLong(col++);
        if (debug.val)
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
                if (trace.val) 
                    LOG.trace(String.format("Loaded %d records for %s airport histogram", h.getValueCount(), name));
            } else {
                this.histograms.put(name, h);
                if (trace.val)
                    LOG.trace(String.format("Loaded %d records for %s histogram", h.getValueCount(), name));
            }
        } // WHILE
        if (debug.val)
            LOG.debug(String.format("Loaded %s data", SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS));
    }
    
    private final void loadCodeXref(VoltTable vt, String codeCol, String idCol) {
        Map<String, Long> m = this.code_id_xref.get(idCol);
        while (vt.advanceRow()) {
            long id = vt.getLong(0);
            String code = vt.getString(1);
            m.put(code, id);
        } // WHILE
        if (debug.val) LOG.debug(String.format("Loaded %d xrefs for %s -> %s", m.size(), codeCol, idCol));
    }
    
//    private final void loadCachedFlights(VoltTable vt) {
//        while (vt.advanceRow()) {
//            long f_id = vt.getLong(0);
//            FlightId flight_id = new FlightId(f_id);
//            this.cached_flight_ids.add(flight_id);
//        } // WHILE
//        if (debug.val)
//            LOG.debug(String.format("Loaded %d cached FlightIds", this.cached_flight_ids.size()));
//    }
    
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

    // -----------------------------------------------------------------
    // FLIGHTS
    // -----------------------------------------------------------------
    
    public long getRandomFlightId() {
        return (long)this.rng.nextInt((int)this.num_flights);
    }
    
    public long getFlightIdCount() {
        return (this.num_flights);
    }
    
    // ----------------------------------------------------------------
    // HISTOGRAM METHODS
    // ----------------------------------------------------------------
    
    /**
     * Return the histogram for the given name
     * @param name
     * @return
     */
    protected Histogram<String> getHistogram(String name) {
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
        return (this.rng.number(1, (int)this.getAirportCount()));
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
     * Return a random date in the future (after the start of upcoming flights)
     * @return
     */
    public TimestampType getRandomUpcomingDate() {
        TimestampType upcoming_start_date = this.flight_upcoming_date;
        int offset = rng.nextInt((int)this.flight_future_days);
        return (new TimestampType(upcoming_start_date.getTime() + (offset * SEATSConstants.MICROSECONDS_PER_DAY)));
    }
    
//    /**
//     * Return a random FlightId from our set of cached ids
//     * @return
//     */
//    public FlightId getRandomFlightId() {
//        assert(this.cached_flight_ids.isEmpty() == false);
//        if (trace.val)
//            LOG.trace("Attempting to get a random FlightId");
//        int idx = rng.nextInt(this.cached_flight_ids.size());
//        FlightId flight_id = this.cached_flight_ids.get(idx);
//        if (trace.val)
//            LOG.trace("Got random " + flight_id);
//        return (flight_id);
//    }
    
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
    
    // ----------------------------------------------------------------
    // CUSTOMER METHODS
    // ----------------------------------------------------------------
    
    public Long getCustomerIdCount(Long airport_id) {
        return (this.airport_max_customer_id.get(airport_id));
    }
    public long getCustomerIdCount() {
        return (this.num_customers);
    }
    /**
     * Return a random customer id based out of any airport 
     * @return
     */
    public long getRandomCustomerId() {
        return (long)this.rng.nextInt((int)this.num_customers);
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
        if (debug.val) LOG.debug("Generating Airport-CustomerCount histogram [numAirports=" + this.getAirportCount() + "]");
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
    
    public long getNextReservationId(int clientId) {
        // Offset it by the client id so that we can ensure it's unique
        long r_id = this.num_reservations++ | clientId<<52; 
        assert(r_id != VoltType.NULL_BIGINT) : 
            String.format("Unexpected null reservation id %d [clientId=%d]\n%s",
                          r_id, clientId, this);
        return (r_id);
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
        m.put("Num Flights", this.num_flights);
        m.put("Num Customers", this.num_customers);
        m.put("Num Reservations", this.num_reservations);
        return (StringUtil.formatMaps(m));
    }
}
