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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.seats.procedures.LoadConfig;
import edu.brown.benchmark.seats.util.FlightId;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONUtil;

public class SEATSProfile {
    private static final Logger LOG = Logger.getLogger(SEATSProfile.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * We maintain a cached version of the profile that we will copy from
     * This prevents the need to have every single client thread load up a separate copy
     */
    private static SEATSProfile cachedProfile;
    
    /**
     * Data Scale Factor
     */
    protected double scale_factor;
    /**
     * For each airport id, store the last id of the customer that uses this airport
     * as their local airport. The customer ids will be stored as follows in the dbms:
     * <16-bit AirportId><48-bit CustomerId>
     */
    public final Histogram<Long> airport_max_customer_id = new Histogram<Long>();
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
    public final Histogram<String> num_records = new Histogram<String>();

    /** TODO */
    protected final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();
    
    /**
     * Each AirportCode will have a histogram of the number of flights 
     * that depart from that airport to all the other airports
     */
    protected final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();

    protected final Map<String, Map<String, Long>> code_id_xref = new HashMap<String, Map<String, Long>>();

    /**
     * We want to maintain a small cache of FlightIds so that the SEATSClient
     * has something to work with. We obviously don't want to store the entire set here
     */    
    protected transient final LinkedList<FlightId> cached_flight_ids = new LinkedList<FlightId>();
    
    // -----------------------------------------------------------------
    // SERIALIZATION METHODS
    // -----------------------------------------------------------------
    
    /**
     * Save the profile information into the database 
     */
    protected final void saveProfile(SEATSBaseClient baseClient) {
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
            this.reservation_upcoming_offset,    // CFP_RESERVATION_OFFSET
            this.num_records.toJSONString(),    // CFP_NUM_RECORDS
            // JSONUtil.toJSONString(this.cached_flight_ids), // CFP_FLIGHT_IDS
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
    
    private SEATSProfile copyProfile(SEATSProfile other) {
        this.scale_factor = other.scale_factor;
        this.airport_max_customer_id.putHistogram(other.airport_max_customer_id);
        this.flight_start_date = other.flight_start_date;
        this.flight_upcoming_date = other.flight_upcoming_date;
        this.flight_past_days = other.flight_past_days;
        this.flight_future_days = other.flight_future_days;
        this.flight_upcoming_offset = other.flight_upcoming_offset;
        this.reservation_upcoming_offset = other.reservation_upcoming_offset;
        this.num_records.putHistogram(other.num_records);
        this.code_id_xref.putAll(other.code_id_xref);
        this.airport_histograms.putAll(other.airport_histograms);
        this.histograms.putAll(other.histograms);
        
        this.cached_flight_ids.addAll(other.cached_flight_ids);
        Collections.shuffle(this.cached_flight_ids);
        
        return (this);
    }
    
    /**
     * Load the profile information stored in the database
     * @param 
     */
    protected synchronized final void loadProfile(SEATSBaseClient baseClient) {
        // Check whether we have a cached Profile we can copy from
        if (cachedProfile != null) {
            if (debug.get()) LOG.debug("Using cached SEATSProfile");
            this.copyProfile(cachedProfile);
            return;
        }
        
        if (debug.get()) LOG.debug("Loading SEATSProfile for the first time");
        
        // Otherwise we have to go fetch everything again
        Client client = baseClient.getClientHandle();
        
        ClientResponse response = null;
        try {
            response = client.callProcedure(LoadConfig.class.getSimpleName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed retrieve data from " + SEATSConstants.TABLENAME_CONFIG_PROFILE, ex);
        }
        assert(response != null);
        assert(response.getStatus() == Hstore.Status.OK) : "Unexpected " + response;

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

        if (trace.get()) LOG.trace("Airport Max Customer Id:\n" + this.airport_max_customer_id);
        cachedProfile = this;
    }
    
    private final void loadConfigProfile(VoltTable vt) {
        boolean adv = vt.advanceRow();
        assert(adv);
        int col = 0;
        this.scale_factor = vt.getDouble(col++);
        JSONUtil.fromJSONString(this.airport_max_customer_id, vt.getString(col++));
        this.flight_start_date = vt.getTimestampAsTimestamp(col++);
        this.flight_upcoming_date = vt.getTimestampAsTimestamp(col++);
        this.flight_past_days = vt.getLong(col++);
        this.flight_future_days = vt.getLong(col++);
        this.flight_upcoming_offset = vt.getLong(col++);
        this.reservation_upcoming_offset = vt.getLong(col++);
        JSONUtil.fromJSONString(this.num_records, vt.getString(col++));
        if (debug.get())
            LOG.debug(String.format("Loaded %s data", SEATSConstants.TABLENAME_CONFIG_PROFILE));
    }
    
    private final void loadConfigHistograms(VoltTable vt) {
        while (vt.advanceRow()) {
            String name = vt.getString(0);
            Histogram<String> h = JSONUtil.fromJSONString(new Histogram<String>(), vt.getString(1));
            boolean is_airline = (vt.getLong(2) == 1);
            
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
}
