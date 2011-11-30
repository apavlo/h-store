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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;

public class AirlineProfile {
    private static final Logger LOG = Logger.getLogger(AirlineProfile.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Data Scale Factor
     */
    public double scale_factor;
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
    /**
     * We want to maintain a small cache of FlightIds so that the AirlineClient
     * has something to work with. We obviously don't want to store the entire set here
     */
    protected final LinkedList<FlightId> cached_flight_ids = new LinkedList<FlightId>();

    /** TODO */
    protected final Map<String, Histogram<String>> histograms = new HashMap<String, Histogram<String>>();
    
    /**
     * Each AirportCode will have a histogram of the number of flights 
     * that depart from that airport to all the other airports
     */
    protected final Map<String, Histogram<String>> airport_histograms = new HashMap<String, Histogram<String>>();

    protected final Map<String, Map<String, Long>> code_id_xref = new HashMap<String, Map<String, Long>>();

    
    /**
     * Save the profile information into the database 
     */
    protected final void saveProfile(AirlineBaseClient baseClient) {
        Database catalog_db = CatalogUtil.getDatabase(baseClient.getCatalog());
        
        // CONFIG_PROFILE
        Table catalog_tbl = catalog_db.getTables().get(AirlineConstants.TABLENAME_CONFIG_PROFILE);
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
            JSONUtil.toJSONString(this.cached_flight_ids), // CFP_FLIGHT_IDS
            JSONUtil.toJSONString(this.code_id_xref) // CFP_CODE_ID_XREF
        );
        LOG.info("Saving profile information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);
        
        // CONFIG_HISTOGRAMS
        catalog_tbl = catalog_db.getTables().get(AirlineConstants.TABLENAME_CONFIG_HISTOGRAMS);
        vt = CatalogUtil.getVoltTable(catalog_tbl);
        assert(vt != null);
        
        for (Entry<String, Histogram<String>> e : this.airport_histograms.entrySet()) {
            vt.addRow(
                e.getKey(),                     // CFH_NAME
                e.getValue().toJSONString(),    // CFH_DATA
                1                               // CFH_IS_AIRPORT
            );
        } // FOR
        LOG.info("Saving airport histogram information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);
        
        for (Entry<String, Histogram<String>> e : this.histograms.entrySet()) {
            vt.addRow(
                e.getKey(),                     // CFH_NAME
                e.getValue().toJSONString(),    // CFH_DATA
                0                               // CFH_IS_AIRPORT
            );
        } // FOR
        LOG.info("Saving benchmark histogram information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);

        return;
    }
    
    private AirlineProfile copy(AirlineProfile other) {
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
        this.cached_flight_ids.addAll(other.cached_flight_ids);
        this.airport_histograms.putAll(other.airport_histograms);
        this.histograms.putAll(other.histograms);
        return (this);
    }
    
    /**
     * Load the profile information stored in the database
     */
    private static AirlineProfile cachedProfile;
    protected synchronized final void loadProfile(AirlineBaseClient baseClient) {
        // Check whether we have a cached Profile we can copy from
        if (cachedProfile != null) {
            if (debug.get()) LOG.debug("Using cached AirlineProfile");
            this.copy(cachedProfile);
            return;
        }
        
        if (debug.get()) LOG.debug("Loading AirlineProfile for the first time");
        
        // Otherwise we have to go fetch everything again
        Client client = baseClient.getClientHandle();
        
        // CONFIG_PROFILE
        ClientResponse response = null;
        try {
            response = client.callProcedure("LoadConfigProfile");
        } catch (Exception ex) {
            throw new RuntimeException("Failed retrieve data from " + AirlineConstants.TABLENAME_CONFIG_PROFILE, ex);
        }
        assert(response != null);
        assert(response.getStatus() == Hstore.Status.OK) : "Unexpected " + response;
        assert(response.getResults().length == 1);
        VoltTable vt = response.getResults()[0]; 
        
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
        
        // HACK: We should really be pull this directly from the FLIGHTS table, but
        // for now we'll just it this way because H-Store doesn't support ad-hoc queries...
        try {
            String json_str = vt.getString(col++);
//            System.err.println(json_str);
            JSONArray json_arr = new JSONArray(json_str);
            for (int i = 0, cnt = json_arr.length(); i < cnt; i++) {
                FlightId flight_id = new FlightId();
                flight_id.fromJSON(json_arr.getJSONObject(i), null);
                this.cached_flight_ids.add(flight_id);
            } // FOR
        } catch (JSONException ex) {
            throw new RuntimeException("Failed to deserialize cached FlightIds", ex);
        }
        try {
            String json_str = vt.getString(col++);
            JSONObject json_obj = new JSONObject(json_str);
            for (String key : CollectionUtil.iterable(json_obj.keys())) {
                JSONObject inner = json_obj.getJSONObject(key);
                for (String inner_key : CollectionUtil.iterable(inner.keys())) {
                    long value = inner.getLong(inner_key);
                    this.code_id_xref.get(key).put(inner_key, value);
                } // FOR
            } // FOR
        } catch (JSONException ex) {
            throw new RuntimeException("Failed to deserialize code id xrefs", ex);
        }
        
        // CONFIG_HISTOGRAMS
        try {
            response = client.callProcedure("LoadConfigHistograms");
        } catch (Exception ex) {
            throw new RuntimeException("Failed retrieve data from " + AirlineConstants.TABLENAME_CONFIG_HISTOGRAMS, ex);
        }
        assert(response != null);
        assert(response.getStatus() == Hstore.Status.OK) : "Unexpected " + response;
        assert(response.getResults().length == 1);
        vt = response.getResults()[0];
        
        while (vt.advanceRow()) {
            String name = vt.getString(0);
            Histogram<String> h = JSONUtil.fromJSONString(new Histogram<String>(), vt.getString(1));
            boolean is_airline = (vt.getLong(2) == 1);
            
            if (is_airline) {
                this.airport_histograms.put(name, h);
            } else {
                this.histograms.put(name, h);
            }
        } // WHILE

        if (debug.get()) LOG.debug("Airport Max Customer Id:\n" + this.airport_max_customer_id);
        cachedProfile = new AirlineProfile().copy(this);
    }
    
}
