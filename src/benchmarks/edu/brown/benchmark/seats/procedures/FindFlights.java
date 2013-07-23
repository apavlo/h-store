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
package edu.brown.benchmark.seats.procedures;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.*;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.seats.SEATSConstants;

@ProcInfo(
    partitionParam = 0,
	singlePartition = true
)	
public class FindFlights extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(FindFlights.class);
    
    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final VoltTable.ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("F_ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("AL_NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("DEPART_TIME", VoltType.TIMESTAMP),
        new VoltTable.ColumnInfo("DEPART_AP_CODE", VoltType.STRING),
        new VoltTable.ColumnInfo("DEPART_AP_NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("DEPART_AP_CITY", VoltType.STRING),
        new VoltTable.ColumnInfo("DEPART_AP_COUNTRY", VoltType.STRING),
        new VoltTable.ColumnInfo("ARRIVE_TIME", VoltType.TIMESTAMP),
        new VoltTable.ColumnInfo("ARRIVE_AP_CODE", VoltType.STRING),
        new VoltTable.ColumnInfo("ARRIVE_AP_NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("ARRIVE_AP_CITY", VoltType.STRING),
        new VoltTable.ColumnInfo("ARRIVE_AP_COUNTRY", VoltType.STRING),
    };
    
    private static final int MAX_NUM_FLIGHTS = 10;
    
    // -----------------------------------------------------------------
    // QUERIES
    // -----------------------------------------------------------------
    
    public final SQLStmt GetNearbyAirports = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstants.TABLENAME_AIRPORT_DISTANCE +
            " WHERE D_AP_ID0 = ? " +
            "   AND D_DISTANCE <= ? " +
            " ORDER BY D_DISTANCE ASC "
    );
 
    public final SQLStmt GetAirportInfo = new SQLStmt(
            "SELECT AP_CODE, AP_NAME, AP_CITY, AP_LONGITUDE, AP_LATITUDE, " +
                  " CO_ID, CO_NAME, CO_CODE_2, CO_CODE_3 " +
             " FROM " + SEATSConstants.TABLENAME_AIRPORT + ", " +
                        SEATSConstants.TABLENAME_COUNTRY +
            " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
    );
    
    private final static String BaseGetFlights =
            "SELECT F_ID, F_AL_ID, " +
                  " F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, " +
                  " AL_NAME, AL_IATTR00, AL_IATTR01 " +
             " FROM " + SEATSConstants.TABLENAME_FLIGHT_INFO + ", " +
                        SEATSConstants.TABLENAME_AIRLINE +
            " WHERE F_DEPART_AP_ID = ? " +
            "   AND F_DEPART_TIME >= ? AND F_DEPART_TIME <= ? " +
            "   AND F_AL_ID = AL_ID " +
            "   AND (%s) " +  // <--- SPECIAL WHERE PREDICATE! 
            " LIMIT " + MAX_NUM_FLIGHTS;
    
    public final SQLStmt GetFlights1 = new SQLStmt(String.format(BaseGetFlights, "F_ARRIVE_AP_ID = ?"));
    public final SQLStmt GetFlights2 = new SQLStmt(String.format(BaseGetFlights, "F_ARRIVE_AP_ID = ? OR " +
    		                                                                     "F_ARRIVE_AP_ID = ?"));
    public final SQLStmt GetFlights3 = new SQLStmt(String.format(BaseGetFlights, "F_ARRIVE_AP_ID = ? OR " +
    		                                                                     "F_ARRIVE_AP_ID = ? OR " +
    		                                                                     "F_ARRIVE_AP_ID = ?"));
 
    public VoltTable run(long depart_aid, long arrive_aid, TimestampType start_date, TimestampType end_date, long distance) {
        final boolean debug = LOG.isDebugEnabled();
        assert(start_date.equals(end_date) == false);
        
        final List<Long> arrive_aids = new ArrayList<Long>();
        arrive_aids.add(arrive_aid);
        
        if (distance > 0) {
            // First get the nearby airports for the departure and arrival cities
            voltQueueSQL(GetNearbyAirports, depart_aid, distance);
            final VoltTable[] nearby_results = voltExecuteSQL();
            assert(nearby_results.length == 1);
            while (nearby_results[0].advanceRow()) {
                long aid = nearby_results[0].getLong(0);
                double aid_distance = nearby_results[0].getLong(1);
                if (debug) LOG.debug("DEPART NEARBY: " + aid + " distance=" + aid_distance + " miles");
                arrive_aids.add(aid);
            } // WHILE
        }
        
        final VoltTable finalResults = new VoltTable(RESULT_COLS);
        
        // H-Store doesn't support IN clauses, so we'll only get nearby flights to 
        // up to three nearby arrival cities
        int num_nearby = arrive_aids.size(); 
        if (num_nearby > 0) {
            if (num_nearby == 1) {
                voltQueueSQL(GetFlights1, depart_aid, start_date, end_date, arrive_aids.get(0));
            } else if (num_nearby == 2) {
                voltQueueSQL(GetFlights2, depart_aid, start_date, end_date, arrive_aids.get(0), arrive_aids.get(1));
            } else {
                voltQueueSQL(GetFlights3, depart_aid, start_date, end_date, arrive_aids.get(0), arrive_aids.get(1), arrive_aids.get(2));
            }
            final VoltTable[] flightResults = voltExecuteSQL();
            assert(flightResults.length == 1);
            assert(flightResults[0].getRowCount() <= MAX_NUM_FLIGHTS) :
                String.format("Expected[%d] < Actual[%d] / num_nearby=%d",
                              MAX_NUM_FLIGHTS, flightResults[0].getRowCount(), num_nearby);
            if (debug)
                LOG.debug(String.format("Found %d flights between %d->%s [start=%s, end=%s]",
                          flightResults[0].getRowCount(), depart_aid, arrive_aids,
                          start_date, end_date));
            
            while (flightResults[0].advanceRow()) {
                long f_depart_airport = flightResults[0].getLong(2);
                long f_arrive_airport = flightResults[0].getLong(4);
                voltQueueSQL(GetAirportInfo, f_depart_airport);
                voltQueueSQL(GetAirportInfo, f_arrive_airport);
            } // WHILE
            final VoltTable[] airportResults = voltExecuteSQL(true);
            assert(flightResults[0].getRowCount()*2 == airportResults.length);
            
            flightResults[0].resetRowPosition();
            int i = -1;
            boolean adv;
            while (flightResults[0].advanceRow()) {
                Object row[] = new Object[RESULT_COLS.length];
                int r = 0;
                
                row[r++] = flightResults[0].getLong(0);             // [00] F_ID
                row[r++] = flightResults[0].getString(6);           // [01] AL_NAME
                
                adv = airportResults[++i].advanceRow();
                assert(adv);
                row[r++] = flightResults[0].getTimestampAsTimestamp(3); // [03] DEPART_TIME
                row[r++] = airportResults[i].getString(0);          // [04] DEPART_AP_CODE
                row[r++] = airportResults[i].getString(1);          // [05] DEPART_AP_NAME
                row[r++] = airportResults[i].getString(2);          // [06] DEPART_AP_CITY
                row[r++] = airportResults[i].getString(6);          // [07] DEPART_AP_COUNTRY
                
                adv = airportResults[++i].advanceRow();
                assert(adv);
                row[r++] = flightResults[0].getTimestampAsTimestamp(5); // [08] ARRIVE_TIME
                row[r++] = airportResults[i].getString(0);          // [09] ARRIVE_AP_CODE
                row[r++] = airportResults[i].getString(1);          // [10] ARRIVE_AP_NAME
                row[r++] = airportResults[i].getString(2);          // [11] ARRIVE_AP_CITY
                row[r++] = airportResults[i].getString(6);          // [12] ARRIVE_AP_COUNTRY
                
                finalResults.addRow(row);
                if (debug)
                    LOG.debug(String.format("Flight %d / %s /  %s -> %s / %s",
                                            row[0], row[2], row[4], row[9], row[03]));
            } // WHILE
        }
        if (debug && finalResults.getRowCount() > 0) LOG.debug("Flight Information:\n" + finalResults);
        return (finalResults);
    }
}
