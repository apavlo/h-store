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
/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */

package edu.brown.benchmark.seats.procedures;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.*;

import edu.brown.benchmark.seats.SEATSConstants;
import edu.brown.benchmark.seats.util.ErrorType;

@ProcInfo(
    partitionInfo = "RESERVATION.R_F_ID: 0"
)
public class FindOpenSeats extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(FindOpenSeats.class);
    
    private final VoltTable.ColumnInfo outputColumns[] = {
        new VoltTable.ColumnInfo("F_ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("SEAT", VoltType.INTEGER),
        new VoltTable.ColumnInfo("PRICE", VoltType.FLOAT),
    };
    
    public final SQLStmt GetFlight = new SQLStmt(
        "SELECT F_ID, F_AL_ID, F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, " +
        "       F_BASE_PRICE, F_SEATS_TOTAL, F_SEATS_LEFT, " +
        "       (F_BASE_PRICE + (F_BASE_PRICE * (1 - (F_SEATS_LEFT / F_SEATS_TOTAL)))) AS F_PRICE " +
        "  FROM " + SEATSConstants.TABLENAME_FLIGHT +
        " WHERE F_ID = ?"
    );
    
    public final SQLStmt GetSeats = new SQLStmt(
        "SELECT R_ID, R_F_ID, R_SEAT " + 
        "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ?"
    );
    
    public VoltTable[] run(long f_id) {
        final boolean debug = LOG.isDebugEnabled();
        
        // Empty seats bitmap
        final long seatmap[] = new long[SEATSConstants.FLIGHTS_NUM_SEATS];
        Arrays.fill(seatmap, -1);
        
        voltQueueSQL(GetFlight, f_id);
        voltQueueSQL(GetSeats, f_id);
        final VoltTable[] results = voltExecuteSQL(true);
        assert (results.length == 2);
        
        // First calculate the seat price using the flight's base price
        // and the number of seats that remaining
        if (results[0].advanceRow() == false) {
            throw new VoltAbortException(ErrorType.INVALID_FLIGHT_ID +
                                         String.format(" Invalid flight #%d", f_id));
        }
        
        int col = 6;
        double base_price = results[0].getDouble(col++);    // F_BASE_PRICE
        long seats_total = results[0].getLong(col++);       // F_SEATS_TOTAL
        long seats_left = results[0].getLong(col++);        // F_SEATS_LEFT
        double seat_price = results[0].getDouble(col++);    // MATHS!
        
        if (debug) {
            // TODO: Figure out why this doesn't match the SQL
            double _seat_price = base_price + (base_price * (1.0 - (seats_left/(double)seats_total)));
            LOG.debug(String.format("Flight %d - SQL[%.2f] <-> JAVA[%.2f] [basePrice=%f, total=%d, left=%d]",
                      f_id, seat_price, _seat_price, base_price, seats_total, seats_left));
        }
        
        // Then build the seat map of the remaining seats
        while (results[1].advanceRow()) {
            long r_id = results[1].getLong(0);
            int seatnum = (int)results[1].getLong(2);
            if (debug)
                LOG.debug(String.format("ROW fid %d rid %d seat %d", f_id, r_id, seatnum));
            assert(seatmap[seatnum] == -1) : "Duplicate seat reservation: R_ID=" + r_id;
            seatmap[seatnum] = 1; // results[1].getLong(1);
        } // WHILE
        
        int ctr = 0;
        VoltTable returnResults = new VoltTable(outputColumns);
        for (int i = 0; i < seatmap.length; ++i) {
            if (seatmap[i] == -1) {
                Object[] row = new Object[]{ f_id, i, seat_price };
                returnResults.addRow(row);
                if (ctr == SEATSConstants.FLIGHTS_NUM_SEATS) break;
            }
        } // FOR
//        assert(seats_left == returnResults.getRowCount()) :
//            String.format("Flight %d - Expected[%d] != Actual[%d]", f_id, seats_left, returnResults.getRowCount());
       
        if (debug)
            LOG.debug(String.format("Flight %d Open Seats:\n%s", f_id, returnResults));
        return new VoltTable[]{ results[0], returnResults };
    }
            
}
