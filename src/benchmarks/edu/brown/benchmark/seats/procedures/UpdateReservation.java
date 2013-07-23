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

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.seats.SEATSConstants;
import edu.brown.benchmark.seats.util.ErrorType;

@ProcInfo(
    partitionInfo = "RESERVATION.R_F_ID: 2"
)    
public class UpdateReservation extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(UpdateReservation.class);
    
    public final SQLStmt CheckSeat = new SQLStmt(
        "SELECT R_ID " +
        "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ? and R_SEAT = ?"
    );

    public final SQLStmt CheckCustomer = new SQLStmt(
        "SELECT R_ID " + 
        "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ? AND R_C_ID = ?"
    );
    
    public final SQLStmt GetSeats = new SQLStmt(
        "SELECT R_ID, R_F_ID, R_SEAT " + 
        "  FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ? ORDER BY R_SEAT ASC "
    );

    private static final String BASE_SQL = "UPDATE " + SEATSConstants.TABLENAME_RESERVATION +
                                           "   SET R_SEAT = ?, R_UPDATED = ?, %s = ? " +
                                           " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?";
    
    public final SQLStmt ReserveSeat0 = new SQLStmt(String.format(BASE_SQL, "R_IATTR00"));
    public final SQLStmt ReserveSeat1 = new SQLStmt(String.format(BASE_SQL, "R_IATTR01"));
    public final SQLStmt ReserveSeat2 = new SQLStmt(String.format(BASE_SQL, "R_IATTR02"));
    public final SQLStmt ReserveSeat3 = new SQLStmt(String.format(BASE_SQL, "R_IATTR03"));

    public static final int NUM_UPDATES = 4;
    public final SQLStmt ReserveSeats[] = {
        ReserveSeat0,
        ReserveSeat1,
        ReserveSeat2,
        ReserveSeat3,
    };
    
    public VoltTable[] run(long r_id, long c_id, long f_id, long seatnum, long attr_idx, long attr_val) {
        final boolean debug = LOG.isDebugEnabled();
        assert(attr_idx >= 0);
        assert(attr_idx < ReserveSeats.length);
        
        // check if the seat is occupied
        // check if the customer has multiple seats on this flight
        voltQueueSQL(CheckSeat, f_id, seatnum);
        voltQueueSQL(CheckCustomer, f_id, c_id);
        final VoltTable[] checkResults = voltExecuteSQL();
        assert(checkResults.length == 2);
        
        // Check if Seat is Available
        if (checkResults[0].advanceRow()) {
            throw new VoltAbortException(ErrorType.SEAT_ALREADY_RESERVED +
                                         String.format(" Seat %d is already reserved on flight #%d", seatnum, f_id));
        }
        // Check if the Customer already has a seat on this flight
        else if (checkResults[1].advanceRow() == false) {
            throw new VoltAbortException(ErrorType.RESERVATION_NOT_FOUND +
                                         String.format(" Customer %d does not have an existing reservation on flight #%d", c_id, f_id));
        }
       
        // Update the seat reservation for the customer
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(ReserveSeats[(int)attr_idx], seatnum, timestamp, attr_val, r_id, c_id, f_id);
        final VoltTable[] results = voltExecuteSQL(true);
        assert results.length == 1;
        for (int i = 0; i < results.length - 1; i++) {
            if (results[i].getRowCount() != 1) {
                String msg = String.format("Failed to update reservation for flight %d - No rows returned for %s", f_id, voltLastQueriesExecuted()[i]);
                if (debug) LOG.warn(msg);
                throw new VoltAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
            }
            long updated = results[i].asScalarLong();
            if (updated == 0) {
                String msg = String.format("Failed to update reservation for flight %d - Did not update any records for %s", f_id, voltLastQueriesExecuted()[i]);
                if (debug) LOG.warn(msg);
                throw new VoltAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
            }
        } // FOR
        
        if (debug) {
            if (LOG.isTraceEnabled()) LOG.trace(this.getTransactionState().debug());
            LOG.debug(String.format("Updated reservation on flight %d for customer %d", f_id, c_id));
        }
        return results;
    } 
}
