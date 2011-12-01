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
package edu.brown.benchmark.airline.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.airline.AirlineConstants;
import edu.brown.benchmark.airline.util.CustomerId;

@ProcInfo(
    singlePartition = false
)
public class NewReservation extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(NewReservation.class);
    
    public final SQLStmt GetFlight = new SQLStmt(
            "SELECT F_AL_ID, F_SEATS_LEFT, " +
                    AirlineConstants.TABLENAME_AIRLINE + ".* " +
            "  FROM " + AirlineConstants.TABLENAME_FLIGHT + ", " +
                        AirlineConstants.TABLENAME_AIRLINE +
            " WHERE F_ID = ? AND F_AL_ID = AL_ID");
    
    public final SQLStmt GetCustomer = new SQLStmt(
            "SELECT C_BASE_AP_ID, C_BALANCE, C_SATTR00 " +
            "  FROM " + AirlineConstants.TABLENAME_CUSTOMER +
            " WHERE C_ID = ? ");
    
    public final SQLStmt CheckSeat = new SQLStmt(
            "SELECT R_ID " +
            "  FROM " + AirlineConstants.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? and R_SEAT = ?");
    
    public final SQLStmt CheckCustomer = new SQLStmt(
            "SELECT R_ID " + 
            "  FROM " + AirlineConstants.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? AND R_C_ID = ?");
    
    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_FLIGHT +
            "   SET F_SEATS_LEFT = F_SEATS_LEFT - 1 " + 
            " WHERE F_ID = ? ");
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_CUSTOMER +
            "   SET C_IATTR10 = C_IATTR10 + 1, " + 
            "       C_IATTR11 = C_IATTR11 + 1, " +
            "       C_IATTR12 = ?, " +
            "       C_IATTR13 = ?, " +
            "       C_IATTR14 = ?, " +
            "       C_IATTR15 = ? " +
            " WHERE C_ID = ? ");
    
    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR10 = FF_IATTR10 + 1, " + 
            "       FF_IATTR11 = ?, " +
            "       FF_IATTR12 = ?, " +
            "       FF_IATTR13 = ?, " +
            "       FF_IATTR14 = ? " +
            " WHERE FF_C_ID = ? " +
            "   AND FF_AL_ID = ?");
    
    public final SQLStmt InsertReservation = new SQLStmt(
            "INSERT INTO " + AirlineConstants.TABLENAME_RESERVATION + " (" +
            "   R_ID, " +
            "   R_C_ID, " +
            "   R_F_ID, " +
            "   R_SEAT, " +
            "   R_PRICE, " +
            "   R_IATTR00, " +
            "   R_IATTR01, " +
            "   R_IATTR02, " +
            "   R_IATTR03, " +
            "   R_IATTR04, " +
            "   R_IATTR05, " +
            "   R_IATTR06, " +
            "   R_IATTR07, " +
            "   R_IATTR08 " +
            ") VALUES (" +
            "   ?, " +  // R_ID
            "   ?, " +  // R_C_ID
            "   ?, " +  // R_F_ID
            "   ?, " +  // R_SEAT
            "   ?, " +  // R_PRICE
            "   ?, " +  // R_ATTR00
            "   ?, " +  // R_ATTR01
            "   ?, " +  // R_ATTR02
            "   ?, " +  // R_ATTR03
            "   ?, " +  // R_ATTR04
            "   ?, " +  // R_ATTR05
            "   ?, " +  // R_ATTR06
            "   ?, " +  // R_ATTR07
            "   ? " +   // R_ATTR08
            ")");
    
    public VoltTable[] run(long r_id, long c_id, long f_id, long seatnum, double price, long attrs[]) throws VoltAbortException {
        final boolean debug = LOG.isDebugEnabled();
        
        voltQueueSQL(GetFlight, f_id);
        voltQueueSQL(CheckSeat, f_id, seatnum);
        voltQueueSQL(CheckCustomer, f_id, c_id);
        voltQueueSQL(GetCustomer, c_id);
        final VoltTable[] initialResults = voltExecuteSQL();
        assert(initialResults.length == 4);

        // Flight Information
        if (initialResults[0].getRowCount() != 1) {
            throw new VoltAbortException("Invalid flight id: " + f_id);
        }
        initialResults[0].advanceRow();
        if (initialResults[0].getLong(1) <= 0) {
            throw new VoltAbortException("No more seats available for flight #" + f_id);
        }
        long airline_id = initialResults[0].getLong(0);
        long seats_left = initialResults[0].getLong(1);
        
        // Check for existing reservation
        if (initialResults[1].getRowCount() > 0) {
            throw new VoltAbortException(String.format("Seat %d is already reserved on flight #%d", seatnum, f_id));
        }
        // Or the customer trying to book themselves twice
        else if (initialResults[2].getRowCount() > 1) {
            throw new VoltAbortException(String.format("Customer %d already owns on a reservations on flight #%d", c_id, f_id));
        }
        // Customer Information
        else if (initialResults[3].getRowCount() != 1) {
            throw new VoltAbortException(String.format("Invalid customer id: %d / %s", c_id, new CustomerId(c_id)));
        }
        
        voltQueueSQL(InsertReservation, r_id, c_id, f_id, seatnum, price,
                            attrs[0], attrs[1], attrs[2], attrs[3],
                            attrs[4], attrs[5], attrs[6], attrs[7],
                            attrs[8]);
        voltQueueSQL(UpdateFlight, f_id);
        voltQueueSQL(UpdateCustomer, attrs[0], attrs[1], attrs[2], attrs[3], c_id);
        voltQueueSQL(UpdateFrequentFlyer, attrs[4], attrs[5], attrs[6], attrs[7], c_id, airline_id);
        
        // We don't care if we updated FrequentFlyer 
        final VoltTable[] results = voltExecuteSQL();
        for (int i = 0; i < results.length - 1; i++) {
            if (results[i].getRowCount() != 1) {
                String msg = String.format("Failed to add reservation for flight %d - No rows returned for %s", f_id, voltLastQueriesExecuted()[i]);
                if (debug) LOG.warn(msg);
                throw new VoltAbortException(msg);
            }
            long updated = results[i].asScalarLong();
            if (updated != 1) {
                String msg = String.format("Failed to add reservation for flight %d - Updated %d records for %s", f_id, updated, voltLastQueriesExecuted()[i]);
                if (debug) LOG.warn(msg);
                throw new VoltAbortException(msg);
            }
        } // FOR
        
        if (debug) LOG.debug(String.format("Reserved new seat on flight %d for customer %d [seatsLeft=%d]", f_id, c_id, seats_left-1));
        
        return (results);
    }
}
