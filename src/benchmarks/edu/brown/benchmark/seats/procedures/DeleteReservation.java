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

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.seats.SEATSConstants;

@ProcInfo(
    partitionInfo = "RESERVATION.R_F_ID: 0"
)
public class DeleteReservation extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(DeleteReservation.class);
    
    public final SQLStmt GetFlight = new SQLStmt(
        "SELECT F_AL_ID, F_SEATS_LEFT, " +
        "       F_IATTR00, F_IATTR01,  F_IATTR02, F_IATTR03, " + 
        "       F_IATTR04, F_IATTR05,  F_IATTR06, F_IATTR07 " +
        "  FROM " + SEATSConstants.TABLENAME_FLIGHT + 
        " WHERE F_ID = ?");
    
    public final SQLStmt GetCustomerByIdStr = new SQLStmt(
        "SELECT C_ID " + 
        "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + 
        " WHERE C_ID_STR = ?");
    
    public final SQLStmt GetCustomerByFFNumber = new SQLStmt(
        "SELECT C_ID, FF_AL_ID " +
        "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + ", " + 
                    SEATSConstants.TABLENAME_FREQUENT_FLYER + 
        " WHERE FF_C_ID_STR = ? AND FF_C_ID = C_ID");
    
    public final SQLStmt GetCustomerReservation = new SQLStmt(
        "SELECT C_SATTR00, C_SATTR02, C_SATTR04, " +
        "       C_IATTR00, C_IATTR02, C_IATTR04, C_IATTR06, " +
        "       F_SEATS_LEFT, " +
        "       R_ID, R_SEAT, R_PRICE, R_IATTR00 " +
        "  FROM " + SEATSConstants.TABLENAME_CUSTOMER + ", " +
                    SEATSConstants.TABLENAME_FLIGHT + ", " +
                    SEATSConstants.TABLENAME_RESERVATION +
        " WHERE C_ID = ? AND C_ID = R_C_ID " +
        "   AND F_ID = ? AND F_ID = R_F_ID "
    );
    
    public final SQLStmt DeleteReservation = new SQLStmt(
        "DELETE FROM " + SEATSConstants.TABLENAME_RESERVATION +
        " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?");

    public final SQLStmt UpdateFlight = new SQLStmt(
        "UPDATE " + SEATSConstants.TABLENAME_FLIGHT +
        "   SET F_SEATS_LEFT = F_SEATS_LEFT + 1 " + 
        " WHERE F_ID = ? ");
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
        "UPDATE " + SEATSConstants.TABLENAME_CUSTOMER +
        "   SET C_BALANCE = C_BALANCE + ?, " +
        "       C_IATTR00 = ?, " +
        "       C_IATTR10 = C_IATTR10 - 1, " + 
        "       C_IATTR11 = C_IATTR10 - 1 " +
        " WHERE C_ID = ? ");
    
    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
        "UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
        "   SET FF_IATTR10 = FF_IATTR10 - 1 " + 
        " WHERE FF_C_ID = ? " +
        "   AND FF_AL_ID = ?");
    
    public long run(long f_id, long c_id, String c_id_str, String ff_c_id_str) {
        final boolean debug = LOG.isDebugEnabled();
        
        // Get flight information
        voltQueueSQL(GetFlight, f_id);
        final VoltTable flight[] = voltExecuteSQL();
        long ff_al_id = VoltType.NULL_BIGINT;
        if (flight[0].advanceRow()) {
            ff_al_id = flight[0].getLong(0);
        } else {
            String msg = String.format("Invalid flight id [f_id=%d]", f_id);
            throw new VoltAbortException(msg);
        }
        
        // If we weren't given the customer id, then look it up
        if (c_id == VoltType.NULL_BIGINT) {
            boolean has_al_id = false;
            
            // Use the customer's id as a string
            if (c_id_str != null && c_id_str.length() > 0) {
                voltQueueSQL(GetCustomerByIdStr, c_id_str);
            // Otherwise use their FrequentFlyer information
            } else {
                assert(ff_c_id_str.isEmpty() == false);
                assert(ff_al_id != VoltType.NULL_BIGINT);
                voltQueueSQL(GetCustomerByFFNumber, ff_c_id_str);
            }
            VoltTable[] customerIdResults = voltExecuteSQL();
            assert(customerIdResults.length == 1);
            if (customerIdResults[0].advanceRow()) {
                c_id = customerIdResults[0].getLong(0);
                if (has_al_id) ff_al_id = customerIdResults[0].getLong(1);
            } else {
                String msg = String.format("No Customer record was found [c_id_str=%s, ff_c_id_str=%s, ff_al_id=%s]",
                                           c_id_str, ff_c_id_str, ff_al_id);
                throw new VoltAbortException(msg);
            }
        }

        // Now get the result of the information that we need
        // If there is no valid customer record, then throw an abort
        // This should happen 5% of the time
        voltQueueSQL(GetCustomerReservation, c_id, f_id);
        VoltTable customer[] = voltExecuteSQL();
        assert(customer.length == 1);
        if (customer[0].advanceRow() == false) {
            String msg = String.format("No Customer information record found for id '%d'", c_id);
            throw new VoltAbortException(msg);
        }
        long c_iattr00 = customer[0].getLong(3) + 1;
        long seats_left = customer[0].getLong(7); 
        long r_id = customer[0].getLong(8);
        double r_price = customer[0].getDouble(10);
        
        // Now delete all of the flights that they have on this flight
        voltQueueSQL(DeleteReservation, r_id, c_id, f_id);
        voltQueueSQL(UpdateFlight, f_id);
        voltQueueSQL(UpdateCustomer, -1*r_price, c_iattr00, c_id);
        if (ff_al_id != VoltType.NULL_BIGINT) {
            voltQueueSQL(UpdateFrequentFlyer, c_id, ff_al_id);
        }
        
        final VoltTable[] results = voltExecuteSQL(true);
        for (int i = 0; i < results.length - 1; i++) {
            if (results[i].getRowCount() != 1) {
                String msg = String.format("Failed to delete reservation for flight %d - No rows returned for %s", f_id, voltLastQueriesExecuted()[i]);
                if (debug) LOG.warn(msg);
                throw new VoltAbortException(msg);
            }
            long updated = results[i].asScalarLong();
            if (updated != 1) {
                String msg = String.format("Failed to delete reservation for flight %d - Updated %d records for %s", f_id, updated, voltLastQueriesExecuted()[i]);
                if (debug) LOG.warn(msg);
                throw new VoltAbortException(msg);
            }
        } // FOR
        
        if (debug)
            LOG.debug(String.format("Deleted reservation on flight %d for customer %d [seatsLeft=%d]", f_id, c_id, seats_left+1));        
        return (1l);
    }

}
