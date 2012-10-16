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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.seats.SEATSConstants;
import edu.brown.benchmark.seats.util.ErrorType;

@ProcInfo(
    partitionInfo = "CUSTOMER.C_ID: 0"
)
public class UpdateCustomer extends VoltProcedure {
    
    public final SQLStmt GetCustomerIdStr = new SQLStmt(
        "SELECT C_ID " +
        "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID_STR = ? "
    );
    
    public final SQLStmt GetCustomer = new SQLStmt(
        "SELECT * " +
        "  FROM " + SEATSConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID = ? "
    );
    
    public final SQLStmt GetBaseAirport = new SQLStmt(
        "SELECT * " +
        "  FROM " + SEATSConstants.TABLENAME_AIRPORT + ", " +
                    SEATSConstants.TABLENAME_COUNTRY +
        " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
    );
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
        "UPDATE " + SEATSConstants.TABLENAME_CUSTOMER +
        "   SET C_IATTR00 = ?, " +
        "       C_IATTR01 = ? " +
        " WHERE C_ID = ?"
    );
    
    public final SQLStmt GetFrequentFlyers = new SQLStmt(
        "SELECT * FROM " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
        " WHERE FF_C_ID = ?"
    );
            
    public final SQLStmt UpdateFrequentFlyers = new SQLStmt(
        "UPDATE " + SEATSConstants.TABLENAME_FREQUENT_FLYER +
        "   SET FF_IATTR00 = ?, " +
        "       FF_IATTR01 = ? " +
        " WHERE FF_C_ID = ? " + 
        "   AND FF_AL_ID = ? "
    );
    
    public VoltTable[] run(long c_id, String c_id_str, long update_ff, long attr0, long attr1) {
        // Use C_ID_STR to get C_ID
        if (c_id == VoltType.NULL_BIGINT) {
            assert(c_id_str != null);
            assert(c_id_str.isEmpty() == false);
            voltQueueSQL(GetCustomerIdStr, c_id_str);
            VoltTable[] customerIdResults = voltExecuteSQL();
            assert(customerIdResults.length == 1);
            if (customerIdResults[0].getRowCount() == 1) {
                c_id = customerIdResults[0].asScalarLong();
            } else {
                throw new VoltAbortException(String.format("No Customer information record found for string '%s'", c_id_str));
            }
        }
        
        voltQueueSQL(GetCustomer, c_id);
        VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);
        if (results[0].getRowCount() == 0) {
            throw new VoltAbortException(String.format("No Customer information record found for id '%d'", c_id));
        }
        boolean adv = results[0].advanceRow();
        assert(adv);
        assert(c_id == results[0].getLong(0));
        long base_airport = results[0].getLong(2);
        
        // Get their airport information
        // TODO: Do something interesting with this data
        voltQueueSQL(GetBaseAirport, base_airport);
        VoltTable airport_results[] = voltExecuteSQL();
        assert(airport_results.length == 1);
        
        if (update_ff > 0) {
            voltQueueSQL(GetFrequentFlyers, c_id); 
            VoltTable ff_results[] = voltExecuteSQL();
            assert(results.length == 1);
            while (ff_results[0].advanceRow()) {
                long ff_al_id = ff_results[0].getLong(1); 
                voltQueueSQL(UpdateFrequentFlyers, attr0, attr1, c_id, ff_al_id);
            } // WHILE
        }
        
        voltQueueSQL(UpdateCustomer, attr0, attr1, c_id);
        results = voltExecuteSQL(true);
        long updated = results[results.length - 1].asScalarLong();
        if (updated != 1) {
            String msg = String.format("Failed to update customer #%d - Updated %d records", c_id, updated);
            throw new VoltAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
        }
        
        assert (results.length >= 1);
        return (results);
    }
}
