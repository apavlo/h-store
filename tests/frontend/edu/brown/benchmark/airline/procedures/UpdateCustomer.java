package edu.brown.benchmark.airline.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)
public class UpdateCustomer extends VoltProcedure {
    
    public final SQLStmt GetCustomerIdStr = new SQLStmt(
        "SELECT C_ID " +
        "  FROM " + AirlineConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID_STR = ? "
    );
    
    public final SQLStmt GetCustomer = new SQLStmt(
        "SELECT * " +
        "  FROM " + AirlineConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID = ? "
    );
    
    public final SQLStmt GetBaseAirport = new SQLStmt(
        "SELECT * " +
        "  FROM " + AirlineConstants.TABLENAME_AIRPORT + ", " +
                    AirlineConstants.TABLENAME_COUNTRY +
        " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
    );
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
        "UPDATE " + AirlineConstants.TABLENAME_CUSTOMER +
        "   SET C_IATTR00 = ?, " +
        "       C_IATTR01 = ? " +
        " WHERE C_ID = ?"
    );
    
    public final SQLStmt GetFrequentFlyers = new SQLStmt(
        "SELECT * FROM " + AirlineConstants.TABLENAME_FREQUENT_FLYER +
        " WHERE FF_C_ID = ?"
    );
            
    public final SQLStmt UpdatFrequentFlyers = new SQLStmt(
        "UPDATE " + AirlineConstants.TABLENAME_FREQUENT_FLYER +
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
                voltQueueSQL(UpdatFrequentFlyers, attr0, attr1, c_id, ff_al_id);
            } // WHILE
        }
        
        voltQueueSQL(UpdateCustomer, attr0, attr1, c_id);
        results = voltExecuteSQL();
        assert (results.length >= 1);
        return (results);
    }
}
