package edu.brown.benchmark.airline.procedures;

import java.util.Arrays;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.airline.AirlineConstants;

public class DeleteReservation extends VoltProcedure {

    public final SQLStmt DeleteReservation = new SQLStmt(
            "DELETE FROM " + AirlineConstants.TABLENAME_RESERVATION +
            " WHERE R_C_ID = ? AND R_F_ID = ?");

    public final SQLStmt GetCustomerById = new SQLStmt(
            "SELECT C_ID, C_SATTR00, C_SATTR01, C_SATTR02, C_SATTR03, C_SATTR04, C_SATTR05," +
            "             C_IATTR00, C_IATTR01, C_IATTR02, C_IATTR03, C_IATTR04, C_IATTR05, C_IATTR06" +
            "  FROM " + AirlineConstants.TABLENAME_CUSTOMER + 
            " WHERE C_ID = ?");
    
    public final SQLStmt GetCustomerByName = new SQLStmt(
            "SELECT C_ID, C_SATTR00, C_SATTR01, C_SATTR02, C_SATTR03, C_SATTR04, C_SATTR05," +
            "             C_IATTR00, C_IATTR01, C_IATTR02, C_IATTR03, C_IATTR04, C_IATTR05, C_IATTR06" +
            "  FROM " + AirlineConstants.TABLENAME_CUSTOMER + 
            " WHERE C_SATTR00 = ?");
    
    public final SQLStmt GetCustomerByFFNumber = new SQLStmt(
            "SELECT C_ID, C_SATTR00, C_SATTR01, C_SATTR02, C_SATTR03, C_SATTR04, C_SATTR05," +
            "             C_IATTR00, C_IATTR01, C_IATTR02, C_IATTR03, C_IATTR04, C_IATTR05, C_IATTR06" +
            "  FROM " + AirlineConstants.TABLENAME_CUSTOMER + ", " + AirlineConstants.TABLENAME_FREQUENT_FLYER + 
            " WHERE FF_AL_ID = ? AND FF_SATTR00 = ? AND FF_C_ID = C_ID");
    
    public VoltTable[] run(long f_id, long c_id, String c_name, String ff_id, long ff_al_id) {
        
        // 60% of the time we will be give the customer's id
        if (c_id != VoltType.NULL_BIGINT) {
            voltQueueSQL(GetCustomerById, c_id);
        // 20% of the time we will be given the customer's name
        } else if (c_name.length() > 0) {
            voltQueueSQL(GetCustomerByName, c_id);
        // 20% of the time we will be given their FrequentFlyer information
        } else {
            assert(ff_id.isEmpty() == false);
            assert(ff_al_id != VoltType.NULL_BIGINT);
            voltQueueSQL(GetCustomerByFFNumber, ff_al_id, ff_id);
        }
        
        // If there is no valid customer record, then throw an abort
        // This should happen 5% of the time
        VoltTable customer[] = voltExecuteSQL();
        assert(customer.length == 1);
        if (customer[0].getRowCount() == 0) {
            throw new VoltAbortException("No CUSTOMER record was found: " + Arrays.toString(voltLastQueriesExecuted()));
        }
        customer[0].advanceRow();
        c_id = customer[0].getLong(0);
        
        // Now delete all of the flights that they have on this flight
        voltQueueSQL(DeleteReservation, c_id);
        return (voltExecuteSQL(true));
    }

}
