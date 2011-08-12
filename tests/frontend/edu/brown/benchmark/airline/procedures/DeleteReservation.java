package edu.brown.benchmark.airline.procedures;

import java.util.Arrays;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.airline.AirlineConstants;

public class DeleteReservation extends VoltProcedure {
    
    public static final String GetCustomerBase = "C_ID, C_SATTR00, C_SATTR01, C_SATTR02, C_SATTR03, C_SATTR04, C_SATTR05," +
                                                 "C_IATTR00, C_IATTR01, C_IATTR02, C_IATTR03, C_IATTR04, C_IATTR05, C_IATTR06";
    
    public final SQLStmt GetCustomerById = new SQLStmt(
            "SELECT " + GetCustomerBase + 
            "  FROM " + AirlineConstants.TABLENAME_CUSTOMER + 
            " WHERE C_ID = ?");
    
    public final SQLStmt GetCustomerByIdStr = new SQLStmt(
            "SELECT " + GetCustomerBase + 
            "  FROM " + AirlineConstants.TABLENAME_CUSTOMER + 
            " WHERE C_ID_STR = ?");
    
    public final SQLStmt GetCustomerByFFNumber = new SQLStmt(
            "SELECT " + GetCustomerBase +
            "  FROM " + AirlineConstants.TABLENAME_CUSTOMER + ", " + AirlineConstants.TABLENAME_FREQUENT_FLYER + 
            " WHERE FF_C_ID_STR = ? AND FF_AL_ID = ? AND FF_C_ID = C_ID");
    
    public final SQLStmt DeleteReservation = new SQLStmt(
            "DELETE FROM " + AirlineConstants.TABLENAME_RESERVATION +
            " WHERE R_C_ID = ? AND R_F_ID = ?");

    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_FLIGHT +
            "   SET F_SEATS_LEFT = F_SEATS_LEFT + 1 " + 
            " WHERE F_ID = ? ");
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_CUSTOMER +
            "   SET C_IATTR10 = C_IATTR10 - 1, " + 
            "       C_IATTR11 = C_IATTR11 - 1 " +
            " WHERE C_ID = ? ");
    
    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR10 = FF_IATTR10 - 1 " + 
            " WHERE FF_C_ID_STR = ? " +
            "   AND FF_AL_ID = ?");
    
    public VoltTable[] run(long f_id, long c_id, String c_id_str, String ff_c_id_str, long ff_al_id) {
        
        // 60% of the time we will be give the customer's id
        if (c_id != VoltType.NULL_BIGINT) {
            voltQueueSQL(GetCustomerById, c_id);
        // 20% of the time we will be given the customer's id as a string
        } else if (c_id_str != null && c_id_str.length() > 0) {
            voltQueueSQL(GetCustomerByIdStr, c_id_str);
        // 20% of the time we will be given their FrequentFlyer information
        } else {
            assert(ff_c_id_str.isEmpty() == false);
            assert(ff_al_id != VoltType.NULL_BIGINT);
            voltQueueSQL(GetCustomerByFFNumber, ff_c_id_str, ff_al_id);
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
        voltQueueSQL(DeleteReservation, c_id, f_id);
        voltQueueSQL(UpdateFlight, f_id);
        voltQueueSQL(UpdateCustomer, c_id);
        if (ff_al_id != VoltType.NULL_BIGINT) {
            voltQueueSQL(UpdateCustomer, c_id, ff_al_id);
        }
        
        return (voltExecuteSQL(true));
    }

}
