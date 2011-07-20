package edu.brown.benchmark.airline.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    partitionInfo = "RESERVATION.R_F_ID: 0"
)    
public class UpdateReservation extends VoltProcedure {
    
    private final String BASE_SQL = "UPDATE " + AirlineConstants.TABLENAME_RESERVATION +
                                    "   SET %s = ? " +
                                    " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?";
    
    public final SQLStmt Update0 = new SQLStmt(String.format(BASE_SQL, "R_IATTR00"));
    public final SQLStmt Update1 = new SQLStmt(String.format(BASE_SQL, "R_IATTR01"));
    public final SQLStmt Update2 = new SQLStmt(String.format(BASE_SQL, "R_IATTR02"));
    public final SQLStmt Update3 = new SQLStmt(String.format(BASE_SQL, "R_IATTR03"));

    public static final int NUM_UPDATES = 4;
    public final SQLStmt UPDATES[] = {
        Update0,
        Update1,
        Update2,
        Update3,
    };
    
    public VoltTable[] run(long r_id, long c_id, long f_id, long value, long attribute_idx) {
        assert(attribute_idx >= 0);
        assert(attribute_idx < UPDATES.length);
        
        voltQueueSQL(UPDATES[(int)attribute_idx], value, r_id, c_id, f_id);
        VoltTable[] results = voltExecuteSQL();
        assert results.length == 1;
        return results;
    } 
}
