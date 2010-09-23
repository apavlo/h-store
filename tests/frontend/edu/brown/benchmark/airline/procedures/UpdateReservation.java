package edu.brown.benchmark.airline.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)    
public class UpdateReservation extends VoltProcedure {
    
    public final String BASE_SQL = "UPDATE " + AirlineConstants.TABLENAME_RESERVATION + " SET ";
    public final String END_SQL  = " WHERE R_ID = ?";
    
    public final SQLStmt UPDATE0 = new SQLStmt(
            BASE_SQL + "R_IATTR00 = ?" + END_SQL);
    public final SQLStmt UPDATE1 = new SQLStmt(
            BASE_SQL + "R_IATTR01 = ?" + END_SQL);
    public final SQLStmt UPDATE2 = new SQLStmt(
            BASE_SQL + "R_IATTR02 = ?" + END_SQL);
    public final SQLStmt UPDATE3 = new SQLStmt(
            BASE_SQL + "R_IATTR03 = ?" + END_SQL);

    public static final int NUM_UPDATES = 4;
    public final SQLStmt UPDATES[] = {
            UPDATE0,
            UPDATE1,
            UPDATE2,
            UPDATE3,
    };
    
    public VoltTable[] run(long rid, long value, long attribute_idx) {
        assert(attribute_idx >= 0);
        assert(attribute_idx < UPDATES.length);
        
        voltQueueSQL(UPDATES[(int)attribute_idx], value, rid);
        VoltTable[] results = voltExecuteSQL();
        assert results.length == 1;
        return results;
    } 
}
