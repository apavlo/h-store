package edu.brown.benchmark.locality.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetRemote extends VoltProcedure {

    public final SQLStmt GET_A = new SQLStmt(
    "SELECT * FROM TABLEA WHERE A_ID = ? ");

    public final SQLStmt GET_B = new SQLStmt(
	"SELECT * FROM TABLEB WHERE B_A_ID = ? LIMIT BY 10");

    /**
     * 
     * @param a_id
     * @return
     */
    public VoltTable[] run(long local_a_id, long a_id) {
        voltQueueSQL(GET_A, a_id);
        voltQueueSQL(GET_B, a_id);
        final VoltTable[] AB_results = voltExecuteSQL();
        return AB_results;
    }
    
}
