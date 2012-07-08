package edu.brown.benchmark.locality.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * @author sw47
 */
@ProcInfo(partitionInfo = "TABLEA.A_ID: 0", singlePartition = false)
public class Set extends VoltProcedure {

    public final SQLStmt WriteA = new SQLStmt("UPDATE TABLEA SET A_VALUE = ? WHERE A_ID = ?");

    public final SQLStmt WriteB = new SQLStmt("UPDATE TABLEB SET B_VALUE = ? WHERE B_ID = ? AND B_A_ID = ?");

    /**
     * @param local_a_id
     * @param a_id
     * @param a_value
     * @param b_id
     * @param b_value
     * @return
     */
    public VoltTable[] run(long local_a_id, long a_id, String a_value, long b_id, String b_value) {
        voltQueueSQL(WriteA, a_value, a_id);
        voltQueueSQL(WriteB, b_value, b_id, a_id);
        return (voltExecuteSQL());
    }

}
