package edu.brown.benchmark.simple.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
    partitionInfo = "STABLE.S_KEY: 0",
    singlePartition = true
)
public class GetData extends VoltProcedure {
    public final SQLStmt readStmt = new SQLStmt("SELECT * FROM STABLE WHERE S_KEY = ? ");

    public VoltTable[] run(long a_id) {
        voltQueueSQL(readStmt, a_id);
        return (voltExecuteSQL());
    }
}
