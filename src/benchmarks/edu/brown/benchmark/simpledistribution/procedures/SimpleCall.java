package edu.brown.benchmark.simpledistribution.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.ProcedureStatsCollector;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
        singlePartition = true
    )
public class SimpleCall extends VoltProcedure {
    
    public final SQLStmt insertS1 = new SQLStmt("INSERT INTO S1 (value) VALUES (0);");

    public long run() {

        voltQueueSQL(insertS1);
        voltExecuteSQL();

        return 0;
    }
}
