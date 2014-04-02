package edu.brown.benchmark.simpledistribution.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.ProcedureStatsCollector;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
        //partitionInfo = "TABLEA.A_ID: 0",
        singlePartition = false
    )
public class SimpleCall extends VoltProcedure {
    
    public final SQLStmt selectStatement = new SQLStmt(
            "SELECT * FROM TABLEA;"
        );
    
    public VoltTable[] run() {

        voltQueueSQL(selectStatement);

        return voltExecuteSQL();
    }
}
