package edu.brown.benchmark.simplestatement.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.ProcedureStatsCollector;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
        singlePartition = true
    )
public class SimpleCall extends VoltProcedure {
    
    public final SQLStmt selectStatement = new SQLStmt(
            "SELECT * FROM S1;"
        );
    
    public long run() {

        voltQueueSQL(selectStatement);

        long startNanoTime = System.nanoTime(); // added by hawk, 2013/12/11, for micro-benchmark 3
        
        voltExecuteSQL(true);
        
        // Begin : HStoreSite.java micro-benchmark 2
        long endNanoTime = System.nanoTime();
        ProcedureStatsCollector collector = this.executor.getProcedureStatsSource();
        if(collector != null)
        {
            boolean aborted = false;
            boolean failed = false;
            // reuse ProcedureStatsCollector to do micro-benchmark 3 
            collector.addTransactionInfo(aborted, failed, startNanoTime, endNanoTime);
        }
        // End : HStoreSite.java micro-benchmark 2
        
        return 0;
    }
}
