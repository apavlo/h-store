package org.voltdb.regressionsuites.prefetchprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.utils.ThreadUtil;

@ProcInfo(
    partitionInfo = "TABLEA.A_ID: 0",
    singlePartition = true
)
public class SquirrelsSingle extends VoltProcedure {
    
    public final SQLStmt getLocal = new SQLStmt(
        "SELECT * FROM TABLEA WHERE A_ID = ?"
    );
    
    public VoltTable[] run(long a_id, long sleep) {
        System.err.printf("Sleeping for %.01f seconds\n", sleep / 1000d);
        ThreadUtil.sleep(sleep);
        System.err.println("Awake!");
        
        voltQueueSQL(getLocal, a_id);
        final VoltTable a_results[] = voltExecuteSQL();
        assert(a_results.length == 1);
        return (a_results);
    }

}
