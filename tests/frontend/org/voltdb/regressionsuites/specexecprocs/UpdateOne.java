package org.voltdb.regressionsuites.specexecprocs;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.utils.ThreadUtil;
@ProcInfo(
    partitionParam = 0,
    singlePartition = true
)
public class UpdateOne extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(UpdateOne.class);
    
    public final SQLStmt getLocal = new SQLStmt(
        "SELECT S_ID, MSC_LOCATION " +
        "  FROM " + TM1Constants.TABLENAME_SUBSCRIBER +
        " WHERE S_ID != ?"
    );
    
    public final SQLStmt localUpdate = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SUBSCRIBER +
        "   SET MSC_LOCATION = ? " +
        " WHERE S_ID = ?"
    );
    
    public VoltTable[] run(int partition) {
        VoltTable results[] = null;
        
        // First execute a remote query
        voltQueueSQL(localUpdate, 5431, partition);
        results = voltExecuteSQL();
        assert(results.length == 1);
        LOG.info(this.getTransactionState() + " - LOCAL UPDATE:\n" + results[0]);

        return (results);
    }

}
