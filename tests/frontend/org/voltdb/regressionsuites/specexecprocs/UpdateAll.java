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
    singlePartition = false
)
public class UpdateAll extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(UpdateAll.class);
    
    public final SQLStmt getLocal = new SQLStmt(
        "SELECT S_ID, MSC_LOCATION " +
        "  FROM " + TM1Constants.TABLENAME_SUBSCRIBER +
        " WHERE S_ID != ?"
    );
    
    public final SQLStmt broadcastUpdate = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SUBSCRIBER +
        "   SET MSC_LOCATION = ?"
    );
    
    public VoltTable[] run(int partition, long sleep) {
        VoltTable results[] = null;
        
        // First execute a remote query
        voltQueueSQL(broadcastUpdate, 1234);
        results = voltExecuteSQL();
        assert(results.length == 1);
        LOG.info(this.getTransactionState() + " - BROADCAST UPDATE:\n" + results[0]);

        // Then sleep for a bit
        LOG.info(String.format("%s - Sleeping for %.01f seconds", this.getTransactionState(), sleep / 1000d));
        ThreadUtil.sleep(sleep);
        LOG.info("Awake!");

        voltQueueSQL(getLocal, partition);
        results = voltExecuteSQL();
        assert(results.length == 1);
        LOG.info(this.getTransactionState() + " - LOCAL RESULTS:\n" + results[0]);

        return (results);
    }

}
