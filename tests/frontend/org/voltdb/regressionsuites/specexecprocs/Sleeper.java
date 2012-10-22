package org.voltdb.regressionsuites.specexecprocs;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;

import edu.brown.utils.ThreadUtil;

@ProcInfo(
    partitionParam = 0,
    singlePartition = true
)
public class Sleeper extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(Sleeper.class);
    
    public final SQLStmt query = new SQLStmt(
        "SELECT COUNT(*) FROM " + TPCCConstants.TABLENAME_ITEM
    );
    
    public VoltTable[] run(int partition, long sleep_before, long sleep_after) {
        VoltTable results[] = null;

        // Sleep first!
        LOG.info(String.format("BEFORE: Sleeping for %.01f seconds", sleep_before / 1000d));
        ThreadUtil.sleep(sleep_before);
        LOG.info("BEFORE: Awake!");

        // Then blast out our query
        voltQueueSQL(query);
        results = voltExecuteSQL();
        assert(results.length == 1);

        // Sleep again!
        LOG.info(String.format("AFTER: Sleeping for %.01f seconds", sleep_before / 1000d));
        ThreadUtil.sleep(sleep_before);
        LOG.info("AFTER: Awake!");
        
        return (results);
    }

}
