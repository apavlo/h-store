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
public class RemoteIdle extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(RemoteIdle.class);
    
    public final SQLStmt getLocal = new SQLStmt(
        "SELECT S_ID " +
        "  FROM " + TM1Constants.TABLENAME_SUBSCRIBER +
        " WHERE S_ID != ?"
    );
    
    public final SQLStmt getRemote = new SQLStmt(
        "SELECT AVG(S_ID) AS a " +
        "  FROM " + TM1Constants.TABLENAME_CALL_FORWARDING
    );
    
    public VoltTable[] run(int partition, long sleep) {
        VoltTable results[] = null;
        
        // First execute a remote query
        voltQueueSQL(getRemote);
        results = voltExecuteSQL();
        assert(results.length == 1);
        LOG.info("RESULTS:\n" + results[0]);

        // Then sleep for a bit
        LOG.info(String.format("Sleeping for %.01f seconds", sleep / 1000d));
        ThreadUtil.sleep(sleep);
        LOG.info("Awake!");

        voltQueueSQL(getLocal, partition);
        results = voltExecuteSQL();
        assert(results.length == 1);

        return (results);
    }

}
