package org.voltdb.regressionsuites.specexecprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;

/**
 * Simple single-partition transaction that checks to see
 * whether the VLR_LOCATION equals the marker value. If it does,
 * then it will abort
 * @author pavlo
 */
@ProcInfo(
    partitionParam = 0,
    singlePartition = true
)
public class CheckSubscriber extends VoltProcedure {
    
    public final SQLStmt getSubscriber = new SQLStmt(
        "SELECT S_ID, VLR_LOCATION " +
        "  FROM " + TM1Constants.TABLENAME_SUBSCRIBER +
        " WHERE S_ID = ? "
    );
    
    public VoltTable[] run(long s_id, int marker) {
        voltQueueSQL(getSubscriber, s_id);
        final VoltTable results[] = voltExecuteSQL();
        assert(results.length == 1);
        
        boolean adv = results[0].advanceRow();
        assert(adv);
        assert(s_id == results[0].getLong(0));
        if (marker == results[0].getLong(1)) {
            String msg = String.format("Aborting transaction [S_ID=%d / MARKER=%d]", s_id, marker);
            throw new VoltAbortException(msg);            
        }
        return (results);
    }

}
