package org.voltdb.regressionsuites.specexecprocs;

import org.apache.log4j.Logger;
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
    private static final Logger LOG = Logger.getLogger(CheckSubscriber.class);
    
    public final SQLStmt getSubscriber = new SQLStmt(
        "SELECT S_ID, VLR_LOCATION " +
        "  FROM " + TM1Constants.TABLENAME_SUBSCRIBER +
        " WHERE S_ID = ? "
    );
    
    public VoltTable[] run(long s_id, int marker, int should_be_equal) {
        voltQueueSQL(getSubscriber, s_id);
        final VoltTable results[] = voltExecuteSQL();
        assert(results.length == 1);
        LOG.debug("RESULTS:\n" + results[0]);
        
        boolean adv = results[0].advanceRow();
        assert(adv);
        assert(s_id == results[0].getLong(0));
        long current = results[0].getLong(1);

        // If should_be_equal is true, then we will abort if the
        // expected marker *is not* the same as the result
        // If should_be_equal is false, then we will abort if the 
        // expected marker *is* the same as the result
        boolean is_equal = (marker == current);
        if (should_be_equal == 1 && is_equal == false) {
            String msg = String.format("Aborting transaction - %d != %d [S_ID=%d]", s_id, current, marker);
            throw new VoltAbortException(msg);            
        }
        else if (should_be_equal == 0 && is_equal == true) {
            String msg = String.format("Aborting transaction - %d == %d [S_ID=%d]", s_id, current, marker);
            throw new VoltAbortException(msg);            
        }
        return (results);
    }

}
