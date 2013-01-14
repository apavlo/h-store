package org.voltdb.regressionsuites.specexecprocs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;

/**
 * Special single-partition transaction that will always abort
 * @author pavlo
 */
@ProcInfo(
    partitionParam = 0,
    singlePartition = true
)
public class SinglePartitionTester extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(SinglePartitionTester.class);
    
    public static final Map<Integer, Semaphore> LOCK_BEFORE = new HashMap<Integer, Semaphore>();
    public static final Map<Integer, Semaphore> NOTIFY_BEFORE = new HashMap<Integer, Semaphore>();
    public static final Map<Integer, Semaphore> LOCK_AFTER = new HashMap<Integer, Semaphore>();
    
    public final SQLStmt updateSubscriber = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SUBSCRIBER +
        "  SET VLR_LOCATION = ? " +
        " WHERE S_ID = ? "
    );
    
    public VoltTable[] run(long s_id, int marker, long abort) {
        // LOCK BEFORE
        Semaphore lockBefore = LOCK_BEFORE.get(marker);
        if (lockBefore != null) {
            LOG.info(String.format("Got LOCK_BEFORE [marker=%d]", marker));
            Semaphore notifyBefore = NOTIFY_BEFORE.get(marker);
            try {
                if (notifyBefore != null) {
                    LOG.info(String.format("Got NOTIFY_BEFORE [marker=%d]", marker));
                    notifyBefore.release();
                }
                lockBefore.acquire(); 
                lockBefore.release();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                throw new VoltAbortException(ex.getMessage());
            } finally {
                if (notifyBefore != null) notifyBefore.drainPermits();
            }
        }
        
        // Let 'er rip!
        voltQueueSQL(updateSubscriber, marker, s_id);
        final VoltTable results[] = voltExecuteSQL();
        assert(results.length == 1);
        LOG.debug("RESULTS:\n" + results[0]);
        
        // LOCK AFTER
        Semaphore lockAfter = LOCK_AFTER.get(marker);
        if (lockAfter != null) {
            try {
                lockAfter.acquire(); 
                lockAfter.release();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                throw new VoltAbortException(ex.getMessage());
            }
        }

        if (abort == 1) {
            String msg = String.format("Aborting [S_ID=%d / MARKER=%d]", s_id, marker);
            LOG.warn(msg);
            throw new VoltAbortException(msg);
        }
        LOG.info(String.format("Updated %s [S_ID=%d / MARKER=%d]",
                 TM1Constants.TABLENAME_SUBSCRIBER, s_id, marker));
        return (results);
    }
}
