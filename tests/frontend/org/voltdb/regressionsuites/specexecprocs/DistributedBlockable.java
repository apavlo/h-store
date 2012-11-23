package org.voltdb.regressionsuites.specexecprocs;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.profilers.ProfileMeasurement;

/**
 * Special distributed transaction that can be blocked programatically
 * @author pavlo
 */
@ProcInfo(
    partitionParam = 0,
    singlePartition = false
)
public class DistributedBlockable extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(DistributedBlockable.class);
    
    public static final Semaphore LOCK_BEFORE = new Semaphore(0);
    public static final Semaphore NOTIFY_BEFORE = new Semaphore(0);
    public static final Semaphore LOCK_AFTER = new Semaphore(0);
    
    /**
     * Changing this flag to true will cause the txn to abort
     */
    public static final AtomicBoolean SHOULD_ABORT = new AtomicBoolean(false);
    
    public final SQLStmt updateAll = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SUBSCRIBER +
        "  SET MSC_LOCATION = MSC_LOCATION + 1 "
    );
    
    public VoltTable[] run(int partition) {
        // -------------------- LOCK BEFORE QUERY -------------------- 
        ProfileMeasurement pm_before = new ProfileMeasurement("BEFORE");
        LOG.info("Blocking until LOCK_BEFORE is released");
        pm_before.start();
        try {
            // Notify others before we lock
            NOTIFY_BEFORE.release();
            LOCK_BEFORE.acquire();
            LOCK_BEFORE.release();
        } catch (InterruptedException ex) {
            throw new VoltAbortException(ex.getMessage());
        } finally {
            NOTIFY_BEFORE.drainPermits();
            pm_before.stop();
            LOG.info("AWAKE - " + pm_before.debug(true));
        }
        
        // -------------------- DISTRIBUTED QUERY --------------------
        voltQueueSQL(updateAll);
        final VoltTable results[] = voltExecuteSQL();
        assert(results.length == 1);
        LOG.info("RESULTS:\n" + results[0]);

        // -------------------- LOCK AFTER QUERY --------------------
        ProfileMeasurement pm_after = new ProfileMeasurement("AFTER");
        LOG.info("Blocking until LOCK_AFTER is released");
        try {
            pm_after.start();
            LOCK_AFTER.acquire();
            LOCK_AFTER.release();
        } catch (InterruptedException ex) {
            throw new VoltAbortException(ex.getMessage());
        } finally {
            pm_after.stop();
            LOG.info("AWAKE - " + pm_after.debug(true));
        }
        
        if (SHOULD_ABORT.get()) {
            String msg = "Txn aborted because somebody asked us to! Deal with it!";
            throw new VoltAbortException(msg);
        }
        

        return (results);
    }

}
