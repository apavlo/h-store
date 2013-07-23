package org.voltdb.regressionsuites.specexecprocs;

import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.procedures.SendPayment;
import edu.brown.profilers.ProfileMeasurement;

/**
 * Special version of SmallBank's SendPayment that is blockable
 * @author pavlo
 */
@ProcInfo(
    partitionParam = 0,
    singlePartition = false
)
public class BlockingSendPayment extends SendPayment {
    private static final Logger LOG = Logger.getLogger(BlockingSendPayment.class);
    
    public final Semaphore LOCK_BEFORE = new Semaphore(0);
    public final Semaphore NOTIFY_BEFORE = new Semaphore(0);
    public final Semaphore LOCK_AFTER = new Semaphore(0);
    public final Semaphore NOTIFY_AFTER = new Semaphore(0);
    
    public VoltTable[] run(long sendAcct, long destAcct, double amount) {
        try {
            return _run(sendAcct, destAcct, amount);
        } finally {
            LOCK_BEFORE.drainPermits();
            LOCK_AFTER.drainPermits();
            NOTIFY_BEFORE.drainPermits();
            NOTIFY_AFTER.drainPermits();
        }
    }
        
    private VoltTable[] _run(long sendAcct, long destAcct, double amount) {
        // If this is the second time that we are running (i.e., this 
        // txn has been restarted before), then we don't want to block on
        // any of the locks.
        boolean takeLocks = (this.getTransactionState().getRestartCounter() == 0);
        
        // -------------------- LOCK BEFORE QUERY -------------------- 
        ProfileMeasurement pm_before = new ProfileMeasurement("BEFORE");
        LOG.info(this.getTransactionState() + " - Blocking until LOCK_BEFORE is released");
        pm_before.start();
        try {
            // Notify others before we lock
            NOTIFY_BEFORE.release();
            if (takeLocks) LOCK_BEFORE.acquire();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            throw new VoltAbortException(ex.getMessage());
        } finally {
            NOTIFY_BEFORE.drainPermits();
            pm_before.stop();
            LOG.info("AWAKE - " + pm_before.debug());
        }
        
        // BATCH #1
        voltQueueSQL(GetAccount, sendAcct);
        voltQueueSQL(GetAccount, destAcct);
        final VoltTable acctResults[] = voltExecuteSQL();
        assert(acctResults != null);
        
        // BATCH #2
        voltQueueSQL(GetCheckingBalance, sendAcct);
        final VoltTable balResults[] = voltExecuteSQL();
        assert(balResults != null);
        
        // BATCH #3
        voltQueueSQL(UpdateCheckingBalance, amount*-1d, sendAcct);
        voltQueueSQL(UpdateCheckingBalance, amount, destAcct);
        final VoltTable updateResults[] = voltExecuteSQL();
        
        // -------------------- LOCK AFTER QUERY --------------------
        ProfileMeasurement pm_after = new ProfileMeasurement("AFTER");
        LOG.info(this.getTransactionState() + " - Blocking until LOCK_AFTER is released");
        pm_after.start();
        try {
            // Notify others before we lock
            NOTIFY_AFTER.release();
            if (takeLocks) LOCK_AFTER.acquire();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            throw new VoltAbortException(ex.getMessage());
        } finally {
            NOTIFY_AFTER.drainPermits();
            pm_after.stop();
            LOG.info("AWAKE - " + pm_after.debug());
        }
        
        return (updateResults);
    }
}
