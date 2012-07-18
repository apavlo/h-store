package edu.brown.hstore;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.internal.StartTxnMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special scheduler that can figure out what the next best single-partition
 * to speculatively execute at a partition based on the current distributed transaction 
 * @author pavlo
 */
public class SpecExecScheduler {
    private static final Logger LOG = Logger.getLogger(SpecExecScheduler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final Database catalog_db;
    private final PartitionExecutor executor;
    private final Queue<InternalMessage> work_queue;
    private final Procedure catalog_procs[];
    private final BitSet procConflicts[];
    
    public SpecExecScheduler(PartitionExecutor executor) {
        this.executor = executor;
        this.work_queue = this.executor.getWorkQueue();
        this.catalog_db = CatalogUtil.getDatabase(executor.getCatalogSite());
        this.procConflicts = new BitSet[this.catalog_db.getProcedures().size()+1];
        this.catalog_procs = new Procedure[catalog_db.getProcedures().size()+1];
        
        for (Procedure catalog_proc : this.catalog_db.getProcedures().values()) {
            if (catalog_proc.getSystemproc() || catalog_proc.getMapreduce()) continue;
            
            int idx = catalog_proc.getId();
            this.catalog_procs[idx] = catalog_proc;
           
            // Precompute bitmaps for the conflicts
            BitSet bs = new BitSet(this.procConflicts.length);
            for (Procedure conflict : CatalogUtil.getConflictProcedures(catalog_proc)) {
                bs.set(conflict.getId());
            } // FOR
            this.procConflicts[idx] = bs;
        } // FOR
    }

    /**
     * Find the next non-conflicting txn that we can speculatively execute.
     * Note that if we find one, it will be immediately removed from the queue
     * and returned. If you do this and then find out for some reason that you
     * can't execute the StartTxnMessage that is returned, you must be sure
     * to requeue it back.
     * @param dtxn The current distributed txn at this partition.
     * @return
     */
    public StartTxnMessage next(AbstractTransaction dtxn) {
        Procedure catalog_proc = this.catalog_procs[dtxn.getProcedureId()];
        BitSet bs = this.procConflicts[dtxn.getProcedureId()];
        if (catalog_proc == null || bs == null) {
            if (debug.get())
                LOG.debug("SKIP - Ignoring current distributed txn because no conflict information exists");
            return (null);
        }
        
        // Now peek in the queue looking for single-partition txns that do not
        // conflict with the current dtxn
        StartTxnMessage next = null;
        Iterator<InternalMessage> it = this.work_queue.iterator();
        while (it.hasNext()) {
            InternalMessage msg = it.next();

            // Any WorkFragmentMessage has to be for our current dtxn,
            // so we want to never speculative execute stuff because we will
            // always want to immediately execute that
            if (msg instanceof WorkFragmentMessage) {
                if (debug.get())
                    LOG.debug(String.format("SKIP %s - No conflict mapping exists",
                                            catalog_proc.getName()));
                return (null);
            }
            // A StartTxnMessage will have a fully initialized LocalTransaction handle
            // that we can examine and execute right away if necessary
            else if (msg instanceof StartTxnMessage) {
                StartTxnMessage txn_msg = (StartTxnMessage)msg;
                LocalTransaction ts = txn_msg.getTransaction();
                int procId = ts.getProcedureId();
                
                // Only release it if it's single-partitioned and does not conflict
                // with our current dtxn
                if (ts.isPredictSinglePartition() && bs.get(procId) == false) {
                    next = txn_msg;
                    // Make sure that we remove it from our queue
                    it.remove();
                    break;
                }
            }
        } // WHILE
        if (debug.get() && next != null) {
            LOG.debug(String.format("NEXT %s - Found next non-conflicting speculative txn %s",
                                    catalog_proc.getName(), next));
        }
        
        return (next);
    }
    
}
