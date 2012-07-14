package edu.brown.hstore;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.internal.InitializeTxnMessage;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

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
        this.procConflicts = new BitSet[this.catalog_db.getProcedures().size()];
        this.catalog_procs = new Procedure[catalog_db.getProcedures().size()+1];
        
        for (Procedure catalog_proc : this.catalog_db.getProcedures().values()) {
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
     * Find the next non-conflicting txn that we can speculatively execute
     * @return
     */
    public InitializeTxnMessage next() {
        AbstractTransaction dtxn = this.executor.getCurrentDtxn();
        int proc_id = dtxn.getProcedureId();
        Procedure catalog_proc = this.catalog_procs[proc_id];
        
        BitSet bs = this.procConflicts[proc_id];
        if (bs == null) {
            if (debug.get())
                LOG.debug(String.format("SKIP %s - No conflict mapping exists",
                                        catalog_proc.getName()));
            return (null);
        }
        
        // Now peek in the queue looking for single-partition txns that do not
        // conflict with the current dtxn
        InitializeTxnMessage next = null;
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
            else if (msg instanceof InitializeTxnMessage) {
                InitializeTxnMessage txn_msg = (InitializeTxnMessage)msg;
                if (bs.get(txn_msg.getProcedure().getId()) == false) {
                    next = txn_msg;
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
