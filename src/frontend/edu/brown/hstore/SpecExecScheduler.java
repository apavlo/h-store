package edu.brown.hstore;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
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
    
    private final CatalogContext catalogContext;
    private final PartitionExecutor executor;
    private final int partitionId;
    private final Queue<InternalMessage> work_queue;
    private final BitSet rwConflicts[];
    private final BitSet wwConflicts[];
    
    public SpecExecScheduler(PartitionExecutor executor) {
        this.executor = executor;
        this.partitionId = this.executor.getPartitionId();
        this.work_queue = this.executor.getWorkQueue();
        this.catalogContext = this.executor.getCatalogContext();
        
        int size = this.catalogContext.procedures.size()+1;
        this.rwConflicts = new BitSet[size];
        this.wwConflicts = new BitSet[size];
        
        for (Procedure catalog_proc : this.catalogContext.procedures) {
            if (catalog_proc.getSystemproc() || catalog_proc.getMapreduce()) continue;
            
            int idx = catalog_proc.getId();
           
            // Precompute bitmaps for the conflicts
            this.rwConflicts[idx] = new BitSet(size);
            for (Procedure conflict : CatalogUtil.getReadWriteConflicts(catalog_proc)) {
                this.rwConflicts[idx].set(conflict.getId());
            } // FOR
            this.wwConflicts[idx] = new BitSet(size);
            for (Procedure conflict : CatalogUtil.getWriteWriteConflicts(catalog_proc)) {
                this.wwConflicts[idx].set(conflict.getId());
            } // FOR
            
            // XXX: Each procedure will conflict with itself if it's not read-only
            if (catalog_proc.getReadonly() == false) {
                this.rwConflicts[idx].set(idx);
                this.wwConflicts[idx].set(idx);
            }
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
        Procedure catalog_proc = this.catalogContext.getProcedureById(dtxn.getProcedureId());
        BitSet rwCon = this.rwConflicts[dtxn.getProcedureId()];
        BitSet wwCon = this.wwConflicts[dtxn.getProcedureId()];
        if (catalog_proc == null || rwCon == null || wwCon == null) {
            if (trace.get())
                LOG.trace("SKIP - Ignoring current distributed txn because no conflict information exists");
            return (null);
        }
        
        boolean readOnly = dtxn.isExecReadOnly(this.partitionId);
        
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
                    LOG.debug(String.format("%s - Not choosing a txn to speculatively execute because there " +
                    		                "are still WorkFragments in the queue", dtxn));
                return (null);
            }
            // A StartTxnMessage will have a fully initialized LocalTransaction handle
            // that we can examine and execute right away if necessary
            else if (msg instanceof StartTxnMessage) {
                StartTxnMessage txn_msg = (StartTxnMessage)msg;
                LocalTransaction ts = txn_msg.getTransaction();
                if (ts.isPredictSinglePartition() == false) continue;

                int procId = ts.getProcedureId();
                boolean hasRWConflict = rwCon.get(procId);
                boolean hasWWConflict = wwCon.get(procId);
                
                // If there is no conflict whatsoever, then we want to let
                // this mofo out of the bag right away
                if (hasWWConflict == false && hasRWConflict == false) {
                    next = txn_msg;
                    ts.setSpeculative(true);
                    break;
                }
                
                // If there is no write-write conflict and a read-write conflict on
                // a table that hasn't been written to yet by the current dtxn,
                // then we can let this mofo drop
                if (hasWWConflict == false) {
                    // TODO: We need to know what the tables actually are that
                    // make up the conflict information so that we can check whether
                    // the current dtxn has written to it yet.
                }
                
                
                
                // Only release it if it's single-partitioned and does not conflict
                // with our current dtxn
                // TODO: We need to be more clever about what we allow depending on
                //       whether it's a read-write conflict or a write-write conflict
//                if (ts.isPredictSinglePartition() && rwCon.get(procId) == false) {
//                    next = txn_msg;
//                    // Make sure that we remove it from our queue
//                    it.remove();
//                    break;
//                }
            }
        } // WHILE
        if (debug.get() && next != null) {
            LOG.debug(dtxn + " - Found next non-conflicting speculative txn " + next);
        }
        
        return (next);
    }
    
}
