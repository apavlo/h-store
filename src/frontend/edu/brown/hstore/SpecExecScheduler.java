package edu.brown.hstore;

import java.util.BitSet;
import java.util.Iterator;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.TableRef;

import edu.brown.catalog.conflicts.ConflictSetUtil;
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
    private final int partitionId;
    private final Queue<InternalMessage> work_queue;
    private final BitSet hasConflicts;
    private final BitSet rwConflicts[];
    private final BitSet wwConflicts[];
    
    /**
     * Constructor
     * @param partitionId
     * @param work_queue
     * @param catalogContext
     */
    public SpecExecScheduler(int partitionId, Queue<InternalMessage> work_queue, CatalogContext catalogContext) {
        this.partitionId = partitionId;
        this.work_queue = work_queue;
        this.catalogContext = catalogContext;
        
        int size = this.catalogContext.procedures.size()+1;
        this.hasConflicts = new BitSet(size);
        this.rwConflicts = new BitSet[size];
        this.wwConflicts = new BitSet[size];
        
        for (Procedure catalog_proc : this.catalogContext.procedures) {
            if (catalog_proc.getSystemproc() || catalog_proc.getMapreduce()) continue;
           
            // Precompute bitmaps for the conflicts
            int idx = catalog_proc.getId();
            
            this.rwConflicts[idx] = new BitSet(size);
            for (Procedure conflict : ConflictSetUtil.getReadWriteConflicts(catalog_proc)) {
                this.rwConflicts[idx].set(conflict.getId());
                this.hasConflicts.set(idx);
            } // FOR
            
            this.wwConflicts[idx] = new BitSet(size);
            for (Procedure conflict : ConflictSetUtil.getWriteWriteConflicts(catalog_proc)) {
                this.wwConflicts[idx].set(conflict.getId());
                this.hasConflicts.set(idx);
            } // FOR
            
            // XXX: Each procedure will conflict with itself if it's not read-only
            if (catalog_proc.getReadonly() == false) {
                this.rwConflicts[idx].set(idx);
                this.wwConflicts[idx].set(idx);
                this.hasConflicts.set(idx);
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
        Procedure dtxnProc = this.catalogContext.getProcedureById(dtxn.getProcedureId());
        if (dtxnProc == null || this.hasConflicts.get(dtxn.getProcedureId()) == false) {
            if (debug.get())
                LOG.debug(String.format("%s - Ignoring current distributed txn because no conflict information exists [%s]",
                          dtxn, dtxnProc));
            return (null);
        }
        
        // If this is a LocalTransaction and all of the remote partitions that it needs are
        // on the same site, then we won't bother with trying to pick something out
        // because there is going to be very small wait times.
        if (dtxn instanceof LocalTransaction && ((LocalTransaction)dtxn).isPredictAllLocal()) {
            if (debug.get())
                LOG.debug(String.format("%s - Ignoring current distributed txn because all of the partitions that " +
                		  "it is using are on the same HStoreSite [%s]", dtxn, dtxnProc));
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
                    LOG.debug(String.format("%s - Not choosing a txn to speculatively execute because there " +
                    		                "are still WorkFragments in the queue", dtxn));
                return (null);
            }
            // A StartTxnMessage will have a fully initialized LocalTransaction handle
            // that we can examine and execute right away if necessary
            else if (msg instanceof StartTxnMessage) {
                StartTxnMessage txn_msg = (StartTxnMessage)msg;
                LocalTransaction ts = txn_msg.getTransaction();
                if (debug.get())
                    LOG.debug(String.format("Examining whether %s conflicts with current dtxn %s", ts, dtxn));
                if (ts.isPredictSinglePartition() == false) {
                    if (trace.get())
                        LOG.trace(String.format("%s - Skipping %s because it is not single-partitioned", dtxn, ts));
                    continue;
                }

                if (this.isConflicting(dtxn, ts) == false) {
                    next = txn_msg;
                    break;
                }
            }
        } // WHILE
        
        // We found somebody to execute right now!
        // Make sure that we set the speculative flag to true!
        if (next != null) {
            it.remove();
            LocalTransaction next_ts = next.getTransaction();
            next_ts.setSpeculative(true);
            if (debug.get()) 
                LOG.debug(dtxn + " - Found next non-conflicting speculative txn " + next);
        }
        
        return (next);
    }
    
    /**
     * Calculate whether to two transaction handles are conflicting. 
     * The dtxn is the current distributed transaction at our partition, while ts
     * is a single-partition transaction from the work queue that we want to try to
     * speculatively execute right now. 
     * @param dtxn
     * @param ts
     * @return
     */
    protected boolean isConflicting(AbstractTransaction dtxn, LocalTransaction ts) {
        final int dtxn_procId = dtxn.getProcedureId();
        final int ts_procId = ts.getProcedureId();
        
        // DTXN->TS
        boolean dtxn_hasRWConflict = this.rwConflicts[dtxn_procId].get(ts_procId);
        boolean dtxn_hasWWConflict = this.wwConflicts[dtxn_procId].get(ts_procId);
        if (debug.get())
            LOG.debug(String.format("%s -> %s [R-W:%s / W-W:%s]", dtxn, ts, dtxn_hasRWConflict, dtxn_hasWWConflict));
        
        // TS->DTXN
        boolean ts_hasRWConflict = this.rwConflicts[ts_procId].get(dtxn_procId);
        boolean ts_hasWWConflict = this.wwConflicts[ts_procId].get(dtxn_procId);
        if (debug.get())
            LOG.debug(String.format("%s -> %s [R-W:%s / W-W:%s]", ts, dtxn, ts_hasRWConflict, ts_hasWWConflict));
        
        // Sanity Check
        assert(dtxn_hasWWConflict == ts_hasWWConflict);
        
        // If there is no conflict whatsoever, then we want to let this mofo out of the bag right away
        if ((dtxn_hasWWConflict || dtxn_hasRWConflict || ts_hasRWConflict || ts_hasWWConflict) == false) {
            if (debug.get())
                LOG.debug(String.format("No conflicts between %s<->%s", dtxn, ts));
            return (false);
        }

        final Procedure dtxn_proc = this.catalogContext.getProcedureById(dtxn_procId);
        final Procedure ts_proc = ts.getProcedure();
        final ConflictSet dtxn_conflicts = dtxn_proc.getConflicts().get(ts_proc.getName());
        final ConflictSet ts_conflicts = ts_proc.getConflicts().get(dtxn_proc.getName());
        
        // If TS is going to write to something that DTXN will read or write, then 
        // we can let that slide as long as DTXN hasn't read from or written to those tables yet
        if (dtxn_hasRWConflict || dtxn_hasWWConflict) {
            assert(dtxn_conflicts != null) :
                String.format("Unexpected null ConflictSet for %s -> %s",
                              dtxn_proc.getName(), ts_proc.getName());
            for (ConflictPair conflict : dtxn_conflicts.getReadwriteconflicts().values()) {
                for (TableRef ref : conflict.getTables().values()) {
                    if (dtxn.isTableReadOrWritten(this.partitionId, ref.getTable())) {
                        return (true);
                    }
                } // FOR
            } // FOR (R-W)
            for (ConflictPair conflict : dtxn_conflicts.getWritewriteconflicts().values()) {
                for (TableRef ref : conflict.getTables().values()) {
                    if (dtxn.isTableReadOrWritten(this.partitionId, ref.getTable())) {
                        return (true);
                    }
                }
            } // FOR (W-W)
        }
        
        // Similarly, if the TS needs to read from (but not write to) a table that DTXN 
        // writes to, then we can allow TS to execute if DTXN hasn't written anything to 
        // those tables yet
        if (ts_hasRWConflict && ts_hasWWConflict == false) {
            assert(ts_conflicts != null) :
                String.format("Unexpected null ConflictSet for %s -> %s",
                              ts_proc.getName(), dtxn_proc.getName());
            if (debug.get())
                LOG.debug(String.format("%s has R-W conflict with %s. Checking read/write sets", ts, dtxn));
            for (ConflictPair conflict : ts_conflicts.getReadwriteconflicts().values()) {
                for (TableRef ref : conflict.getTables().values()) {
                    if (dtxn.isTableWritten(this.partitionId, ref.getTable())) {
                        return (true);
                    }
                } // FOR
            } // FOR (R-W)
        }
        
        // If we get to this point, then we know that these two txns do not conflict
        return (false);
    }
    
}
