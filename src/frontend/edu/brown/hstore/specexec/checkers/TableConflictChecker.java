package edu.brown.hstore.specexec.checkers;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.TableRef;

import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class TableConflictChecker extends AbstractConflictChecker {
    private static final Logger LOG = Logger.getLogger(TableConflictChecker.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final boolean hasConflicts[];
    private final boolean rwConflicts[][];
    private final boolean wwConflicts[][];
    
    public TableConflictChecker(CatalogContext catalogContext) {
        super(catalogContext);
        
        int size = this.catalogContext.procedures.size()+1;
        this.hasConflicts = new boolean[size];
        this.rwConflicts = new boolean[size][size];
        this.wwConflicts = new boolean[size][size];
        
        Arrays.fill(this.hasConflicts, false);
        for (Procedure catalog_proc : this.catalogContext.procedures) {
            if (catalog_proc.getSystemproc() || catalog_proc.getMapreduce()) continue;
           
            // Precompute bitmaps for the conflicts
            int idx = catalog_proc.getId();
            
            this.rwConflicts[idx] = new boolean[size];
            for (Procedure conflict : ConflictSetUtil.getReadWriteConflicts(catalog_proc)) {
                this.rwConflicts[idx][conflict.getId()] = true;
                this.hasConflicts[idx] = true;
            } // FOR
            
            this.wwConflicts[idx] = new boolean[size];
            for (Procedure conflict : ConflictSetUtil.getWriteWriteConflicts(catalog_proc)) {
                this.wwConflicts[idx][conflict.getId()] = true;
                this.hasConflicts[idx] = true;
            } // FOR
            
            // XXX: Each procedure will conflict with itself if it's not read-only
            if (catalog_proc.getReadonly() == false) {
                this.rwConflicts[idx][idx] = true;
                this.wwConflicts[idx][idx] = true;
                this.hasConflicts[idx] = true;
            }
        } // FOR
    }

    @Override
    public boolean shouldIgnoreTransaction(AbstractTransaction ts) {
        return (this.hasConflicts[ts.getProcedure().getId()] == false);
    }

    @Override
    public boolean hasConflictBefore(AbstractTransaction dtxn, LocalTransaction candidate, int partitionId) {
        assert(dtxn.isInitialized()) :
            String.format("Uninitialized distributed transaction handle [%s]", dtxn);
        assert(candidate.isInitialized()) :
            String.format("Uninitialized speculative transaction handle [%s]", candidate);
        final Procedure dtxn_proc = dtxn.getProcedure();
        final Procedure ts_proc = candidate.getProcedure();
        final int dtxn_procId = dtxn_proc.getId();
        final int ts_procId = ts_proc.getId();
        
        // DTXN->TS
        boolean dtxn_hasRWConflict = this.rwConflicts[dtxn_procId][ts_procId];
        boolean dtxn_hasWWConflict = this.wwConflicts[dtxn_procId][ts_procId];
        if (debug.val)
            LOG.debug(String.format("%s -> %s [R-W:%s / W-W:%s]",
                      dtxn, candidate, dtxn_hasRWConflict, dtxn_hasWWConflict));
        
        // TS->DTXN
        boolean ts_hasRWConflict = this.rwConflicts[ts_procId][dtxn_procId];
        boolean ts_hasWWConflict = this.wwConflicts[ts_procId][dtxn_procId];
        if (debug.val)
            LOG.debug(String.format("%s -> %s [R-W:%s / W-W:%s]",
                      candidate, dtxn, ts_hasRWConflict, ts_hasWWConflict));
        
        // Sanity Check
        assert(dtxn_hasWWConflict == ts_hasWWConflict);
        
        // If there is no conflict whatsoever, then we want to let this mofo out of the bag right away
        if ((dtxn_hasWWConflict || dtxn_hasRWConflict || ts_hasRWConflict || ts_hasWWConflict) == false) {
            if (debug.val)
                LOG.debug(String.format("No conflicts between %s<->%s",
                          dtxn, candidate));
            return (false);
        }

        final ConflictSet dtxn_conflicts = dtxn_proc.getConflicts().get(ts_proc.getName());
        final ConflictSet ts_conflicts = ts_proc.getConflicts().get(dtxn_proc.getName());
        
        // If TS is going to write to something that DTXN will read or write, then 
        // we can let that slide as long as DTXN hasn't read from or written to those tables yet
        if (dtxn_hasRWConflict || dtxn_hasWWConflict) {
            assert(dtxn_conflicts != null) :
                String.format("Unexpected null DTXN ConflictSet for %s -> %s",
                              dtxn_proc.getName(), ts_proc.getName());
            
            // READ-WRITE
            if (debug.val && dtxn_conflicts.getReadwriteconflicts().isEmpty() == false)
                LOG.debug(String.format("Examining %d R-W Conflicts from %s -> %s",
                          dtxn_conflicts.getReadwriteconflicts().size(), dtxn_proc.getName(), ts_proc.getName()));
            for (ConflictPair conflict : dtxn_conflicts.getReadwriteconflicts().values()) {
                assert(conflict != null) :
                    String.format("Unexpected null DTXN R/W ConflictSet tables for %s [candidate=%s]",
                                  dtxn_proc.getName(), ts_proc.getName());
                for (TableRef ref : conflict.getTables().values()) {
                    assert(ref.getTable() != null) :
                        String.format("Unexpected null table reference %s [%s -> %s]",
                                      ref.fullName(), dtxn_proc.getName(), ts_proc.getName());
                    if (dtxn.isTableReadOrWritten(partitionId, ref.getTable())) {
                        return (true);
                    }
                } // FOR
            } // FOR (R-W)
            
            // WRITE-WRITE
            if (debug.val && dtxn_conflicts.getWritewriteconflicts().isEmpty() == false)
                LOG.debug(String.format("Examining %d W-W Conflicts from %s -> %s",
                          dtxn_conflicts.getWritewriteconflicts().size(), dtxn_proc.getName(), ts_proc.getName()));
            for (ConflictPair conflict : dtxn_conflicts.getWritewriteconflicts().values()) {
                assert(conflict != null) : 
                    String.format("Unexpected null ConflictSet for %s [candidate=%s]",
                                  dtxn_proc.getName(), ts_proc.getName());
                for (TableRef ref : conflict.getTables().values()) {
                    assert(ref.getTable() != null) :
                        String.format("Unexpected null table reference %s [%s -> %s]",
                                      ref.fullName(), dtxn_proc.getName(), ts_proc.getName());
                    if (dtxn.isTableReadOrWritten(partitionId, ref.getTable())) {
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
            if (debug.val) LOG.debug(String.format("%s has R-W conflict with %s. Checking read/write sets", candidate, dtxn));
            for (ConflictPair conflict : ts_conflicts.getReadwriteconflicts().values()) {
                assert(conflict != null) : 
                    String.format("Unexpected null ConflictSet for %s [candidate=%s]",
                                  dtxn_proc.getName(), ts_proc.getName());
                for (TableRef ref : conflict.getTables().values()) {
                    assert(ref.getTable() != null) :
                        String.format("Unexpected null table reference %s [%s -> %s]",
                                      ref.fullName(), dtxn_proc.getName(), ts_proc.getName());
                    if (dtxn.isTableWritten(partitionId, ref.getTable())) {
                        return (true);
                    }
                } // FOR
            } // FOR (R-W)
        }
        
        // If we get to this point, then we know that these two txns do not conflict
        return (false);
    }
    
    @Override
    public boolean hasConflictAfter(AbstractTransaction ts0, LocalTransaction ts1, int partitionId) {
        return (false);
    }
}
