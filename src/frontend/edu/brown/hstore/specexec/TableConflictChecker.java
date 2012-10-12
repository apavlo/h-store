package edu.brown.hstore.specexec;

import java.util.BitSet;

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
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final BitSet hasConflicts;
    private final BitSet rwConflicts[];
    private final BitSet wwConflicts[];
    
    public TableConflictChecker(CatalogContext catalogContext) {
        super(catalogContext);
        
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

    @Override
    public boolean ignoreProcedure(Procedure proc) {
        return (this.hasConflicts.get(proc.getId()) == false);
    }

    @Override
    public boolean isConflicting(AbstractTransaction dtxn, LocalTransaction ts, int partitionId) {
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
                    if (dtxn.isTableReadOrWritten(partitionId, ref.getTable())) {
                        return (true);
                    }
                } // FOR
            } // FOR (R-W)
            for (ConflictPair conflict : dtxn_conflicts.getWritewriteconflicts().values()) {
                for (TableRef ref : conflict.getTables().values()) {
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
            if (debug.get())
                LOG.debug(String.format("%s has R-W conflict with %s. Checking read/write sets", ts, dtxn));
            for (ConflictPair conflict : ts_conflicts.getReadwriteconflicts().values()) {
                for (TableRef ref : conflict.getTables().values()) {
                    if (dtxn.isTableWritten(partitionId, ref.getTable())) {
                        return (true);
                    }
                } // FOR
            } // FOR (R-W)
        }
        
        // If we get to this point, then we know that these two txns do not conflict
        return (false);
    }

}
