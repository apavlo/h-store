package edu.brown.hstore;

import java.util.BitSet;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.txns.AbstractTransaction;

public class SpecExecScheduler {

    private final Database catalog_db;
    private final PartitionExecutor executor;
    private final BitSet procConflicts[];
    
    public SpecExecScheduler(PartitionExecutor executor) {
        this.executor = executor;
        this.catalog_db = CatalogUtil.getDatabase(executor.getCatalogSite());
        this.procConflicts = new BitSet[this.catalog_db.getProcedures().size()];
        
        // Precompute bitmaps for the conflicts
        for (Procedure catalog_proc : this.catalog_db.getProcedures().values()) {
            int idx = catalog_proc.getId();
            BitSet bs = new BitSet(this.procConflicts.length);
            for (Procedure conflict : CatalogUtil.getConflictProcedures(catalog_proc)) {
                bs.set(conflict.getId());
            } // FOR
            this.procConflicts[idx] = bs;
        } // FOR
    }
    
    /**
     * 
     * @return
     */
    public InternalMessage next() {
        AbstractTransaction dtxn = this.executor.getCurrentDtxn();
        
        
        return (null);
    }
    
}
