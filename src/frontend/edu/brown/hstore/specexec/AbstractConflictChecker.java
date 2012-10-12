package edu.brown.hstore.specexec;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;

public abstract class AbstractConflictChecker {

    protected final CatalogContext catalogContext;
    
    public AbstractConflictChecker(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
    }
    
    /**
     * Returns true if the given Procedure should be ignored from conflict checking
     * @param proc
     * @return
     */
    public abstract boolean ignoreProcedure(Procedure proc);
    
    /**
     * Calculate whether to two transaction handles are conflicting. 
     * The dtxn is the current distributed transaction at our partition, while ts
     * is a single-partition transaction from the work queue that we want to try to
     * speculatively execute right now. 
     * @param dtxn
     * @param ts
     * @param partitionId TODO
     * @return
     */
    public abstract boolean isConflicting(AbstractTransaction dtxn, LocalTransaction ts, int partitionId);
}
