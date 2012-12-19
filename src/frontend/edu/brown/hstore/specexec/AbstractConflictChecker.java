package edu.brown.hstore.specexec;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.interfaces.Loggable;
import edu.brown.markov.EstimationThresholds;

/**
 * A ConflictChecker is used to determine whether a local single-partition
 * txn can be speculatively executed while we are waiting for a dtxn to finish
 * @author pavlo
 */
public abstract class AbstractConflictChecker implements Loggable {

    protected final CatalogContext catalogContext;
    
    public AbstractConflictChecker(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
    }
    
    public void setEstimationThresholds(EstimationThresholds t) {
        // Nothing...
    }
    
    /**
     * Returns true if the given Procedure should be ignored from conflict checking
     * @param proc
     * @return
     */
    public abstract boolean shouldIgnoreProcedure(Procedure proc);
    
    /**
     * Calculate whether to two transaction handles are conflicting.
     * Returns true if the LocalTransaction can be speculatively executed now.
     * The dtxn is the current distributed transaction at our partition, while ts
     * is a single-partition transaction from the work queue that we want to try to
     * speculatively execute right now. 
     * @param dtxn
     * @param ts
     * @param partitionId TODO
     * @return
     */
    public abstract boolean canExecute(AbstractTransaction dtxn, LocalTransaction ts, int partitionId);
}
