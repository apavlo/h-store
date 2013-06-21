package edu.brown.hstore.specexec.checkers;

import org.voltdb.CatalogContext;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.markov.EstimationThresholds;

/**
 * A ConflictChecker is used to determine whether a local single-partition
 * txn can be speculatively executed while we are waiting for a dtxn to finish
 * @author pavlo
 */
public abstract class AbstractConflictChecker {

    protected final CatalogContext catalogContext;
    protected boolean disabled = false;
    
    public AbstractConflictChecker(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
    }
    
    public void setEstimationThresholds(EstimationThresholds t) {
        // Nothing...
    }
    
    public boolean isDisabled() {
        return (this.disabled);
    }
    
    /**
     * Returns true if the given transaction should be ignored from conflict checking
     * @param ts
     * @return
     */
    public abstract boolean shouldIgnoreTransaction(AbstractTransaction ts);
    
    /**
     * Calculate whether to two transaction handles are conflicting.
     * Returns false if the LocalTransaction can be speculatively executed now.
     * The dtxn is the current distributed transaction at our partition, while ts
     * is a single-partition transaction from the work queue that we want to try to
     * speculatively execute right now. 
     * @param ts0
     * @param ts1
     * @param partitionId TODO
     * @return
     */
    public abstract boolean hasConflict(AbstractTransaction ts0, LocalTransaction ts1, int partitionId);
}
