package edu.brown.workload.filters;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TransactionIdFilter extends Filter {
    private static final Logger LOG = Logger.getLogger(DuplicateTraceFilter.class);
    
    private final Set<Long> txn_ids = new HashSet<Long>();
    private long skip_ctr = 0;
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + "[num_ids=" + this.txn_ids.size() + ", skip_ctr=" + this.skip_ctr + "]");
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        if (element instanceof TransactionTrace) {
            long txn_id = ((TransactionTrace)element).getTransactionId();
            if (this.txn_ids.contains(txn_id)) {
                this.skip_ctr++;
                if (LOG.isTraceEnabled() && this.skip_ctr % 100 == 0) LOG.trace(this.debugImpl());
                if (LOG.isTraceEnabled()) LOG.trace("SKIP: " + element);
                return (FilterResult.SKIP);
            }
            this.txn_ids.add(txn_id);
        }
        return (FilterResult.ALLOW);
    }
    
    public void exclude(Long txn_id) {
        this.txn_ids.add(txn_id);
    }
    
    @Override
    protected void resetImpl() {
        this.txn_ids.clear();
    }
}
