package edu.brown.workload.filters;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;

/**
 * @author pavlo
 */
public class DuplicateTraceFilter extends Filter {
    private static final Logger LOG = Logger.getLogger(DuplicateTraceFilter.class);
    
    private final Set<Long> trace_ids = new HashSet<Long>();
    private long skip_ctr = 0;
    
    @Override
    protected String debug() {
        return (this.getClass().getSimpleName() + "[num_ids=" + this.trace_ids.size() + ", skip_ctr=" + this.skip_ctr + "]");
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        long trace_id = element.getId();
        if (this.trace_ids.contains(trace_id)) {
            this.skip_ctr++;
            if (LOG.isTraceEnabled() && this.skip_ctr % 100 == 0) LOG.trace(this.debug());
            if (LOG.isTraceEnabled()) LOG.trace("SKIP: " + element);
            return (FilterResult.SKIP);
        }
        this.trace_ids.add(trace_id);
        return (FilterResult.ALLOW);
    }
    
    @Override
    protected void resetImpl() {
        this.trace_ids.clear();
    }
}
