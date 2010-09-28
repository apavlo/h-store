package edu.brown.workload.filters;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.AbstractWorkload;

/**
 * @author pavlo
 */
public class DuplicateTraceFilter extends AbstractWorkload.Filter {
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
            if (LOG.isDebugEnabled() && this.skip_ctr % 100 == 0) LOG.debug(this.debug());
            if (LOG.isDebugEnabled()) LOG.debug("SKIP: " + element);
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
