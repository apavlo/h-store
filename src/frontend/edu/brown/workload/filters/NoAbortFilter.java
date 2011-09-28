package edu.brown.workload.filters;

import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;

/**
 * @author pavlo
 */
public class NoAbortFilter extends Filter {
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName());
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        return (element.isAborted() ? FilterResult.SKIP : FilterResult.ALLOW);
    }
    
    @Override
    protected void resetImpl() {
        // Nothing...
    }
}
