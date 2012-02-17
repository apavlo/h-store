package edu.brown.workload.filters;

import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * 
 * @author pavlo
 *
 */
public class QueryLimitFilter extends Filter {
    private final long limit;
    private long count = 0;

    public QueryLimitFilter(Long limit) {
        this(limit, null);
    }
    
    public QueryLimitFilter(Long limit, Filter next) {
        super(next);
        this.limit = (limit == null ? -1 : limit);
    }
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + ": limit=" + this.limit);
    }
    
    @Override
    protected void resetImpl() {
        this.count = 0;
    }
    
    @Override
    public FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        //
        // Keep the count up until we reach our limit. Then we will return
        // HALT for the next transaction that we get
        //
        if (element instanceof TransactionTrace) {
            if (this.limit < 0) return (FilterResult.ALLOW);
            return (this.count < this.limit ? FilterResult.ALLOW : FilterResult.HALT);
        } else if (element instanceof QueryTrace) {
            this.count++;
            return (FilterResult.ALLOW);
        }
        return (FilterResult.ALLOW);
    }
}
