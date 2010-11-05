package edu.brown.workload.filters;

import org.voltdb.catalog.*;
import edu.brown.workload.*;

/**
 * 
 * @author pavlo
 *
 */
public class ProcedureLimitFilter extends Workload.Filter {
    private final long limit;
    private long offset;
    private long count = 0;
    private long included_count = 0;

    public ProcedureLimitFilter(Long limit) {
        this(limit, 0l);
    }
    
    public ProcedureLimitFilter(long limit) {
        this(limit, 0l);
    }
    
    public ProcedureLimitFilter(Long limit, Long offset) {
        this(limit, offset, null);
    }
    
    public ProcedureLimitFilter(Long limit, Long offset, Workload.Filter next) {
        super(next);
        this.limit = (limit == null ? -1 : limit);
        this.offset = offset;
    }
    
    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    @Override
    protected String debug() {
        return (this.getClass().getSimpleName() + ": limit=" + this.limit);
    }
    
    @Override
    protected void resetImpl() {
        this.count = 0;
        this.included_count = 0;
    }
        
    @Override
    public FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            if (this.limit >= 0) {
                if (this.included_count >= this.limit) {
                    result = FilterResult.HALT;    
                } else if (this.count < this.offset) {
                    result = FilterResult.SKIP;
                }
            } else if (this.offset > 0 && this.count < this.offset) {
                result = FilterResult.SKIP;
            }
            if (result == FilterResult.ALLOW) this.included_count++;
            this.count++;
        }
        return (result);
    }
}
