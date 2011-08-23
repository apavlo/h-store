package edu.brown.workload.filters;

import org.voltdb.catalog.*;
import edu.brown.workload.*;

/**
 * 
 * @author pavlo
 *
 */
public class ProcedureLimitFilter extends Filter {
    private final long limit;
    private long offset;
    private long count = 0;
    private long includedCount = 0;
    private boolean weighted = false;

    public ProcedureLimitFilter(Long limit, boolean weighted) {
        this(limit, 0l, weighted);
    }
    
    public ProcedureLimitFilter(long limit, boolean weighted) {
        this(limit, 0l, weighted);
    }
    
    public ProcedureLimitFilter(long limit) {
        this(limit, 0l, false);
    }
    
    public ProcedureLimitFilter(Long limit, Long offset, boolean count_weights) {
        this(limit, offset, count_weights, null);
    }
    
    public ProcedureLimitFilter(Long limit, Long offset, boolean count_weights, Filter next) {
        super(next);
        this.limit = (limit == null ? -1 : limit);
        this.offset = offset;
        this.weighted = count_weights;
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
        this.includedCount = 0;
    }
        
    @Override
    public synchronized FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            if (this.limit >= 0) {
                if (this.includedCount >= this.limit) {
                    result = FilterResult.HALT;    
                } else if (this.count < this.offset) {
                    result = FilterResult.SKIP;
                }
            } else if (this.offset > 0 && this.count < this.offset) {
                result = FilterResult.SKIP;
            }
            if (result == FilterResult.ALLOW) this.includedCount += (this.weighted ? ((TransactionTrace)element).getWeight() : 1);
            this.count += 1;
        }
        return (result);
    }
}
