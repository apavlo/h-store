package edu.brown.workload.filters;

import org.voltdb.catalog.CatalogType;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * 
 * @author pavlo
 *
 */
public class ProcedureLimitFilter extends Filter {
    /** How many txn we want to limit. We will halt when txnsAllowed is great than this */
    private final long limit;
    
    /** We can skip elements until txnsExamined is greater than this value */
    private long offset;
    
    /** The total number of txns we've examined */
    private long txnExamined = 0;
    
    /** The total number of txns that we've allowed **/
    private long txnsAllowed = 0;
    
    /** If set to true, then we will increase includedTxn by the txn's weights */
    private final boolean weighted;
    
    /**
     * Constructor
     * @param limit
     * @param offset
     * @param weighted
     * @param next
     */
    public ProcedureLimitFilter(Long limit, Long offset, boolean weighted, Filter next) {
        super(next);
        this.limit = (limit == null ? -1 : limit);
        this.offset = offset;
        this.weighted = weighted;
    }

    public ProcedureLimitFilter(Long limit, boolean weighted) {
        this(limit, 0l, weighted);
    }
    public ProcedureLimitFilter(long limit, boolean weighted) {
        this(limit, 0l, weighted);
    }
    public ProcedureLimitFilter(long limit) {
        this(limit, 0l, false);
    }
    public ProcedureLimitFilter(Long limit, Long offset, boolean weighted) {
        this(limit, offset, weighted, null);
    }
    

    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + ": limit=" + this.limit);
    }
    
    @Override
    protected void resetImpl() {
        this.txnExamined = 0;
        this.txnsAllowed = 0;
    }
        
    @Override
    public synchronized FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            if (this.limit >= 0) {
                if (this.txnsAllowed >= this.limit) {
                    result = FilterResult.HALT;    
                } else if (this.txnExamined < this.offset) {
                    result = FilterResult.SKIP;
                }
            } else if (this.offset > 0 && this.txnExamined < this.offset) {
                result = FilterResult.SKIP;
            }
            if (result == FilterResult.ALLOW) this.txnsAllowed += (this.weighted ? ((TransactionTrace)element).getWeight() : 1);
            this.txnExamined += 1;
        }
        return (result);
    }
}
