package edu.brown.workload.filters;

import java.util.BitSet;
import java.util.Collection;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;

import edu.brown.hstore.HStoreConstants;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

public class BasePartitionTxnFilter extends Filter {
    
    private final PartitionEstimator p_estimator;
    private final CatalogContext catalogContext;
    private final BitSet base_partitions;
    
    public BasePartitionTxnFilter(PartitionEstimator p_estimator, int...base_partitions) {
        super();
        this.p_estimator = p_estimator;
        this.catalogContext = p_estimator.getCatalogContext();
        
        this.base_partitions = new BitSet(this.catalogContext.numberOfPartitions);
        for (int p : base_partitions) {
            this.base_partitions.set(p);
        }
    }
    
    public void addPartitions(Collection<Integer> partitions) {
        for (Integer p : partitions) {
            this.base_partitions.set(p.intValue());
        }
    }
    
    @Override
    protected void resetImpl() {
        // Ignore...
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        if (element instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)element;
            int partition = HStoreConstants.NULL_PARTITION_ID;
            try {
                partition = this.p_estimator.getBasePartition(xact);
            } catch (Exception ex) {
                ex.printStackTrace();
                assert(false);
            }
            assert(partition >= 0);
            return (this.base_partitions.get(partition) ? FilterResult.ALLOW : FilterResult.SKIP);
        }
        return FilterResult.ALLOW;
    }
    
    @Override
    public String debugImpl() {
        return null;
    }

}
