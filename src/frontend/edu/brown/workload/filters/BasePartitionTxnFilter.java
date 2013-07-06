package edu.brown.workload.filters;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;

import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

public class BasePartitionTxnFilter extends Filter {
    private static final Logger LOG = Logger.getLogger(BasePartitionTxnFilter.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    
    private final PartitionEstimator p_estimator;
    private final PartitionSet allowedPartitions = new PartitionSet();
    
    public BasePartitionTxnFilter(PartitionEstimator p_estimator, int...partitions) {
        super();
        this.p_estimator = p_estimator;
        this.allowedPartitions.addAll(partitions);
    }
    
    public void addPartitions(Collection<Integer> partitions) {
        this.allowedPartitions.addAll(partitions);
    }
    
    @Override
    protected void resetImpl() {
        // Ignore...
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)element;
            int basePartition = HStoreConstants.NULL_PARTITION_ID;
            try {
                basePartition = this.p_estimator.getBasePartition(xact);
            } catch (Exception ex) {
                ex.printStackTrace();
                assert(false);
            }
            assert(basePartition >= 0);
            result = (this.allowedPartitions.contains(basePartition) ? FilterResult.ALLOW : FilterResult.SKIP);
            if (debug.val)
                LOG.debug(String.format("%s :: basePartition=%d / allowed=%s ==> %s",
                          xact, basePartition, this.allowedPartitions, result));
        }
        return (result);
    }
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + ": allowed=" + this.allowedPartitions);
    }

}
