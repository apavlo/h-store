package edu.brown.hstore.estimators.fixed;

import org.voltdb.catalog.Procedure;

import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

/**
 * Voter Benchmark Fixed Estimator
 * @author pavlo
 */
public class FixedVoterEstimator extends AbstractFixedEstimator {

    public FixedVoterEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        String procName = catalog_proc.getName();
        FixedEstimatorState ret = new FixedEstimatorState(this.catalogContext, txn_id, base_partition);
        PartitionSet partitions = null;
        PartitionSet readonly = EMPTY_PARTITION_SET;
        if (procName.equalsIgnoreCase("vote")) {
            partitions = this.catalogContext.getPartitionSetSingleton(base_partition);
        } else {
            partitions = this.catalogContext.getAllPartitionIds();
        }
        ret.createInitialEstimate(partitions, readonly, EMPTY_PARTITION_SET);
        return ((T)ret);
    }
}
