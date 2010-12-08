package edu.brown.markov.features;

import org.voltdb.catalog.Procedure;

import edu.brown.markov.FeatureSet;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

/**
 * Generate features for the length of the array parameters
 * @author pavlo
 */
public class BasePartitionFeature extends AbstractFeature {
    
    public BasePartitionFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, "basepartition");
    }

    @Override
    public void calculate(FeatureSet fset, TransactionTrace txn_trace) throws Exception {
        Integer base_partition = this.p_estimator.getPartition(this.catalog_proc, txn_trace.getParams());
        fset.addFeature(txn_trace, this.getFeatureKey(), base_partition);
    }

}
