package edu.brown.markov.features;

import org.voltdb.catalog.Procedure;

import edu.brown.markov.FeatureSet;
import edu.brown.markov.FeatureSet.Type;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

/**
 * Extract the base partition for the txn
 * @author pavlo
 */
public class BasePartitionFeature extends AbstractFeature {
    
    public BasePartitionFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, BasePartitionFeature.class);
    }

    @Override
    public void calculate(FeatureSet fset, TransactionTrace txn_trace) throws Exception {
        Integer base_partition = this.p_estimator.getBasePartition(this.catalog_proc, txn_trace.getParams());
        fset.addFeature(txn_trace, this.getFeatureKey(), base_partition, Type.RANGE);
    }

}
