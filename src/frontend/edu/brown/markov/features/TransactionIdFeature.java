package edu.brown.markov.features;

import org.voltdb.catalog.Procedure;

import edu.brown.markov.FeatureSet;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

public class TransactionIdFeature extends AbstractFeature {

    public TransactionIdFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, "TxnId");
    }

    
    @Override
    public void calculate(FeatureSet fset, TransactionTrace txnTrace) throws Exception {
        fset.addFeature(txnTrace, this.getFeatureKey(), txnTrace.getTransactionId());

    }

}
