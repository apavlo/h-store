package edu.brown.markov.features;

import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.markov.FeatureSet;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

/**
 * The hash value of a particular parameter equals the same thing as the base partition
 * @author pavlo
 *
 */
public class ParameterHashPartitionFeature extends AbstractFeature {

    public ParameterHashPartitionFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, "ParamPartition");
    }
    
    @Override
    public void calculate(FeatureSet fset, TransactionTrace txn_trace) throws Exception {
        for (ProcParameter catalog_param : this.catalog_proc.getParameters()) {
            Object param = txn_trace.getParam(catalog_param.getIndex());
            if (catalog_param.getIsarray()) {
                Object inner[]  = (Object[])param;
                for (int i = 0; i < inner.length; i++) {
                    int param_hash = this.p_estimator.getHasher().hash(inner[i]);
                    fset.addFeature(txn_trace, this.getFeatureKey(catalog_param, i), param_hash, FeatureSet.Type.RANGE);
                } // FOR
                
            } else {
                int param_hash = this.p_estimator.getHasher().hash(param);
                fset.addFeature(txn_trace, this.getFeatureKey(catalog_param), param_hash, FeatureSet.Type.RANGE);
            }
        } // FOR

    }

}
