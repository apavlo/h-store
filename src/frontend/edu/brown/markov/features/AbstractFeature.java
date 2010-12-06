package edu.brown.markov.features;

import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

public abstract class AbstractFeature {

    protected final PartitionEstimator p_estimator;
    protected final Procedure catalog_proc;
    protected final String prefix;
    
    public AbstractFeature(PartitionEstimator p_estimator, Procedure catalog_proc, String prefix) {
        this.p_estimator = p_estimator;
        this.catalog_proc = catalog_proc;
        this.prefix = prefix;
    }

    /**
     * For the given TransactionTrace object, generate the set of features that we 
     * extract from the ProcParameters 
     * @param txn_trace
     * @return
     * @throws Exception
     */
    public abstract void calculate(FeatureSet fset, TransactionTrace txn_trace) throws Exception;
    
    /**
     * 
     * @return
     */
    public String getFeatureKey() {
        return (this.prefix);
    }
    
    /**
     * 
     * @param catalog_param
     * @return
     */
    public String getFeatureKey(ProcParameter catalog_param) {
        return (this.prefix + catalog_param.getIndex());
    }

    public String getFeatureKey(ProcParameter catalog_param, int idx) {
        return (this.getFeatureKey(catalog_param) + "-" + idx);
    }

}
