package edu.brown.markov.features;

import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import edu.brown.markov.FeatureSet;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

public abstract class AbstractFeature {

    protected final PartitionEstimator p_estimator;
    protected final Procedure catalog_proc;
    protected final String prefix;
    protected final Class<? extends AbstractFeature> feature_class;
    
    /**
     * Constructor
     * @param p_estimator
     * @param catalog_proc
     * @param feature_class
     */
    protected AbstractFeature(PartitionEstimator p_estimator, Procedure catalog_proc, Class<? extends AbstractFeature> feature_class) {
        this.p_estimator = p_estimator;
        this.catalog_proc = catalog_proc;
        this.feature_class = feature_class;
        this.prefix = FeatureUtil.getFeatureKeyPrefix(this.feature_class);
    }

    /**
     * For the given TransactionTrace object, extract the set of features from the ProcParameters 
     * @param txn_trace
     * @return
     * @throws Exception
     */
    public abstract void extract(FeatureSet fset, TransactionTrace txn_trace) throws Exception;
    
    /**
     * For the given feature key, calculate its value from the given transaction parameters 
     * @param key
     * @param txn_trace
     * @return
     * @throws Exception
     */
    public abstract Object calculate(String key, Object params[]) throws Exception;
    
    /**
     * For the given feature key, calculate its value from the given TransactionTrace 
     * @param key
     * @param txn_trace
     * @return
     * @throws Exception
     */
    public Object calculate(String key, TransactionTrace txn_trace) throws Exception {
        return (this.calculate(key, txn_trace.getParams()));
    }

    /**
     * Return the ProcParameter referenced in this feature key
     * @param feature_key
     * @return
     */
    public ProcParameter getProcParameter(String feature_key) {
        assert(feature_key.contains(FeatureUtil.DELIMITER)) : "Invalid: " + feature_key;
        Integer param_idx = Integer.valueOf(FeatureUtil.PREFIX_SPLITTER.split(feature_key, 2)[1]);
        return (this.catalog_proc.getParameters().get(param_idx));
    }
    
    public Pair<ProcParameter, Integer> getProcParameterWithIndex(String feature_key) {
        assert(feature_key.contains(FeatureUtil.DELIMITER)) : "Invalid: " + feature_key;
        String split[] = FeatureUtil.PREFIX_SPLITTER.split(feature_key);
        ProcParameter catalog_param = this.catalog_proc.getParameters().get(Integer.valueOf(split[1]));
        Integer param_idx = (split.length > 2 ? Integer.valueOf(split[2]) : null);
        return (Pair.of(catalog_param, param_idx));
    }
    
    /**
     * Return the default key for this feature 
     * @return
     */
    public String getFeatureKey() {
        return (this.prefix);
    }
    
    /**
     * Return the feature key that includes the given ProcParameter index number
     * @param catalog_param
     * @return
     */
    public String getFeatureKey(ProcParameter catalog_param) {
        return (String.format(FeatureUtil.PREFIX_FORMAT, this.prefix, catalog_param.getIndex()));
    }

    /**
     * Return the feature key that includes the given ProcParameter index number and the array offset
     * @param catalog_param
     * @param idx
     * @return
     */
    public String getFeatureKey(ProcParameter catalog_param, int idx) {
        return (String.format(FeatureUtil.PREFIX_FORMAT, this.getFeatureKey(catalog_param), idx));
    }

}