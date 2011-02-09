package edu.brown.markov.features;

import java.util.List;

import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.FeatureSet;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

/**
 * Generate features for the length of the array parameters
 * @author pavlo
 */
public class ParamArrayLengthFeature extends AbstractFeature {
    
    private final List<ProcParameter> array_params;
    
    public ParamArrayLengthFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, ParamArrayLengthFeature.class);
        
        // Get the list of ProcParameters that should be arrays
        this.array_params = CatalogUtil.getArrayProcParameters(this.catalog_proc);
    }
    
    @Override
    public void extract(FeatureSet fset, TransactionTrace txn_trace) throws Exception {
        for (ProcParameter catalog_param : this.array_params) {
            Object params[] = (Object[])txn_trace.getParam(catalog_param.getIndex());
            fset.addFeature(txn_trace, this.getFeatureKey(catalog_param), params.length);
        } // FOR
    }
    
    @Override
    public Object calculate(String key, Object params[]) throws Exception {
        ProcParameter catalog_param = this.getProcParameter(key);
        assert(catalog_param.getIsarray()) : "Invalid: " + catalog_param;
        Object inner_params[] = (Object[])params[catalog_param.getIndex()];
        return (inner_params.length);
    }

}
