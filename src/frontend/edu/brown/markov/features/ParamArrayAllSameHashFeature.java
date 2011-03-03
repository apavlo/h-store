package edu.brown.markov.features;

import java.util.List;

import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.FeatureSet;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

/**
 * For each ProcParameter that is an array, check whether all of the array
 * elements hash to the same value
 * @author pavlo
 */
public class ParamArrayAllSameHashFeature extends AbstractFeature {
    
    private final List<ProcParameter> array_params;
    
    public ParamArrayAllSameHashFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, ParamArrayAllSameHashFeature.class);
        
        // Get the list of ProcParameters that should be arrays
        this.array_params = CatalogUtil.getArrayProcParameters(this.catalog_proc);
    }

    private Boolean calculate(Object params[]) {
        Boolean all_same = null;
        if (params.length > 0) {
            all_same = true;
            boolean first = true;
            Integer hash = null;
            
            for (Object v : params) {
                Integer param_hash = this.p_estimator.getHasher().hash(v);
                if (first) {
                    hash = param_hash;
                    first = false;
                } else if (param_hash.equals(hash) == false) {
                    all_same = false;
                    break;
                }
            } // FOR
        }
        return (all_same);
    }
    
    @Override
    public void extract(FeatureSet fset, TransactionTrace txn_trace) throws Exception {
        for (ProcParameter catalog_param : this.array_params) {
            Object params[] = (Object[])txn_trace.getParam(catalog_param.getIndex());
            Boolean all_same = this.calculate(params);
            if (all_same != null) fset.addFeature(txn_trace, this.getFeatureKey(catalog_param), all_same);
        } // FOR
    }
    
    @Override
    public Object calculate(String key, Object params[]) throws Exception {
        ProcParameter catalog_param = this.getProcParameter(key);
        Object inner_params[] = (Object[])params[catalog_param.getIndex()];
        return (this.calculate(inner_params));
    }

}
