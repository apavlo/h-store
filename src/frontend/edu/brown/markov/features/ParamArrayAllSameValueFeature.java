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
 * elements have the exact same value
 * @author pavlo
 */
public class ParamArrayAllSameValueFeature extends AbstractFeature {
    
    private final List<ProcParameter> array_params;
    
    public ParamArrayAllSameValueFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, ParamArrayAllSameValueFeature.class);
        
        // Get the list of ProcParameters that should be arrays
        this.array_params = CatalogUtil.getArrayProcParameters(this.catalog_proc);
    }
    
    private Boolean calculate(Object params[]) {
        Boolean all_same = null;
        if (params.length > 0) {
            all_same = true;
            Object value = null;
            boolean first = true;
            
            for (Object v : params) {
                if (first) {
                    value = v;
                    first = false;
                } else if ((value == null && v != null) ||
                           (value != null && value.equals(v) == false)) {
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
