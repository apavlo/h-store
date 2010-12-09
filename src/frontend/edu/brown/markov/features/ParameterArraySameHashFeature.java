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
public class ParameterArraySameHashFeature extends AbstractFeature {
    
    private final List<ProcParameter> array_params;
    
    public ParameterArraySameHashFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, "ParamArrayHash");
        
        // Get the list of ProcParameters that should be arrays
        this.array_params = CatalogUtil.getArrayProcParameters(this.catalog_proc);
    }

    @Override
    public void calculate(FeatureSet fset, TransactionTrace txn_trace) throws Exception {
        for (ProcParameter catalog_param : this.array_params) {
            Object params[] = (Object[])txn_trace.getParam(catalog_param.getIndex());
            boolean all_same = true;
            boolean first = true;
            Integer hash = null;
            
            if (params.length > 0) {
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
                fset.addFeature(txn_trace, this.getFeatureKey(catalog_param), all_same);
            }
        } // FOR
    }

}
