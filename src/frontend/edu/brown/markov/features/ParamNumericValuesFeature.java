package edu.brown.markov.features;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import edu.brown.markov.FeatureSet;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;

/**
 * Extract all parameters that have numeric values
 * @author pavlo
 *
 */
public class ParamNumericValuesFeature extends AbstractFeature {
    
    private final List<ProcParameter> numeric_params = new ArrayList<ProcParameter>();

    public ParamNumericValuesFeature(PartitionEstimator p_estimator, Procedure catalog_proc) {
        super(p_estimator, catalog_proc, ParamNumericValuesFeature.class);
        
        for (ProcParameter catalog_param : catalog_proc.getParameters()) {
            VoltType type = VoltType.get(catalog_param.getType());
            switch (type) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
//                case FLOAT:
//                case DECIMAL:
                    this.numeric_params.add(catalog_param);
                    break;
            } // SWITCH
        } // FOR
    }
    
    @Override
    public void extract(FeatureSet fset, TransactionTrace txn_trace) throws Exception {
        for (ProcParameter catalog_param : this.numeric_params) {
            Object param = txn_trace.getParam(catalog_param.getIndex());
            if (catalog_param.getIsarray()) {
                Object inner[]  = (Object[])param;
                for (int i = 0; i < inner.length; i++) {
                    fset.addFeature(txn_trace, this.getFeatureKey(catalog_param, i), inner[i], FeatureSet.Type.NUMERIC);
                } // FOR
            } else {
                fset.addFeature(txn_trace, this.getFeatureKey(catalog_param), param, FeatureSet.Type.NUMERIC);
            }
        } // FOR
    }

    @Override
    public Object calculate(String key, Object params[]) throws Exception {
        Pair<ProcParameter, Integer> p = this.getProcParameterWithIndex(key);
        Object param = params[p.getFirst().getIndex()];
        if (p.getSecond() != null) {
            assert(p.getFirst().getIsarray()) : "Invalid: " + key;
            param = ((Object[])param)[p.getSecond()];
        }
        return (param);
    }
}
