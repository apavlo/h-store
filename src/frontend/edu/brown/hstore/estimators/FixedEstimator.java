package edu.brown.hstore.estimators;

import org.voltdb.CatalogContext;

import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;

public abstract class FixedEstimator {

    public static AbstractEstimator<EstimationState, Estimation> getFixedEstimator(PartitionEstimator p_estimator, CatalogContext catalogContext){
        AbstractEstimator<EstimationState, Estimation> estimator = null;
        ProjectType ptype = ProjectType.get(catalogContext.database.getProject());
        switch (ptype) {
            case TPCC:
                estimator = new TPCCEstimator(catalogContext, p_estimator);
                break;
            case TM1:
                estimator = new TM1Estimator(catalogContext, p_estimator);
                break;
            case SEATS:
                estimator = new SEATSEstimator(catalogContext, p_estimator);
                break;
            default:
                estimator = null;
        } // SWITCH
        return (estimator);
    }
    
}
