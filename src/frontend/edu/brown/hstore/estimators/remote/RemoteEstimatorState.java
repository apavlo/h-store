package edu.brown.hstore.estimators.remote;

import org.voltdb.CatalogContext;

import edu.brown.hstore.Hstoreservice.QueryEstimate;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.pools.TypedPoolableObjectFactory;

public class RemoteEstimatorState extends EstimatorState {

    /**
     * State Factory
     */
    public static class Factory extends TypedPoolableObjectFactory<RemoteEstimatorState> {
        private final CatalogContext catalogContext;
        
        public Factory(CatalogContext catalogContext) {
            super(HStoreConf.singleton().site.pool_profiling);
            this.catalogContext = catalogContext;
        }
        @Override
        public RemoteEstimatorState makeObjectImpl() throws Exception {
            return (new RemoteEstimatorState(this.catalogContext));
        }
    };
    
    
    /**
     * Constructor
     */
    private RemoteEstimatorState(CatalogContext catalogContext) {
        super(catalogContext);
    }
    
    /**
     * Get the next Estimate object for this State
     * @return
     */
    protected RemoteEstimate createNextEstimate(QueryEstimate query_est) {
        RemoteEstimate next = new RemoteEstimate(this.catalogContext);
        if (this.getInitialEstimate() == null) {
            this.addInitialEstimate(next);
        } else {
            this.addEstimate(next);
        }
        return (next);
    }
}
