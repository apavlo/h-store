package edu.brown.hstore.estimators.remote;

import org.voltdb.CatalogContext;

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
}
