package edu.brown.hstore.estimators.remote;

import org.voltdb.CatalogContext;

import edu.brown.hstore.Hstoreservice.QueryEstimate;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.pools.TypedPoolableObjectFactory;

public class RemoteEstimatorState extends EstimatorState {

    private int currentBatch = 0;
    
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
    
    @Override
    public void finish() {
        this.currentBatch = 0;
        super.finish();
    }
    
    /**
     * Get the next Estimate object for this State
     * @param batchId TODO
     * @return
     */
    protected RemoteEstimate createNextEstimate(QueryEstimate query_est) {
        // FIXME: The way that this is written now means that we're going to
        // be making a new RemoteEstimate handle for every single QueryEstimate
        // that gets sent over the wire in a TransactionWorkRequest.
        // We need a way to identify that they're a part of the same batch
        RemoteEstimate next = new RemoteEstimate(this.catalogContext);
        next.init(this.currentBatch++);
        // We never have an initial estimate for remote txns
        this.addEstimate(next);
        return (next);
    }
}
