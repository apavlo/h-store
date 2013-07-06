package edu.brown.hstore.estimators.remote;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.NotImplementedException;

import edu.brown.hstore.Hstoreservice.QueryEstimate;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.TypedObjectPool;
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

public class RemoteEstimator extends TransactionEstimator {
    private static final Logger LOG = Logger.getLogger(MarkovEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final TypedObjectPool<RemoteEstimatorState> statesPool;
    
    public RemoteEstimator(PartitionEstimator p_estimator) {
        super(p_estimator);
        
        if (debug.val) LOG.debug("Creating RemoteEstimatorState Object Pool");
        TypedPoolableObjectFactory<RemoteEstimatorState> s_factory = new RemoteEstimatorState.Factory(this.catalogContext);
        int num_idle = (int)(hstore_conf.site.network_incoming_limit_txns * hstore_conf.site.pool_scale_factor);
        this.statesPool = new TypedObjectPool<RemoteEstimatorState>(s_factory, num_idle);
    }

    public void processQueryEstimate(RemoteEstimatorState state, QueryEstimate query_est, int partition) {
        RemoteEstimate est = state.createNextEstimate(query_est);
        est.addQueryEstimate(query_est, partition);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        RemoteEstimatorState state = null;
        try {
            state = this.statesPool.borrowObject();
            state.init(txn_id, base_partition, EstTime.currentTimeMillis());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return ((T)state);
    }
    
    @Override
    public void destroyEstimatorState(EstimatorState state) {
        this.statesPool.returnObject((RemoteEstimatorState)state);
    }

    @Override
    public <T extends Estimate> T executeQueries(EstimatorState state, Statement[] catalog_stmts, PartitionSet[] partitions) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    protected void completeTransaction(EstimatorState state, Status status) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

}
