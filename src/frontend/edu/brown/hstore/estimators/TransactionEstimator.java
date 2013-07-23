package edu.brown.hstore.estimators;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * An Estimator is the runtime piece that we use to keep track of where the 
 * transaction is in its execution workflow. This allows us to make predictions about
 * what kind of things we expect the txn to do in the future
 * @author pavlo
 * @param <S> An EstimationState for the txn
 * @param <E> The current Estimation generated for the txn
 */
public abstract class TransactionEstimator {
    private static final Logger LOG = Logger.getLogger(TransactionEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    protected final HStoreConf hstore_conf;
    protected final PartitionEstimator p_estimator;
    protected final CatalogContext catalogContext;
    protected final AbstractHasher hasher;
    protected final int num_partitions;
    
    protected final AtomicInteger txn_count = new AtomicInteger(0);
    
    public TransactionEstimator(PartitionEstimator p_estimator) {
        this.hstore_conf = HStoreConf.singleton();
        this.p_estimator = p_estimator;
        this.catalogContext = p_estimator.getCatalogContext();
        this.hasher = p_estimator.getHasher();
        this.num_partitions = this.catalogContext.numberOfPartitions;
        
        if (debug.val)
            LOG.debug("Initialized fixed transaction estimator -> " + this.getClass().getSimpleName());
    }
    
    public CatalogContext getCatalogContext() {
        return this.catalogContext;
    }
    public PartitionEstimator getPartitionEstimator() {
        return this.p_estimator;
    }
    
    /**
     * Sets up the beginning of a transaction. Returns an estimate of where this
     * transaction will go.
     * @param txn_id
     * @param catalog_proc
     * @param BASE_PARTITION
     * @return an estimate for the transaction's future
     */
    public final <T extends EstimatorState> T startTransaction(Long txn_id, Procedure catalog_proc, Object args[]) {
        int base_partition = HStoreConstants.NULL_PARTITION_ID;
        try {
            base_partition = this.p_estimator.getBasePartition(catalog_proc, args);
            assert(base_partition != HStoreConstants.NULL_PARTITION_ID);
        } catch (Throwable ex) {
            throw new RuntimeException(String.format("Failed to calculate base partition for <%s, %s>", catalog_proc.getName(), Arrays.toString(args)), ex);
        }
        return (this.startTransaction(txn_id, base_partition, catalog_proc, args));
    }
    
    /**
     * Sets up the beginning of a transaction. Returns an estimate of where this
     * transaction will go.
     * @param txn_id
     * @param base_partition
     * @param catalog_proc
     * @param args
     * @return
     */
    public final <T extends EstimatorState> T startTransaction(Long txn_id, int base_partition, Procedure catalog_proc, Object args[]) {
        if (debug.val) LOG.debug(String.format("Checking %s input parameters: %s",
                                   catalog_proc.getName(),
                                   StringUtil.toString(args, true, true)));

        T state = this.startTransactionImpl(txn_id, base_partition, catalog_proc, args);
        if (state != null) this.txn_count.incrementAndGet();
        return (state);
    }
    
    
    /**
     * Implementation for starting a opening up a new transaction in the estimator 
     * @param txn_id
     * @param base_partition
     * @param catalog_proc
     * @param args
     * @return
     */
    protected abstract <T extends EstimatorState> T startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object args[]);
    
    
    /**
     * Takes a series of queries and executes them in order given the partition
     * information. Provides an estimate of where the transaction might go next.
     * @param state
     * @param catalog_stmts
     * @param partitions
     * @return
     */
    public abstract <T extends Estimate> T executeQueries(EstimatorState state, Statement catalog_stmts[], PartitionSet partitions[]);
    
    /**
     * The transaction with provided txn_id is finished
     * @param txn_id finished transaction
     */
    public void commit(EstimatorState state) {
        this.completeTransaction(state, Status.OK);
    }

    /**
     * The transaction with provided txn_id has aborted
     * @param txn_id
     */
    public void abort(EstimatorState state, Status status) {
        this.completeTransaction(state, status);
    }

    /**
     * Mark a transaction as finished execution
     * @param txn_id
     * @param vtype
     */
    protected abstract void completeTransaction(EstimatorState state, Status status);
    
    /**
     * Return the EstimatorState and have it destroyed.
     * <B>Note:</B> The EstimateState will be invalidated when this method is called.
     * @param state
     */
    public abstract void destroyEstimatorState(EstimatorState state);
    
}
