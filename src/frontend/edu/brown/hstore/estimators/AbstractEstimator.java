package edu.brown.hstore.estimators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.interfaces.Loggable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;

/**
 * An Estimator is the runtime piece that we use to keep track of where the 
 * transaction is in its execution workflow. This allows us to make predictions about
 * what kind of things we expect the txn to do in the future
 * @author pavlo
 * @param <S> An EstimationState for the txn
 * @param <E> The current Estimation generated for the txn
 */
public abstract class AbstractEstimator<S extends EstimationState, E extends Estimation> implements Loggable {
    private static final Logger LOG = Logger.getLogger(AbstractEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    protected final HStoreConf hstore_conf;
    protected final PartitionEstimator p_estimator;
    protected final CatalogContext catalogContext;
    protected final AbstractHasher hasher;
    protected final int num_partitions;
    
    /**
     * If we're using the TransactionEstimator, then we need to convert all 
     * primitive array ProcParameters into object arrays...
     */
    protected final Map<Procedure, ParameterMangler> manglers;
    
    protected final AtomicInteger txn_count = new AtomicInteger(0);
    
    /**
     * PartitionId -> PartitionId Singleton Sets
     */
    protected final Map<Integer, PartitionSet> singlePartitionSets = new HashMap<Integer, PartitionSet>();
    
    public AbstractEstimator(PartitionEstimator p_estimator) {
        this.hstore_conf = HStoreConf.singleton();
        this.p_estimator = p_estimator;
        this.catalogContext = p_estimator.getCatalogContext();
        this.hasher = p_estimator.getHasher();
        this.num_partitions = this.catalogContext.numberOfPartitions;
        
        // Create all of our parameter manglers
        this.manglers = new IdentityHashMap<Procedure, ParameterMangler>();
        for (Procedure catalog_proc : this.catalogContext.database.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
        } // FOR
        if (debug.get()) LOG.debug(String.format("Created ParameterManglers for %d procedures",
                                   this.manglers.size()));
        
        for (Integer p : catalogContext.getAllPartitionIdArray()) {
            this.singlePartitionSets.put(p, catalogContext.getPartitionSetSingleton(p));
        } // FOR
        
        if (debug.get())
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
    public S startTransaction(Long txn_id, Procedure catalog_proc, Object args[]) {
        int base_partition = HStoreConstants.NULL_PARTITION_ID;
        try {
            base_partition = this.p_estimator.getBasePartition(catalog_proc, args);
            assert(base_partition != HStoreConstants.NULL_PARTITION_ID);
        } catch (Throwable ex) {
            throw new RuntimeException(String.format("Failed to calculate base partition for <%s, %s>", catalog_proc.getName(), Arrays.toString(args)), ex);
        }
        return (this.startTransactionImpl(txn_id, base_partition, catalog_proc, args));
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
    public S startTransaction(Long txn_id, int base_partition, Procedure catalog_proc, Object args[]) {
        ParameterMangler mangler = this.manglers.get(catalog_proc);
        if (mangler == null) return (null);
        
        Object mangled[] = mangler.convert(args);
        if (debug.get()) LOG.debug(String.format("Checking %s input parameters:\n%s",
                                   catalog_proc.getName(), mangler.toString(mangled)));
        
        return (this.startTransactionImpl(txn_id, base_partition, catalog_proc, mangled));
    }
    
    
    public abstract S startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object args[]);
    
    
    /**
     * Takes a series of queries and executes them in order given the partition
     * information. Provides an estimate of where the transaction might go next.
     * @param state
     * @param catalog_stmts
     * @param partitions
     * @param allow_cache_lookup TODO
     * @return
     */
    public abstract E executeQueries(S state, Statement catalog_stmts[], PartitionSet partitions[], boolean allow_cache_lookup);
    
    /**
     * The transaction with provided txn_id is finished
     * @param txn_id finished transaction
     */
    public S commit(Long txn_id) {
        return (this.completeTransaction(txn_id, Status.OK));
    }

    /**
     * The transaction with provided txn_id has aborted
     * @param txn_id
     */
    public S abort(long txn_id, Status status) {
        return (this.completeTransaction(txn_id, status));
    }

    /**
     * 
     * @param txn_id
     * @param vtype
     * @return
     */
    protected abstract S completeTransaction(Long txn_id, Status status);
    
}
