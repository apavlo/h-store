package edu.brown.hstore;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.callbacks.TransactionRedirectCallback;
import edu.brown.hstore.callbacks.TransactionRedirectResponseCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.DistributedState;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.hstore.txns.PrefetchState;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.pools.TypedObjectPool;

public final class HStoreObjectPools {

    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    /**
     * TransactionInitQueueCallback Pool
     */
    public final TypedObjectPool<TransactionInitQueueCallback> CALLBACKS_TXN_INITQUEUE;
    
    /**
     * ForwardTxnRequestCallback Pool
     */
    public final TypedObjectPool<TransactionRedirectCallback> CALLBACKS_TXN_REDIRECT_REQUEST;
    
    /**
     * ForwardTxnResponseCallback Pool
     */
    public final TypedObjectPool<TransactionRedirectResponseCallback> CALLBACKS_TXN_REDIRECTRESPONSE;
    
    // ----------------------------------------------------------------------------
    // INTERNAL STATE OBJECTS
    // ----------------------------------------------------------------------------
    
    /**
     * LocalTransaction State ObjectPool
     */
    private final TypedObjectPool<LocalTransaction> STATES_TXN_LOCAL[];
    
    /**
     * MapReduceTransaction State ObjectPool
     */
    private final TypedObjectPool<MapReduceTransaction> STATES_TXN_MAPREDUCE[];
    
    /**
     * RemoteTransaction State ObjectPool
     */
    private final TypedObjectPool<RemoteTransaction> STATES_TXN_REMOTE[];
    
    /**
     * PrefetchState ObjectPool
     */
    public final TypedObjectPool<PrefetchState> STATES_PREFETCH;
    
    /**
     * DistributedState ObjectPool
     */
    private final TypedObjectPool<DistributedState> STATES_DISTRIBUTED[];
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    private final HStoreSite hstore_site;
    
    
    @SuppressWarnings("unchecked")
    public HStoreObjectPools(HStoreSite hstore_site) {
        assert(hstore_site != null);
        this.hstore_site = hstore_site;
        HStoreConf hstore_conf = hstore_site.getHStoreConf();
        
        // CALLBACKS
        
        this.CALLBACKS_TXN_INITQUEUE = TypedObjectPool.factory(TransactionInitQueueCallback.class,
                (int)(hstore_conf.site.pool_txninitqueue_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        this.CALLBACKS_TXN_REDIRECT_REQUEST = TypedObjectPool.factory(TransactionRedirectCallback.class,
                (int)(hstore_conf.site.pool_txnredirect_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        this.CALLBACKS_TXN_REDIRECTRESPONSE = TypedObjectPool.factory(TransactionRedirectResponseCallback.class,
                (int)(hstore_conf.site.pool_txnredirectresponses_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);

        // -------------------------------
        // LOCAL PARTITION POOLS
        // -------------------------------
        
        // We will have one object pool per local partition
        int num_local_partitions = hstore_site.getLocalPartitionIds().size();
        
        this.STATES_TXN_LOCAL = (TypedObjectPool<LocalTransaction>[])new TypedObjectPool<?>[num_local_partitions];
        this.STATES_DISTRIBUTED = (TypedObjectPool<DistributedState>[])new TypedObjectPool<?>[num_local_partitions];
        
        for (int i = 0; i < num_local_partitions; i++) {
            
            // LocalTransaction
            this.STATES_TXN_LOCAL[i] = TypedObjectPool.factory(LocalTransaction.class,
                (int)(hstore_conf.site.pool_localtxnstate_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
            
            // DistributedState
            this.STATES_DISTRIBUTED[i] = TypedObjectPool.factory(DistributedState.class,
                (int)(hstore_conf.site.pool_dtxnstates_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
            
        } // FOR
        
        // -------------------------------
        // ALL PARTITION POOLS
        // -------------------------------
        
        int num_all_partitions = hstore_site.getAllPartitionIds().size();
        boolean has_mapreduce = (CatalogUtil.getMapReduceProcedures(hstore_site.getDatabase()).isEmpty() == false);
        
        this.STATES_TXN_REMOTE = (TypedObjectPool<RemoteTransaction>[])new TypedObjectPool<?>[num_all_partitions];
        this.STATES_TXN_MAPREDUCE = (TypedObjectPool<MapReduceTransaction>[])new TypedObjectPool<?>[num_all_partitions];
        
        for (int i = 0; i < num_all_partitions; i++) {
            
            // RemoteTransaction
            this.STATES_TXN_REMOTE[i] = TypedObjectPool.factory(RemoteTransaction.class,
                (int)(hstore_conf.site.pool_remotetxnstate_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
            
            // MapReduceTransaction
            if (has_mapreduce) {
                this.STATES_TXN_MAPREDUCE[i] = TypedObjectPool.factory(MapReduceTransaction.class,
                    (int)(hstore_conf.site.pool_mapreducetxnstate_idle * hstore_conf.site.pool_scale_factor),
                    hstore_conf.site.pool_profiling, hstore_site);
            }
        } // FOR
        
        
        // ADDITIONAL
//        this.PARAMETERSETS = TypedObjectPool.factory(ParameterSet.class,
//                (int)(hstore_conf.site.pool_parametersets_idle * hstore_conf.site.pool_scale_factor),
//                hstore_conf.site.pool_profiling);
        
        // If there are no prefetchable queries or MapReduce procedures in the catalog, then we will not
        // create these special object pools
        TypedObjectPool<PrefetchState> STATES_PREFETCH = null;
        for (Procedure catalog_proc : hstore_site.getDatabase().getProcedures()) {
            if (STATES_PREFETCH == null && catalog_proc.getPrefetchable() && hstore_conf.site.exec_prefetch_queries) {
                STATES_PREFETCH = TypedObjectPool.factory(PrefetchState.class,
                        (int)(hstore_conf.site.pool_prefetchstates_idle * hstore_conf.site.pool_scale_factor),
                        hstore_conf.site.pool_profiling, hstore_site);
            }
        } // FOR
        this.STATES_PREFETCH = STATES_PREFETCH;
        
        // Sanity Check: Make sure that we allocated an object pool for all of the 
        // fields that we have defined except for STATES_PREFETCH_STATE
        for (Entry<String, TypedObjectPool<?>> e : this.getAllPools().entrySet()) {
            String poolName = e.getKey();
            if (poolName.equals("STATES_PREFETCH") || poolName.equals("STATES_TXN_MAPREDUCE")) continue;
            assert(e.getValue() != null) : poolName + " is null!";
        } // FOR
    }

    public TypedObjectPool<LocalTransaction> getLocalTransactionPool(int partition) {
        int offset = this.hstore_site.getLocalPartitionOffset(partition);
        return this.STATES_TXN_LOCAL[offset];
    }
    
    public TypedObjectPool<RemoteTransaction> getRemoteTransactionPool(int partition) {
        return this.STATES_TXN_REMOTE[partition];
    }
    
    public TypedObjectPool<MapReduceTransaction> getMapReduceTransactionPool(int partition) {
        return this.STATES_TXN_MAPREDUCE[partition];
    }
    
    public TypedObjectPool<DistributedState> getDistributedStatePool(int partition) {
        int offset = this.hstore_site.getLocalPartitionOffset(partition);
        return this.STATES_DISTRIBUTED[offset];
    }
    
    public Map<String, TypedObjectPool<?>> getAllPools() {
        Map<String, TypedObjectPool<?>> m = new LinkedHashMap<String, TypedObjectPool<?>>();
        Object val = null;
        for (Field f : HStoreObjectPools.class.getFields()) {
            try {
                val = f.get(this);
                if (val instanceof TypedObjectPool<?>) {
                    m.put(f.getName(), (TypedObjectPool<?>)val);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        
        return (m);
    }
}
