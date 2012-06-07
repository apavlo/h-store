package edu.brown.hstore;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.ParameterSet;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.callbacks.TransactionRedirectCallback;
import edu.brown.hstore.callbacks.TransactionRedirectResponseCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.DependencyInfo;
import edu.brown.hstore.dtxn.DistributedState;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.hstore.dtxn.PrefetchState;
import edu.brown.hstore.dtxn.RemoteTransaction;
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
    public final TypedObjectPool<LocalTransaction> STATES_TXN_LOCAL;
    
    /**
     * MapReduceTransaction State ObjectPool
     */
    public final TypedObjectPool<MapReduceTransaction> STATES_TXN_MAPREDUCE;
    
    /**
     * RemoteTransaction State ObjectPool
     */
    public final TypedObjectPool<RemoteTransaction> STATES_TXN_REMOTE;
    
    /**
     * DependencyInfo ObjectPool
     */
    public final TypedObjectPool<DependencyInfo> STATES_DEPENDENCYINFO;

    /**
     * PrefetchState ObjectPool
     */
    public final TypedObjectPool<PrefetchState> STATES_PREFETCH;
    
    /**
     * DistributedState ObjectPool
     */
    public final TypedObjectPool<DistributedState> STATES_DISTRIBUTED;
    
    // ----------------------------------------------------------------------------
    // ADDITIONAL OBJECTS
    // ----------------------------------------------------------------------------
    
    public final TypedObjectPool<ParameterSet> PARAMETERSETS;
    
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public HStoreObjectPools(HStoreSite hstore_site) {
        assert(hstore_site != null);
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

        // STATES
        
        this.STATES_TXN_LOCAL = TypedObjectPool.factory(LocalTransaction.class,
                (int)(hstore_conf.site.pool_localtxnstate_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        this.STATES_TXN_REMOTE = TypedObjectPool.factory(RemoteTransaction.class,
                (int)(hstore_conf.site.pool_remotetxnstate_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        this.STATES_DEPENDENCYINFO = TypedObjectPool.factory(DependencyInfo.class,
                (int)(hstore_conf.site.pool_dependencyinfos_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling);
        this.STATES_DISTRIBUTED = TypedObjectPool.factory(DistributedState.class,
                (int)(hstore_conf.site.pool_dtxnstates_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        
        // ADDITIONAL
        this.PARAMETERSETS = TypedObjectPool.factory(ParameterSet.class,
                (int)(hstore_conf.site.pool_parametersets_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling);
        
        // If there are no prefetchable queries or MapReduce procedures in the catalog, then we will not
        // create these special object pools
        TypedObjectPool<PrefetchState> STATES_PREFETCH = null;
        TypedObjectPool<MapReduceTransaction> STATES_TXN_MAPREDUCE = null;
        for (Procedure catalog_proc : hstore_site.getDatabase().getProcedures()) {
            if (STATES_PREFETCH == null && catalog_proc.getPrefetchable() && hstore_conf.site.exec_prefetch_queries) {
                STATES_PREFETCH = TypedObjectPool.factory(PrefetchState.class,
                        (int)(hstore_conf.site.pool_prefetchstates_idle * hstore_conf.site.pool_scale_factor),
                        hstore_conf.site.pool_profiling, hstore_site);
            }
            if (STATES_TXN_MAPREDUCE == null && catalog_proc.getMapreduce()) {
                STATES_TXN_MAPREDUCE = TypedObjectPool.factory(MapReduceTransaction.class,
                        (int)(hstore_conf.site.pool_mapreducetxnstate_idle * hstore_conf.site.pool_scale_factor),
                        hstore_conf.site.pool_profiling, hstore_site);
            }
        } // FOR
        this.STATES_PREFETCH = STATES_PREFETCH;
        this.STATES_TXN_MAPREDUCE = STATES_TXN_MAPREDUCE;
        
        // Sanity Check: Make sure that we allocated an object pool for all of the 
        // fields that we have defined except for STATES_PREFETCH_STATE
        for (Entry<String, TypedObjectPool<?>> e : this.getAllPools().entrySet()) {
            String poolName = e.getKey();
            if (poolName.equals("STATES_PREFETCH") || poolName.equals("STATES_TXN_MAPREDUCE")) continue;
            assert(e.getValue() != null) : poolName + " is null!";
        } // FOR
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
