package edu.brown.hstore;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.pool.impl.StackObjectPool;
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
import edu.brown.utils.TypedStackObjectPool;

public abstract class HStoreObjectPools {

    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    /**
     * TransactionInitQueueCallback Pool
     */
    public static TypedStackObjectPool<TransactionInitQueueCallback> CALLBACKS_TXN_INITQUEUE;
    
    /**
     * ForwardTxnRequestCallback Pool
     */
    public static TypedStackObjectPool<TransactionRedirectCallback> CALLBACKS_TXN_REDIRECT_REQUEST;
    
    /**
     * ForwardTxnResponseCallback Pool
     */
    public static TypedStackObjectPool<TransactionRedirectResponseCallback> CALLBACKS_TXN_REDIRECTRESPONSE;
    
    // ----------------------------------------------------------------------------
    // INTERNAL STATE OBJECTS
    // ----------------------------------------------------------------------------
    
    /**
     * LocalTransaction State ObjectPool
     */
    public static TypedStackObjectPool<LocalTransaction> STATES_TXN_LOCAL;
    
    /**
     * MapReduceTransaction State ObjectPool
     */
    public static TypedStackObjectPool<MapReduceTransaction> STATES_TXN_MAPREDUCE;
    
    /**
     * RemoteTransaction State ObjectPool
     */
    public static TypedStackObjectPool<RemoteTransaction> STATES_TXN_REMOTE;
    
    /**
     * DependencyInfo ObjectPool
     */
    public static TypedStackObjectPool<DependencyInfo> STATES_DEPENDENCYINFO;

    /**
     * PrefetchState ObjectPool
     */
    public static TypedStackObjectPool<PrefetchState> STATES_PREFETCH;
    
    /**
     * DistributedState ObjectPool
     */
    public static TypedStackObjectPool<DistributedState> STATES_DISTRIBUTED;
    
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param hstore_site
     */
    public synchronized static void initialize(HStoreSite hstore_site) {
        assert(hstore_site != null);
        HStoreConf hstore_conf = hstore_site.getHStoreConf();
        
        CALLBACKS_TXN_INITQUEUE = TypedStackObjectPool.factory(TransactionInitQueueCallback.class,
                (int)(hstore_conf.site.pool_txninitqueue_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        
        CALLBACKS_TXN_REDIRECT_REQUEST = TypedStackObjectPool.factory(TransactionRedirectCallback.class,
                (int)(hstore_conf.site.pool_txnredirect_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling);
        CALLBACKS_TXN_REDIRECTRESPONSE = TypedStackObjectPool.factory(TransactionRedirectResponseCallback.class,
                (int)(hstore_conf.site.pool_txnredirectresponses_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling);

        STATES_TXN_LOCAL = TypedStackObjectPool.factory(LocalTransaction.class,
                (int)(hstore_conf.site.pool_localtxnstate_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        STATES_TXN_REMOTE = TypedStackObjectPool.factory(RemoteTransaction.class,
                (int)(hstore_conf.site.pool_remotetxnstate_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        STATES_DEPENDENCYINFO = TypedStackObjectPool.factory(DependencyInfo.class,
                (int)(hstore_conf.site.pool_dependencyinfos_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling);
        STATES_DISTRIBUTED = TypedStackObjectPool.factory(DistributedState.class,
                (int)(hstore_conf.site.pool_dtxnstates_idle * hstore_conf.site.pool_scale_factor),
                hstore_conf.site.pool_profiling, hstore_site);
        
        // If there are no prefetchable queries or MapReduce procedures in the catalog, then we will not
        // create these special object pools
        STATES_PREFETCH = null;
        STATES_TXN_MAPREDUCE = null;
        for (Procedure catalog_proc : hstore_site.getDatabase().getProcedures()) {
            if (STATES_PREFETCH == null && catalog_proc.getPrefetchable() && hstore_conf.site.exec_prefetch_queries) {
                STATES_PREFETCH = TypedStackObjectPool.factory(PrefetchState.class,
                        (int)(hstore_conf.site.pool_prefetchstates_idle * hstore_conf.site.pool_scale_factor),
                        hstore_conf.site.pool_profiling, hstore_site);
            }
            if (STATES_TXN_MAPREDUCE == null && catalog_proc.getMapreduce()) {
                STATES_TXN_MAPREDUCE = TypedStackObjectPool.factory(MapReduceTransaction.class,
                        (int)(hstore_conf.site.pool_mapreducetxnstate_idle * hstore_conf.site.pool_scale_factor),
                        hstore_conf.site.pool_profiling, hstore_site);
            }
        } // FOR
        
        // Sanity Check: Make sure that we allocated an object pool for all of the 
        // fields that we have defined except for STATES_PREFETCH_STATE
        for (Entry<String, StackObjectPool> e : getAllPools().entrySet()) {
            String poolName = e.getKey();
            if (poolName.equals("STATES_PREFETCH") || poolName.equals("STATES_TXN_MAPREDUCE")) continue;
            assert(e.getValue() != null) : poolName + " is null!";
        } // FOR
    }
    
    public static Map<String, StackObjectPool> getAllPools() {
        Map<String, StackObjectPool> m = new LinkedHashMap<String, StackObjectPool>();
        Object val = null;
        for (Field f : HStoreObjectPools.class.getFields()) {
            try {
                val = f.get(null);
                if (val instanceof TypedStackObjectPool<?>) {
                    m.put(f.getName(), (StackObjectPool)val);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        
        return (m);
    }
}
