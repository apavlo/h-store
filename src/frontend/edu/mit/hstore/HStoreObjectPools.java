package edu.mit.hstore;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.impl.StackObjectPool;

import edu.brown.utils.TypedStackObjectPool;
import edu.mit.hstore.callbacks.LocalTransactionInitCallback;
import edu.mit.hstore.callbacks.RemoteTransactionInitCallback;
import edu.mit.hstore.callbacks.TransactionPrepareCallback;
import edu.mit.hstore.callbacks.TransactionRedirectCallback;
import edu.mit.hstore.callbacks.TransactionRedirectResponseCallback;
import edu.mit.hstore.callbacks.TransactionWorkCallback;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.RemoteTransaction;

public abstract class HStoreObjectPools {

    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    public static TypedStackObjectPool<LocalTransactionInitCallback> CALLBACKS_TXN_LOCALINIT;
    
    /**
     * 
     */
    public static TypedStackObjectPool<RemoteTransactionInitCallback> CALLBACKS_TXN_REMOTEINIT;
    
    /**
     * 
     */
    public static TypedStackObjectPool<TransactionWorkCallback> CALLBACKS_TXN_WORK;

    /**
     * 
     */
    public static TypedStackObjectPool<TransactionPrepareCallback> CALLBACKS_TXN_PREPARE;
    
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
     * RemoteTransaction State ObjectPool
     */
    public static TypedStackObjectPool<RemoteTransaction> STATES_TXN_REMOTE;
    
    /**
     * DependencyInfo ObjectPool
     */
    public static TypedStackObjectPool<DependencyInfo> STATES_DEPENDENCYINFO;

    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param hstore_site
     */
    public synchronized static void initialize(HStoreSite hstore_site) {
        assert(hstore_site != null);
        if (CALLBACKS_TXN_REDIRECT_REQUEST == null) {
            HStoreConf hstore_conf = hstore_site.getHStoreConf();
            CALLBACKS_TXN_REDIRECT_REQUEST = TypedStackObjectPool.factory(TransactionRedirectCallback.class,
                    hstore_conf.site.pool_forwardtxnrequests_idle,
                    hstore_conf.site.pool_profiling);
            CALLBACKS_TXN_REDIRECTRESPONSE = TypedStackObjectPool.factory(TransactionRedirectResponseCallback.class,
                    hstore_conf.site.pool_forwardtxnresponses_idle,
                    hstore_conf.site.pool_profiling);
            CALLBACKS_TXN_WORK = TypedStackObjectPool.factory(TransactionWorkCallback.class,
                    hstore_conf.site.pool_forwardtxnresponses_idle,
                    hstore_conf.site.pool_profiling);
            CALLBACKS_TXN_PREPARE = TypedStackObjectPool.factory(TransactionPrepareCallback.class,
                    hstore_conf.site.pool_txnprepare_idle,
                    hstore_conf.site.pool_profiling, hstore_site);
            CALLBACKS_TXN_LOCALINIT = TypedStackObjectPool.factory(LocalTransactionInitCallback.class,
                    hstore_conf.site.pool_localtxninit_idle,
                    hstore_conf.site.pool_profiling, hstore_site);

            STATES_TXN_LOCAL = TypedStackObjectPool.factory(LocalTransaction.class,
                    hstore_conf.site.pool_localtxnstate_idle,
                    hstore_conf.site.pool_profiling);
            STATES_TXN_REMOTE = TypedStackObjectPool.factory(RemoteTransaction.class,
                    hstore_conf.site.pool_remotetxnstate_idle,
                    hstore_conf.site.pool_profiling);
            STATES_DEPENDENCYINFO = TypedStackObjectPool.factory(DependencyInfo.class,
                    hstore_conf.site.pool_dependencyinfos_idle,
                    hstore_conf.site.pool_profiling);
        }
        assert(CALLBACKS_TXN_REDIRECT_REQUEST != null);
        assert(CALLBACKS_TXN_REDIRECTRESPONSE != null);
        assert(CALLBACKS_TXN_WORK != null);
        assert(CALLBACKS_TXN_PREPARE != null);
        assert(CALLBACKS_TXN_LOCALINIT != null);
        assert(STATES_TXN_REMOTE != null);
        assert(STATES_TXN_LOCAL != null);
        assert(STATES_DEPENDENCYINFO != null);
    }
    
    public static Map<String, StackObjectPool> getAllPools() {
        Map<String, StackObjectPool> m = new ListOrderedMap<String, StackObjectPool>();
        
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
