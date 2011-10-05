package edu.mit.hstore;

import edu.brown.utils.TypedStackObjectPool;
import edu.mit.hstore.callbacks.LocalTransactionInitCallback;
import edu.mit.hstore.callbacks.TransactionPrepareCallback;
import edu.mit.hstore.callbacks.TransactionRedirectCallback;
import edu.mit.hstore.callbacks.TransactionRedirectResponseCallback;
import edu.mit.hstore.callbacks.TransactionWorkCallback;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.RemoteTransaction;

public abstract class HStoreObjectPools {

    /**
     * ForwardTxnRequestCallback Pool
     */
    public static TypedStackObjectPool<TransactionRedirectCallback> POOL_TXNREDIRECT_REQUEST;
    
    /**
     * ForwardTxnResponseCallback Pool
     */
    public static TypedStackObjectPool<TransactionRedirectResponseCallback> POOL_TXNREDIRECT_RESPONSE;
    
    /**
     * 
     */
    public static TypedStackObjectPool<TransactionWorkCallback> POOL_TXNWORK;

    /**
     * 
     */
    public static TypedStackObjectPool<TransactionPrepareCallback> POOL_TXNPREPARE;
    
    /**
     * 
     */
    public static TypedStackObjectPool<LocalTransactionInitCallback> POOL_TXN_LOCALINIT;
    
    /**
     * RemoteTransaction Object Pool
     */
    public static TypedStackObjectPool<RemoteTransaction> remoteTxnPool;

    /**
     * LocalTransactionState Object Pool
     */
    public static TypedStackObjectPool<LocalTransaction> localTxnPool;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param hstore_site
     */
    public synchronized static void initialize(HStoreSite hstore_site) {
        assert(hstore_site != null);
        if (POOL_TXNREDIRECT_REQUEST == null) {
            HStoreConf hstore_conf = hstore_site.getHStoreConf();
            POOL_TXNREDIRECT_REQUEST = TypedStackObjectPool.factory(TransactionRedirectCallback.class,
                    hstore_conf.site.pool_forwardtxnrequests_idle,
                    hstore_conf.site.pool_profiling);
            POOL_TXNREDIRECT_RESPONSE = TypedStackObjectPool.factory(TransactionRedirectResponseCallback.class,
                    hstore_conf.site.pool_forwardtxnresponses_idle,
                    hstore_conf.site.pool_profiling);
            POOL_TXNWORK = TypedStackObjectPool.factory(TransactionWorkCallback.class,
                    hstore_conf.site.pool_forwardtxnresponses_idle,
                    hstore_conf.site.pool_profiling);
            POOL_TXNPREPARE = TypedStackObjectPool.factory(TransactionPrepareCallback.class,
                    hstore_conf.site.pool_txnprepare_idle,
                    hstore_conf.site.pool_profiling, hstore_site);
            POOL_TXN_LOCALINIT = TypedStackObjectPool.factory(LocalTransactionInitCallback.class,
                    hstore_conf.site.pool_localtxninit_idle,
                    hstore_conf.site.pool_profiling, hstore_site);
            remoteTxnPool = TypedStackObjectPool.factory(RemoteTransaction.class,
                    hstore_conf.site.pool_remotetxnstate_idle,
                    hstore_conf.site.pool_profiling);
            localTxnPool = TypedStackObjectPool.factory(LocalTransaction.class,
                    hstore_conf.site.pool_localtxnstate_idle,
                    hstore_conf.site.pool_profiling);
        }
    }
}
