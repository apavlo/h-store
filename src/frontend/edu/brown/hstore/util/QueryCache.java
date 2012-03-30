package edu.brown.hstore.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class QueryCache {
    private static final Logger LOG = Logger.getLogger(PartitionExecutor.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * The default number of CacheEntry offsets to include in the txnCache's 
     * pool of List<Integer>. Note that these are just the entries at the
     * PartitionExecutor that this QueryCache is attached to
     */
    private static final int TXNCACHE_POOL_DEFAULT_SIZE = 3;
    
    /**
     * The number of List<Integer> we're allowed to keep idle at this PartitionExecutor.
     */
    private static final int TXNCACHE_POOL_MAXIDLE = 200;
    
    // ----------------------------------------------------------------------------
    // INTERNAL CACHE MEMBERS
    // ----------------------------------------------------------------------------
    
    private class CacheEntry {
        final Integer idx;
        Long txnId;
        int fragmentId;
        int paramsHash;
        VoltTable result;
        int accessCounter = 0;
        
        public CacheEntry(int idx) {
            this.idx = Integer.valueOf(idx);
        }
        
        public void init(int fragmentId, ParameterSet params, VoltTable result) {
            this.fragmentId = fragmentId;
            this.paramsHash = params.hashCode();
            this.result = result;
        }
    } // CLASS
    
    /**
     * Simple circular buffer cache
     */
    private class Cache {
        private final CacheEntry buffer[];
        private int current = 0;
        
        public Cache(int bufferSize) {
            this.buffer = new CacheEntry[bufferSize];
            for (int i = 0; i < bufferSize; i++) {
                this.buffer[i] = new CacheEntry(i);
            }
        }
        
        public CacheEntry getNext(Long txnId) {
            if (this.current == this.buffer.length) {
                this.current = 0;
            }
            CacheEntry next = this.buffer[this.current++];
            next.txnId = txnId;
            next.accessCounter = 0;
            return (next);
        }
        
        public CacheEntry get(Integer idx) {
            return (this.buffer[idx.intValue()]);
        }
    } // CLASS
    
    /**
     * List<Integer> pool used by txnCache
     */
    private final ObjectPool listPool = new StackObjectPool(new BasePoolableObjectFactory() {
        @Override
        public Object makeObject() throws Exception {
            return (new ArrayList<Integer>(TXNCACHE_POOL_DEFAULT_SIZE));
        }

        public void passivateObject(Object obj) throws Exception {
            @SuppressWarnings("unchecked")
            ArrayList<Integer> l = (ArrayList<Integer>) obj;
            l.clear();
        };
    }, TXNCACHE_POOL_MAXIDLE);
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    private final Cache globalCache;
    private final Cache txnCache;
    private final IdentityHashMap<Long, List<Integer>> txnCacheXref = new IdentityHashMap<Long, List<Integer>>();
    
    /**
     * Constructor
     */
    public QueryCache(int globalBufferSize, int txnBufferSize) {
        this.globalCache = new Cache(globalBufferSize);
        this.txnCache = new Cache(txnBufferSize);
    }

    // ----------------------------------------------------------------------------
    // UTILITY CODE
    // ----------------------------------------------------------------------------
    
    protected int computeHashCode(int fragmentId, ParameterSet params) {
        return ((fragmentId * 31) + params.hashCode());
    }
    
    // ----------------------------------------------------------------------------
    // API
    // ----------------------------------------------------------------------------

    
    public void addGlobalQueryResult(int fragmentId, ParameterSet params, VoltTable result) {
        
    }
    
    /**
     * Store a new cache entry for a query that is specific to a transaction
     * This cached result is not be available to other transactions
     * @param txnId
     * @param fragmentId
     * @param params
     * @param result
     */
    @SuppressWarnings("unchecked")
    public void addTransactionQueryResult(Long txnId, int fragmentId, ParameterSet params, VoltTable result) {
        List<Integer> entries = this.txnCacheXref.get(txnId);
        if (entries == null) {
            try {
                entries = (List<Integer>)this.listPool.borrowObject();
            } catch (Exception ex) {
                throw new RuntimeException("Failed to initialize list from object pool", ex);
            }
        }
        
        CacheEntry entry = this.txnCache.getNext(txnId);
        entry.init(fragmentId, params, result);
        entries.add(entry.idx);
    }
    
    /**
     * This assumes that we only have own thread accessing the cache
     * @param txnId
     * @param fragmentId
     * @param params
     * @return
     */
    public VoltTable getTransactionCachedResult(Long txnId, int fragmentId, ParameterSet params) {
        List<Integer> entries = this.txnCacheXref.get(txnId);
        if (entries != null) {
            int paramsHash = -1;
            for (int i = 0, cnt = entries.size(); i < cnt; i++) {
                CacheEntry entry = this.txnCache.get(entries.get(i));
                assert(entry != null);
                
                // Check whether somebody took our place in the cache or that
                // we don't even have the same fragmentId
                if (entry.txnId.equals(txnId) == false ||
                    entry.fragmentId == fragmentId) {
                    continue;
                }
                
                // Then check whether our ParameterSet has the proper hashCode
                // We do this down here to allow us to only to compute the hashCode
                // unless we really have to.
                if (paramsHash == -1) paramsHash = params.hashCode();
                if (entry.paramsHash != paramsHash) {
                    continue;
                }
                
                // Bingo!
                entry.accessCounter++;
                return (entry.result);
            } // FOR
        }
        
        return (null);
    }
    
    /**
     * Remove all the cached query results that are specific for this transaction
     * @param txn_id
     */
    public void purgeTransaction(Long txnId) {
        List<Integer> entries = this.txnCacheXref.get(txnId);
        if (entries != null) {
            try {
                this.listPool.returnObject(entries);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to return list to object pool", ex);
            }
        }
    }
    
}
