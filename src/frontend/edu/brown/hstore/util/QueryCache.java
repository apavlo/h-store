package edu.brown.hstore.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.utils.EstTime;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.FastObjectPool;
import edu.brown.utils.StringUtil;

public class QueryCache {
    private static final Logger LOG = Logger.getLogger(QueryCache.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
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
    
    private static class CacheEntry {
        final Integer idx;
        Long txnId;
        int fragmentId;
        int partitionId;
        int paramsHash;
        VoltTable result;
        int accessCounter = 0;
        long accessTimestamp = 0;
        
        public CacheEntry(int idx) {
            this.idx = Integer.valueOf(idx);
        }
        
        public void init(int fragmentId, int partitionId, int paramsHash, VoltTable result) {
            this.fragmentId = fragmentId;
            this.partitionId = partitionId;
            this.paramsHash = paramsHash;
            this.result = result;
        }
        
        @Override
        public String toString() {
            Class<?> confClass = this.getClass();
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            for (Field f : confClass.getDeclaredFields()) {
                Object obj = null;
                try {
                    obj = f.get(this);
                } catch (IllegalAccessException ex) {
                    throw new RuntimeException(ex);
                }
                if (obj instanceof VoltTable) {
                    obj = "{Rows: " + ((VoltTable)obj).getRowCount() + "}";
                }
//                System.err.printf("[%d] %s -> %s\n", this.hashCode(), f.getName(), obj);
                m.put(f.getName().toUpperCase(), obj);
            } // FOR
//            System.err.println();
            return (StringUtil.formatMaps(m));
        }
    } // CLASS
    
    /**
     * Simple circular buffer cache
     */
    private static class Cache {
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
     * TODO: Switch to a better object pool
     */
    private final FastObjectPool<ArrayList<Integer>> listPool = new FastObjectPool<ArrayList<Integer>>(new BasePoolableObjectFactory() {
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
    
    /**
     * TransactionId -> List of CacheEntry Offsets
     */
    private final Map<Long, List<Integer>> txnCacheXref = new HashMap<Long, List<Integer>>();
    
    /**
     * Constructor
     */
    public QueryCache(int globalBufferSize, int txnBufferSize) {
        this.globalCache = new Cache(globalBufferSize);
        this.txnCache = new Cache(txnBufferSize);
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
    public void addResult(Long txnId, int fragmentId, int partitionId, ParameterSet params, VoltTable result) {
        if (debug.val)
            LOG.debug(String.format("#%d - Storing query result for FragmentId %d - %s",
                                    txnId, fragmentId, params));
        this.addResult(txnId, fragmentId, partitionId, params.hashCode(), result);
    }
    
    /**
     * Store a new cache entry for a query that is specific to a transaction
     * This cached result is not be available to other transactions
     * @param txnId
     * @param fragmentId
     * @param partitionId
     * @param paramsHash
     * @param result
     */
    public void addResult(Long txnId, int fragmentId, int partitionId, int paramsHash, VoltTable result) {
        if (debug.val)
            LOG.debug(String.format("#%d - Storing query result for FragmentId %d / paramsHash:%d",
                                    txnId, fragmentId, paramsHash));
        
        List<Integer> entries = this.txnCacheXref.get(txnId);
        if (entries == null) {
            synchronized (this) {
                entries = this.txnCacheXref.get(txnId);
                if (entries == null) {
                    try {
                        entries = this.listPool.borrowObject();
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to initialize list from object pool", ex);
                    }
                    this.txnCacheXref.put(txnId, entries);
                }
            } // SYNCH
        }
        
        CacheEntry entry = this.txnCache.getNext(txnId);
        assert(entry != null);
        entry.init(fragmentId, partitionId, paramsHash, result);
        entries.add(entry.idx);
        if (debug.val) LOG.debug(String.format("#%d - CacheEntry\n%s", txnId, entry.toString()));
    }
    
    /**
     * This assumes that we only have own thread accessing the cache
     * @param txnId
     * @param fragmentId
     * @param params
     * @return
     */
    public VoltTable getResult(Long txnId, int fragmentId, int partitionId, ParameterSet params) {
        
        if (debug.val) LOG.debug(String.format("#%d - Retrieving query cache for FragmentId %d - %s",
                                                 txnId, fragmentId, params));
        
        List<Integer> entries = this.txnCacheXref.get(txnId);
        if (entries != null) {
            if (trace.val) LOG.trace(String.format("Txn #%d has %d cache entries", txnId, entries.size()));
            int paramsHash = -1;
            for (int i = 0, cnt = entries.size(); i < cnt; i++) {
                CacheEntry entry = this.txnCache.get(entries.get(i));
                assert(entry != null);
                
                // Check whether somebody took our place in the cache or that
                // we don't even have the same fragmentId or partitionId
                if (entry.txnId.equals(txnId) == false ||
                    entry.fragmentId != fragmentId ||
                    entry.partitionId != partitionId) {
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
                entry.accessTimestamp = EstTime.currentTimeMillis();
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


    // ----------------------------------------------------------------------------
    // UTILITY CODE
    // ----------------------------------------------------------------------------
    
    @Override
    public String toString() {
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, Object> m[] = (LinkedHashMap<String, Object>[])new LinkedHashMap<?,?>[2];
        int idx = 0;
        
        // Global Cache
        m[idx] = new LinkedHashMap<String, Object>();
        m[idx].put("Global Cache", null);
        
        // TxnCache
        m[++idx] = new LinkedHashMap<String, Object>();
        List<CacheEntry> entries = new ArrayList<CacheEntry>();
        for (CacheEntry entry : this.txnCache.buffer) {
            if (entry.txnId != null) entries.add(entry);
        } // FOR
        m[idx].put(String.format("TxnCache[%d]", entries.size()),
                   StringUtil.join("\n", entries).trim());
        m[idx].put("Current Transactions", this.txnCacheXref);
        
        return StringUtil.formatMaps(m);
    }
}
