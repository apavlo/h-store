package edu.brown.hstore.util;

import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;

public class QueryCache {

    private static class CacheEntry {
        int fragmentId;
        ParameterSet params;
        VoltTable result;
    }
    
    public QueryCache() {
        
    }
    
    public void addGlobalQueryResult(int fragmentId, ParameterSet params, VoltTable result) {
        
    }
    
    public void addTransactionQueryResult(Long txn_id, int fragmentId, ParameterSet params, VoltTable result) {
        
    }
    
    public VoltTable getTransactionCachedResult(Long txn_id, int fragmentId, ParameterSet params) {
        
        return (null);
    }
    
    /**
     * Remove all the cached query results that are specific for this transaction
     * @param txn_id
     */
    public void purgeTransaction(Long txn_id) {
        
    }
    
}
