package edu.brown.profilers;

import java.util.Collection;
import java.util.TreeSet;

import org.voltdb.exceptions.EvictedTupleAccessException;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.txns.LocalTransaction;

/**
 * Anti-Cache Profiler Information
 * There should be one of these per partition.
 * @author pavlo
 */
public class AntiCacheManagerProfiler extends AbstractProfiler {
    
    /**
     * Eviction History
     */
    public static class EvictionHistory implements Comparable<EvictionHistory> {
        public final long startTimestamp;
        public final long stopTimestamp;
        public final long tuplesEvicted;
        public final long blocksEvicted;
        public final long bytesEvicted;
        
        public EvictionHistory(long startTimestamp,
                               long stopTimestamp,
                               long tuplesEvicted,
                               long blocksEvicted,
                               long bytesEvicted) {
            this.startTimestamp = startTimestamp;
            this.stopTimestamp = stopTimestamp;
            this.tuplesEvicted = tuplesEvicted;
            this.blocksEvicted = blocksEvicted;
            this.bytesEvicted = bytesEvicted;
        }
        @Override
        public int compareTo(EvictionHistory other) {
            if (this.startTimestamp != other.startTimestamp) {
                return (int)(this.startTimestamp - other.startTimestamp);
            } else if (this.stopTimestamp != other.stopTimestamp) {
                return (int)(this.stopTimestamp - other.stopTimestamp);
            } else if (this.tuplesEvicted != other.tuplesEvicted) {
                return (int)(this.tuplesEvicted - other.tuplesEvicted);
            } else if (this.blocksEvicted != other.blocksEvicted) {
                return (int)(this.blocksEvicted - other.blocksEvicted);
            } else if (this.bytesEvicted != other.bytesEvicted) {
                return (int)(this.bytesEvicted - other.bytesEvicted);
            }
            return (0);
        }
    };
    
    /**
     * Transaction Evicted Tuple Access History
     */
    public static class AccessHistory implements Comparable<AccessHistory> {
        public final long startTimestamp;
        public final Long txnId;
        public final int procId;
        public final int numTuples;
        public final int numBlocks;
        public final int numTables;
        public final int restarts;
        
        public AccessHistory(LocalTransaction ts, EvictedTupleAccessException ex) {
            this.startTimestamp = EstTime.currentTimeMillis();
            this.txnId = ts.getTransactionId();
            this.procId = ts.getProcedure().getId();
            this.restarts = ts.getRestartCounter();
            
            this.numBlocks = ex.getBlockIds().length;
            this.numTuples = ex.getTupleOffsets().length;
            this.numTables = 1; // FIXME
        }
        @Override
        public int compareTo(AccessHistory other) {
            if (this.startTimestamp != other.startTimestamp) {
                return (int)(this.startTimestamp - other.startTimestamp);
            }
            return (this.txnId.compareTo(other.txnId));
        }
    }
    
    /**
     * The number of transactions that attempted to access evicted data.
     */
    public int restarted_txns = 0;
    
    /**
     * Eviction history
     */
    public Collection<EvictionHistory> eviction_history = new TreeSet<EvictionHistory>();

    /**
     * Evicted Tuple Access History
     */
    public Collection<AccessHistory> evictedaccess_history = new TreeSet<AccessHistory>();
    
    /**
     * The amount of time it takes for the AntiCacheManager to evict a block
     * of tuples from this partition
     */
    public ProfileMeasurement eviction_time = new ProfileMeasurement("EVICTION");
    
    /**
     * The amount of time it takes for the AntiCacheManager to retrieve
     * an evicted block from disk.
     */
    public ProfileMeasurement retrieval_time = new ProfileMeasurement("RETRIEVAL");
    
    /**
     * The amount of time it takes for the AntiCacheManager to merge an evicted 
     * block down in the EE.
     */
    public ProfileMeasurement merge_time = new ProfileMeasurement("MERGE");
    
    public void reset() {
        super.reset();
        this.eviction_history.clear();
        this.evictedaccess_history.clear();
        this.restarted_txns = 0;
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public void addEvictedAccess(LocalTransaction ts, EvictedTupleAccessException ex) {
        AccessHistory eah = new AccessHistory(ts, ex);
        this.evictedaccess_history.add(eah);
    }
}
