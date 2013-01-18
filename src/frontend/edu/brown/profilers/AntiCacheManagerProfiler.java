package edu.brown.profilers;

import java.util.Collection;
import java.util.TreeSet;

public class AntiCacheManagerProfiler extends AbstractProfiler {
    
    public static class EvictionHistory implements Comparable<EvictionHistory> {
        public final long startTimestamp;
        public final long stopTimestamp;
        public final long tuplesEvicted;
        public final long blocksEvicted;
        public final long bytesEvicted;
        
        public EvictionHistory(long startTimestamp, long stopTimestamp, long tuplesEvicted, long blocksEvicted, long bytesEvicted) {
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
     * The number of transactions that attempted to access evicted data.
     */
    public int restarted_txns = 0;
    
    /**
     * Eviction history
     */
    public Collection<EvictionHistory> eviction_history = new TreeSet<EvictionHistory>();
    
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
        this.restarted_txns = 0;
    }
}
