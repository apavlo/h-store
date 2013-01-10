package edu.brown.profilers;

public class AntiCacheManagerProfiler extends AbstractProfiler {
    
    /**
     * The number of block evictions ocurred
     */
    public int num_evictions = 0;
    
    /**
     * The number of transactions that attempted to access evicted data.
     */
    public int evicted_access = 0;
    
    /**
     * The amount of time it takes for the AntiCacheManager to retrieve
     * an evicted block from disk.
     */
    public ProfileMeasurement eviction_time = new ProfileMeasurement("RETRIEVAL");
    
    /**
     * The amount of time it takes for the AntiCacheManager to merge an evicted 
     * block down in the EE.
     */
    public ProfileMeasurement merge_time = new ProfileMeasurement("MERGE");
    
    public void reset() {
        super.reset();
        this.num_evictions = 0;
        this.evicted_access = 0;
    }
}
