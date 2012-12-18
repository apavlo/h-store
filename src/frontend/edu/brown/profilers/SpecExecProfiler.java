package edu.brown.profilers;

import edu.brown.statistics.FastIntHistogram;

public class SpecExecProfiler extends AbstractProfiler {
    
    /**
     * The total amount time spent in SpecExecScheduler.next()
     */
    public final ProfileMeasurement total_time = new ProfileMeasurement("TOTAL_TIME");
    
    /**
     * The amount of time spent performing the conflict calculations
     */
    public final ProfileMeasurement compute_time = new ProfileMeasurement("COMPUTE_TIME");
    
    /**
     * The current queue size when SpecExecScheduler.next() is invoked
     */
    public final FastIntHistogram queue_size = new FastIntHistogram(100);
    
    /**
     * The number of messages analyzed per invocation of SpecExecScheduler.next()
     */
    public final FastIntHistogram num_comparisons = new FastIntHistogram(100);
    
    /**
     * The number of times that the SpecExecScheduler successfully found
     * something to execute that didn't have conflicts.
     */
    public int success = 0;

    
    @Override
    public void reset() {
        super.reset();
        this.success = 0;
        this.num_comparisons.clear();
        this.queue_size.clear();
    }
    
}
