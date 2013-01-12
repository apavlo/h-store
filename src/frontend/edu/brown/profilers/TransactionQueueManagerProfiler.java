package edu.brown.profilers;

import java.util.HashSet;
import java.util.Set;

import edu.brown.statistics.FastIntHistogram;

public class TransactionQueueManagerProfiler extends AbstractProfiler {
    
    public final FastIntHistogram concurrent_dtxn = new FastIntHistogram();
    public final Set<Long> concurrent_dtxn_ids = new HashSet<Long>();
    
    /**
     * The time spent checking the init queue for a partition.
     */
    public final ProfileMeasurement init_time = new ProfileMeasurement("INIT_QUEUE");
    
    /**
     * The time spent checking the lock queue for a partition.
     */
    public final ProfileMeasurement lock_time = new ProfileMeasurement("LOCK_QUEUE");
    
    /**
     * The time spent processing a transaction rejection
     */
    public final ProfileMeasurement rejection_time = new ProfileMeasurement("REJECTION");
    
    /**
     * The time spent checking the blocked queue for a partition.
     */
    public final ProfileMeasurement block_time = new ProfileMeasurement("BLOCK_QUEUE");
    
    /**
     * The time spent checking the blocked queue for a partition.
     */
    public final ProfileMeasurement restart_time = new ProfileMeasurement("RESTART_QUEUE");
    
    public void reset() {
        super.reset();
        this.concurrent_dtxn.clear();
        this.concurrent_dtxn_ids.clear();
    }
}
