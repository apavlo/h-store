package edu.brown.profilers;

import java.util.HashSet;
import java.util.Set;

import edu.brown.statistics.FastIntHistogram;

public class TransactionQueueManagerProfiler extends AbstractProfiler {
    
    public final FastIntHistogram concurrent_dtxn;
    public final Set<Long> concurrent_dtxn_ids = new HashSet<Long>();
    
    public final ProfileMeasurement idle = new ProfileMeasurement("IDLE");
    
    public final ProfileMeasurement lock_queue = new ProfileMeasurement("LOCK_QUEUE");
    
    public final ProfileMeasurement init_queue = new ProfileMeasurement("INIT_QUEUE");
    
    public final ProfileMeasurement block_queue = new ProfileMeasurement("BLOCK_QUEUE");
    
    public final ProfileMeasurement restart_queue = new ProfileMeasurement("RESTART_QUEUE");
    
    public TransactionQueueManagerProfiler(int num_partitions) {
        this.concurrent_dtxn = new FastIntHistogram(num_partitions);
    }
}
