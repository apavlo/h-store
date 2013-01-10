package edu.brown.profilers;

import java.util.LinkedHashMap;
import java.util.Map;

import edu.brown.hstore.PartitionLockQueue.QueueState;
import edu.brown.statistics.FastIntHistogram;

public class PartitionLockQueueProfiler extends AbstractProfiler {

    /**
     * The amount of time that a txn has to spend waiting in milliseconds
     */
    public final FastIntHistogram waitTimes = new FastIntHistogram();
    
    /**
     * The number of times that we spent in the different
     * states in our queue
     */
    public final Map<QueueState, ProfileMeasurement> queueStates = new LinkedHashMap<QueueState, ProfileMeasurement>();
    
    {
        for (QueueState s : QueueState.values()) {
            this.queueStates.put(s, new ProfileMeasurement(s.name().toUpperCase()));
        } // FOR
    }
    
    @Override
    public void reset() {
        super.reset();
        this.waitTimes.clear();
        for (ProfileMeasurement pm : this.queueStates.values()) {
            pm.reset();
        } // FOR
    }

    
}