package edu.brown.profilers;

import java.util.LinkedHashMap;
import java.util.Map;

import edu.brown.hstore.TransactionInitPriorityQueue.QueueState;
import edu.brown.statistics.FastIntHistogram;

public class TransactionInitPriorityQueueProfiler extends AbstractProfiler {

    /**
     * The amount of time that a txn has to spend waiting
     */
    public final FastIntHistogram waitTimes = new FastIntHistogram();
    
    
    /**
     * The number of times that we spent in the different
     * states in our queue
     */
    public final Map<QueueState, ProfileMeasurement> queueStates = new LinkedHashMap<QueueState, ProfileMeasurement>();
    
    {
        for (QueueState s : QueueState.values()) {
            this.queueStates.put(s, new ProfileMeasurement(s.name()));
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