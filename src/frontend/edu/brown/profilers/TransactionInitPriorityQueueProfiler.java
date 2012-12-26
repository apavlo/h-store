package edu.brown.profilers;

import java.util.HashMap;
import java.util.Map;

import edu.brown.hstore.TransactionInitPriorityQueue.QueueState;

public class TransactionInitPriorityQueueProfiler extends AbstractProfiler {

    /**
     * The number of times that we spent in the different
     * states in our queue
     */
    public final Map<QueueState, ProfileMeasurement> state = new HashMap<QueueState, ProfileMeasurement>();
    
    {
        for (QueueState s : QueueState.values()) {
            this.state.put(s, new ProfileMeasurement(s.name()));
        } // FOR
    }
    
    @Override
    public void reset() {
        super.reset();
        for (ProfileMeasurement pm : this.state.values()) {
            pm.reset();
        }
    }

    
}