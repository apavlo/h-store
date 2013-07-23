package edu.brown.markov;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Special container for keeping track of transaction visitation times at MarkovVertexes
 * @author pavlo
 */
public class MarkovGraphTimes {
    private static final Logger LOG = Logger.getLogger(MarkovGraphTimes.class);
    private final static LoggerBoolean debug = new LoggerBoolean();
    private final static LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * The execution times of the transactions in the on-line run
     * A map of the xact_id to the time it took to get to this vertex
     */
    private final Map<MarkovVertex, Map<Long, Long>> instancetimes = new HashMap<MarkovVertex, Map<Long,Long>>();
    
    /**
     * Normalizes the times kept during online tallying of execution times.
     * TODO (svelagap): What about aborted transactions? Should they be counted in the normalization?
     */
    public void normalizeTimes(MarkovGraph graph) {
        Map<Long, Long> stoptimes = this.getInstanceTimes(graph.getCommitVertex());
        List<Long> to_remove = new ArrayList<Long>();
        for (MarkovVertex v : graph.getVertices()) {
            this.normalizeInstanceTimes(v, stoptimes, to_remove);
            to_remove.clear();
        } // FOR
    }
    
    /**
     * Add another instance time to the map. We use these times to figure out how long each
     * transaction takes to execute in the on-line model.
     */
    public MarkovVertex addInstanceTime(MarkovVertex v, Long xact_id, long time) {
        Map<Long, Long> m = this.instancetimes.get(v);
        if (m == null) {
            m = new HashMap<Long, Long>();
            this.instancetimes.put(v, m);
        }
        m.put(xact_id, time);
        return (v);
    }
    
    /**
     * Since we cannot know when a transaction ends in the on-line updates world, we wait until we find out
     * that we need to recompute, then we normalize all the vertices in every graph with the end times, to
     * get how long the xact actually lasted
     * @param end_times
     */
    protected void normalizeInstanceTimes(MarkovVertex v, Map<Long, Long> end_times, Collection<Long> to_remove) {
        Map<Long, Long> m = this.instancetimes.get(v);
        for (Entry<Long, Long> e : m.entrySet()) {
            Long stop = end_times.get(e.getKey());
            if (e.getValue() != null && stop != null) {
                long time = stop.longValue() - e.getValue().longValue();
                v.addExecutionTime(time);
                to_remove.add(e.getKey());
                if (trace.val) 
                    LOG.trace(String.format("Updating %s with %d time units from txn #%d", v, time, e.getKey())); 
            }
        } // FOR
        for (Long txn_id : to_remove) {
            end_times.remove(txn_id);
        } // FOR
    }
    
    /**
     * @return a map of xact_ids to times
     */
    public Map<Long,Long> getInstanceTimes(MarkovVertex v) {
        return this.instancetimes.get(v);
    }
}
