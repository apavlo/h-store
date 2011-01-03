package edu.brown.markov;

import java.util.*;

import org.voltdb.catalog.Procedure;

/**
 * Convenience wrapper for a collection of Procedure-based MarkovGraphs that split by their base partitions 
 * <BasePartitionId> -> <Procedure> -> <MarkovGraph> 
 * @author pavlo
 */
public class MarkovGraphsContainer extends HashMap<Integer, Map<Procedure, MarkovGraph>> {
    private static final long serialVersionUID = 1L;

    public void put(Integer partition, MarkovGraph markov) {
        if (!this.containsKey(partition)) {
            this.put(partition, new HashMap<Procedure, MarkovGraph>());
        }
        this.get(partition).put(markov.getProcedure(), markov);
    }
    
    public MarkovGraph get(Integer partition, Procedure catalog_proc) {
        if (this.containsKey(partition)) {
            return (this.get(partition).get(catalog_proc));
        }
        return (null);
    }
}
