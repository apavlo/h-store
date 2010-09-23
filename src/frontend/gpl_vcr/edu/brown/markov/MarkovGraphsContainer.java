package edu.brown.markov;

import java.util.*;

import org.voltdb.catalog.Procedure;

public class MarkovGraphsContainer extends HashMap<Integer, Map<Procedure, MarkovGraph>> {
    private static final long serialVersionUID = 1L;

    public void put(Integer partition, MarkovGraph markov) {
        if (!this.containsKey(partition)) {
            this.put(partition, new HashMap<Procedure, MarkovGraph>());
        }
        this.get(partition).put(markov.getProcedure(), markov);
    }
    
    public MarkovGraph get(Object key, Procedure catalog_proc) {
        if (this.containsKey(key)) {
            return (this.get(key).get(catalog_proc));
        }
        return (null);
    }
}
