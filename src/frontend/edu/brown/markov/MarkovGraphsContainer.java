package edu.brown.markov;

import java.util.*;

import org.voltdb.catalog.Procedure;

/**
 * Convenience wrapper for a collection of Procedure-based MarkovGraphs that split on a unique id 
 * <SetId> -> <Procedure> -> <MarkovGraph> 
 * @author pavlo
 */
public class MarkovGraphsContainer extends HashMap<Integer, Map<Procedure, MarkovGraph>> {
    private static final long serialVersionUID = 1L;

    public MarkovGraph create(Integer id, Procedure catalog_proc) {
        MarkovGraph markov = this.get(id, catalog_proc);
        if (markov == null) {
            markov = new MarkovGraph(catalog_proc);
            this.put(id, markov);
        }
        return (markov);
    }
    
    public void put(Integer id, MarkovGraph markov) {
        if (!this.containsKey(id)) {
            this.put(id, new HashMap<Procedure, MarkovGraph>());
        }
        this.get(id).put(markov.getProcedure(), markov);
    }
    
    public MarkovGraph get(Integer id, Procedure catalog_proc) {
        if (this.containsKey(id)) {
            return (this.get(id).get(catalog_proc));
        }
        return (null);
    }
}
