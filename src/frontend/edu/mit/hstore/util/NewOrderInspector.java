package edu.mit.hstore.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.brown.hashing.AbstractHasher;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;

public class NewOrderInspector {
    private static final Logger LOG = Logger.getLogger(NewOrderInspector.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final HStoreSite hstore_site;
    private final AbstractHasher hasher;
    private final ParameterMangler mangler;
    
    /**
     * PartitionId -> PartitionId Singleton Sets
     */
    private final Map<Integer, Collection<Integer>> singlePartitionSets = new HashMap<Integer, Collection<Integer>>();
    
    /**
     * W_ID Short -> PartitionId
     */
    private final Map<Short, Integer> neworder_hack_hashes = new HashMap<Short, Integer>();
    
    /**
     * Constructor
     * @param hstore_site
     */
    public NewOrderInspector(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hasher = hstore_site.getHasher();
        this.mangler = hstore_site.getParameterMangler("neworder");
        assert(this.mangler != null);
        
        for (Integer p : this.hstore_site.getLocalPartitionIds()) {
            this.singlePartitionSets.put(p, Collections.singleton(p));
        } // FOR
    }
    
    /**
     * 
     * @param ts
     * @param args
     * @return
     */
    public Collection<Integer> initializeTransaction(Object args[]) {
        Object mangled[] = this.mangler.convert(args);
        
        if (debug.get())
            LOG.debug("Checking NewOrder input parameters:\n" + this.mangler.toString(mangled));
        
        final Short w_id = (Short)mangled[0];
        assert(w_id != null);
        short s_w_ids[] = (short[])args[5];
        
        Integer base_partition = this.neworder_hack_hashes.get(w_id);
        if (base_partition == null) {
            base_partition = this.hasher.hash(w_id);
            this.neworder_hack_hashes.put(w_id, base_partition);
        }
        assert(base_partition != null);
        
        Collection<Integer> touchedPartitions = this.singlePartitionSets.get(base_partition);
        assert(touchedPartitions != null) : "base_partition = " + base_partition;
        for (short s_w_id : s_w_ids) {
            if (s_w_id != w_id) {
                if (touchedPartitions.size() == 1) {
                    touchedPartitions = new HashSet<Integer>(touchedPartitions);
                }
                touchedPartitions.add(this.neworder_hack_hashes.get(s_w_id));
            }
        } // FOR
        if (debug.get())
            LOG.debug(String.format("NewOrder - Partitions=%s, W_ID=%d, S_W_IDS=%s",
                                    touchedPartitions, w_id, Arrays.toString(s_w_ids)));
        return (touchedPartitions);
    }
}
