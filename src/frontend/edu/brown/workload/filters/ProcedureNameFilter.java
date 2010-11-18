package edu.brown.workload.filters;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

import edu.brown.workload.*;

/**
 * @author pavlo
 */
public class ProcedureNameFilter extends Workload.Filter {
    private static final Logger LOG = Logger.getLogger(ProcedureNameFilter.class);
    
    public static final String INCLUDE_ALL = "*";
    public static final Integer INCLUDE_UNLIMITED = -1;

    /**
     * Whitelist counters
     * When the finished set is the same size as the whitelist, then we will HALT
     */
    private final Map<String, AtomicInteger> whitelist = new HashMap<String, AtomicInteger>();
    private final Set<String> whitelist_finished = new HashSet<String>();
    private final Map<String, Integer> whitelist_orig_ctr = new HashMap<String, Integer>();
    
    /**
     * Blacklist
     */
    private final Set<String> blacklist = new HashSet<String>();
    
    /**
     * Add the given catalog object names to this filter's whitelist
     * @param names
     */
    public ProcedureNameFilter include(String...names) {
        for (String name : names) {
            this.include(name, INCLUDE_UNLIMITED);
        }
        return (this);
    }
    
    public ProcedureNameFilter include(String name, int limit) {
        this.whitelist.put(name, new AtomicInteger(limit));
        this.whitelist_orig_ctr.put(name, limit);
        return (this);
    }
    
    public Map<String, Integer> getProcIncludes() {
        return (Collections.unmodifiableMap(this.whitelist_orig_ctr));
    }
    
    /**
     * Add the given catalog object names to this filter's blacklist
     * @param names
     */
    public ProcedureNameFilter exclude(String...names) {
        for (String proc_name : names) {
            this.blacklist.add(proc_name);
            this.whitelist.remove(proc_name);
            this.whitelist_orig_ctr.remove(proc_name);
        } // FOR
        return (this);
    }
    
    @Override
    protected String debug() {
        return (this.getClass().getSimpleName() + "[include=" + this.whitelist + " exclude=" + this.blacklist + "]");
    }
    
    @Override
    protected void resetImpl() {
        for (Entry<String, AtomicInteger> e : this.whitelist.entrySet()) {
            assert(this.whitelist_orig_ctr.containsKey(e.getKey())) : "Missing '" + e.getKey() + "'";
            e.getValue().set(this.whitelist_orig_ctr.get(e.getKey()));
        } // FOR
        this.whitelist_finished.clear();
    }
    
    @Override
    public FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            final boolean trace = LOG.isTraceEnabled();
            final String name = element.getCatalogItemName();
            
            // BLACKLIST
            if (this.blacklist.contains(name)) {
                result = FilterResult.SKIP;
                
            // WHITELIST
            } else if (!this.whitelist.isEmpty()) {
                if (trace) LOG.trace("Checking whether " + name + " is in whitelist [total=" + this.whitelist.size() + ", finished=" + this.whitelist_finished.size() + "]");
                
                // If the HALT countdown is zero, then we know that we have all of the procs that
                // we want for this workload
                if (this.whitelist_finished.size() == this.whitelist.size()) {
                    result = FilterResult.HALT;
                    
                // Check whether this guy is allowed and keep count of how many we've added
                } else if (this.whitelist.containsKey(name)) {
                    int count = this.whitelist.get(name).getAndDecrement();
                    
                    // If the counter goes to zero, then we have seen enough of this procedure
                    // Reset its counter back to 1 so that the next time we see it it will always get skipped
                    if (count == 0) {
                        result = FilterResult.SKIP;
                        this.whitelist.get(name).set(0);
                        this.whitelist_finished.add(name);
                        if (trace) LOG.debug("Transaction '" + name + "' has exhausted count. Skipping [count=" + count + "]");
                    } else if (trace) {
                        LOG.debug("Transaction '" + name + "' is allowed [count=" + count + "]");
                    }
                // Not allowed. Just skip...
                } else {
                    result = FilterResult.SKIP;
                }
            }
        }
        return (result);
    }
}
