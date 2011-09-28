package edu.brown.workload.filters;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.*;

import edu.brown.workload.*;

/**
 * @author pavlo
 */
public class ProcedureNameFilter extends Filter {
    private static final Logger LOG = Logger.getLogger(ProcedureNameFilter.class);
    
    public static final String INCLUDE_ALL = "*";
    public static final Integer INCLUDE_UNLIMITED = -1;

    /** If set to true, then we will increase includedTxn by the txn's weights */
    private final boolean weighted;
    
    /**
     * Whitelist counters
     * When includeFinished set is the same size as the includeTotals, then we will HALT
     */
    private final Map<String, AtomicInteger> includeCounters = new HashMap<String, AtomicInteger>();
    private final Set<String> includeFinished = new HashSet<String>();
    private final Map<String, Integer> includeTotals = new HashMap<String, Integer>();
    
    /**
     * Blacklist
     */
    private final Set<String> exclude = new HashSet<String>();
    
    public ProcedureNameFilter(boolean weighted) {
        super();
        this.weighted = weighted;
    }
    
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
    
    public ProcedureNameFilter include(Class<? extends VoltProcedure>...procClasses) {
        for (Class<? extends VoltProcedure> procClass : procClasses) {
            this.include(procClass.getSimpleName(), INCLUDE_UNLIMITED);
        }
        return (this);
    }
    
    public ProcedureNameFilter include(String name, int limit) {
        this.includeCounters.put(name, new AtomicInteger(limit));
        this.includeTotals.put(name, limit);
        return (this);
    }
    
    public Map<String, Integer> getProcIncludes() {
        return (Collections.unmodifiableMap(this.includeTotals));
    }
    
    /**
     * Return the set of procedure names that we want to include
     * @return
     */
    public Set<String> getProcedureNames() {
        return (this.includeCounters.keySet());
    }
    
    /**
     * Add the given catalog object names to this filter's blacklist
     * @param names
     */
    public ProcedureNameFilter exclude(String...names) {
        for (String proc_name : names) {
            this.exclude.add(proc_name);
            this.includeCounters.remove(proc_name);
            this.includeTotals.remove(proc_name);
        } // FOR
        return (this);
    }
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + "[include=" + this.includeCounters + " exclude=" + this.exclude + "]");
    }
    
    @Override
    protected void resetImpl() {
        for (Entry<String, AtomicInteger> e : this.includeCounters.entrySet()) {
            assert(this.includeTotals.containsKey(e.getKey())) : "Missing '" + e.getKey() + "'";
            e.getValue().set(this.includeTotals.get(e.getKey()));
        } // FOR
        this.includeFinished.clear();
    }
    
    @Override
    public FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            final boolean trace = LOG.isTraceEnabled();
            final String name = element.getCatalogItemName();
            
            // BLACKLIST
            if (this.exclude.contains(name)) {
                result = FilterResult.SKIP;
                
            // WHITELIST
            } else if (!this.includeCounters.isEmpty()) {
                if (trace) LOG.trace("Checking whether " + name + " is in whitelist [total=" + this.includeCounters.size() + ", finished=" + this.includeFinished.size() + "]");
                
                // If the HALT countdown is zero, then we know that we have all of the procs that
                // we want for this workload
                if (this.includeFinished.size() == this.includeCounters.size()) {
                    result = FilterResult.HALT;
                    
                // Check whether this guy is allowed and keep count of how many we've added
                } else if (this.includeCounters.containsKey(name)) {
                    int weight = (this.weighted ? element.getWeight() : 1);
                    int count = this.includeCounters.get(name).getAndAdd(-1*weight);
                    
                    // If the counter goes to zero, then we have seen enough of this procedure
                    // Reset its counter back to 1 so that the next time we see it it will always get skipped
                    if (count == 0) {
                        result = FilterResult.SKIP;
                        synchronized (this) {
                            this.includeCounters.get(name).set(0);
                            this.includeFinished.add(name);
                        } // SYNCH
                        if (trace) LOG.debug("Transaction '" + name + "' has exhausted count. Skipping...");
                    } else if (trace) {
                        LOG.debug("Transaction '" + name + "' is allowed [remaining=" + count + "]");
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
