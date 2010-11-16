package edu.brown.workload.filters;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Procedure;

import edu.brown.statistics.Histogram;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

/**
 * This filter allows uniformly samples procedures from the workload 
 * @author pavlo
 */
public class SamplingFilter extends Workload.Filter {
    
    private static final Logger LOG = Logger.getLogger(SamplingFilter.class);
    
    /**
     * Procedure Name -> Total # of Txns Wanted
     */
    private final Map<String, Integer> proc_includes;
    private final Histogram proc_histogram;
    
    private final Map<String, AtomicInteger> proc_counters = new HashMap<String, AtomicInteger>();
    private final Map<String, Integer> proc_rates = new HashMap<String, Integer>();
    
    /**
     * 
     * @param proc_includes
     * @param proc_histogram
     */
    public SamplingFilter(Map<String, Integer> proc_includes, Histogram proc_histogram) {
        this.proc_includes = proc_includes;
        this.proc_histogram = proc_histogram;
        
        // For each procedure, figure out their sampling rate
        Set<Procedure> procs = proc_histogram.values();
        for (Procedure catalog_proc : procs) {
            String proc_name = catalog_proc.getName();
            long needed = this.proc_includes.get(proc_name);
            long total = this.proc_histogram.get(catalog_proc, 0);
            int rate = Math.round(total / needed);
            this.proc_counters.put(proc_name, new AtomicInteger(0));
            this.proc_rates.put(proc_name, rate);
            LOG.debug(String.format("%-20s: Every %d [total=%d]", proc_name, rate, total));
        } // FOR
    }

    @Override
    protected String debug() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName())
          .append("[");
        
        String add = "";
        for (Entry<String, AtomicInteger> e : this.proc_counters.entrySet()) {
            int proc_total = this.proc_includes.get(e.getKey());
            sb.append(add).append(String.format("%s=(%d/%d)", e.getKey(), e.getValue().get(), proc_total));
            add = ", ";
        }
        sb.append("]");
        return (sb.toString());
    }

    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        FilterResult result = FilterResult.ALLOW;
        if (element instanceof TransactionTrace) {
            final boolean trace = LOG.isTraceEnabled();
            final String proc_name = element.getCatalogItemName();
            final AtomicInteger proc_counter = this.proc_counters.get(proc_name);
            
            if (proc_counter == null) {
                if (trace) LOG.trace("Procedure " + proc_name + " is not included in whitelist. Skipping...");
                result = FilterResult.SKIP;
            } else {
                int proc_idx = proc_counter.getAndIncrement();
                int proc_rate = this.proc_rates.get(proc_name);
                result = (proc_idx % proc_rate == 0 ? FilterResult.ALLOW : FilterResult.SKIP);
            }
        }
        return (result);
    }

    @Override
    protected void resetImpl() {
        for (AtomicInteger proc_counter : this.proc_counters.values()) {
            proc_counter.set(0);
        } // FOR
    }
}
