package edu.brown.workload.filters;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Procedure;

import edu.brown.statistics.ObjectHistogram;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * This filter allows uniformly samples procedures from the workload 
 * @author pavlo
 */
public class SamplingFilter extends Filter {
    
    private static final Logger LOG = Logger.getLogger(SamplingFilter.class);
    
    /**
     * Procedure Name -> Total # of Txns Wanted
     */
    private final Map<String, Integer> proc_includes;
    private final ObjectHistogram<String> proc_histogram;
    
    private final Map<String, AtomicInteger> proc_counters = new HashMap<String, AtomicInteger>();
    private final Map<String, Integer> proc_rates = new HashMap<String, Integer>();
    
    /**
     * 
     * @param proc_includes
     * @param proc_histogram
     */
    public SamplingFilter(Map<String, Integer> proc_includes, ObjectHistogram<String> proc_histogram) {
        final boolean debug = LOG.isDebugEnabled();
        
        this.proc_includes = proc_includes;
        this.proc_histogram = proc_histogram;
        
        // For each procedure, figure out their sampling rate
        for (Object o : proc_histogram.values()) {
            String proc_name = null;
            if (o instanceof Procedure) {
                proc_name = ((Procedure)o).getName();
            } else {
                proc_name = (String)o;
            }
            Integer needed = this.proc_includes.get(proc_name);
            String debug_msg = "";
            if (needed != null) {
                long total = this.proc_histogram.get(proc_name, 0);
                int rate = (needed > 0 && needed < total ? Math.round(total / needed) : 1);
                if (needed == 0) rate = 0;
                this.proc_counters.put(proc_name, new AtomicInteger(0));
                this.proc_rates.put(proc_name, rate);
                if (debug) {
                    if (rate == 0) debug_msg = "NONE";
                    else debug_msg = (rate == 1 ? "ALL" : "Every " + rate) + 
                                     " [total=" + total + "]";
                }
            } else {
                if (debug) debug_msg = "SKIPPED";
            }
            if (debug) LOG.debug(String.format("%-20s %s", proc_name+":", debug_msg));
        } // FOR
    }

    @Override
    public String debugImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName())
          .append("[");
        
        String add = "";
        for (Entry<String, AtomicInteger> e : this.proc_counters.entrySet()) {
            int proc_total = this.proc_includes.get(e.getKey());
            sb.append(String.format("%s%s=%d/", add, e.getKey(), e.getValue().get()));
            sb.append(proc_total >= 0 ? proc_total : "UNLIMITED"); 
            add = ", ";
        } // FOR
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
                result = (proc_rate != 0 && proc_idx % proc_rate == 0 ? FilterResult.ALLOW : FilterResult.SKIP);
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
