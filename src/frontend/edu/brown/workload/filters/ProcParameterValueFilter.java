package edu.brown.workload.filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.ProcParameter;

import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * Filters out transactions based on the whether a ProcParameter equals a specific value
 * @author pavlo
 */
public class ProcParameterValueFilter extends Filter {
    private static final Logger LOG = Logger.getLogger(ProcParameterValueFilter.class);
    private static final boolean d = LOG.isDebugEnabled();
    
    private final Map<Integer, Set<Object>> values = new HashMap<Integer, Set<Object>>(); 
    
    /**
     * Add a parameter index and value equality pair
     * @param param_idx
     * @param param_value
     * @return
     */
    public ProcParameterValueFilter include(int param_idx, Object param_value) {
        Set<Object> values = this.values.get(param_idx);
        if (values == null) {
            values = new HashSet<Object>();
            this.values.put(param_idx, values);
        }
        values.add(param_value);
        return (this);
    }
    
    /**
     * Add a ProcParameter and value equality pair
     * @param catalog_param
     * @param param_value
     * @return
     */
    public ProcParameterValueFilter include(ProcParameter catalog_param, Object param_value) {
        return (this.include(catalog_param.getIndex(), param_value));
    }
    
    @Override
    protected void resetImpl() {
        // Ignore...
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        if (element instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)element;
            boolean allow = true;
            for (Entry<Integer, Set<Object>> e : this.values.entrySet()) {
                Object param_val = xact.getParam(e.getKey());
                
                // The parameter must equal at least one of these values
                for (Object val : e.getValue()) {
                    allow = val.equals(param_val); 
                    if (d) LOG.debug(String.format("%s #%02d => [%s (%s) <-> %s (%s)] = %s",
                                     xact, e.getKey(),
                                     val, val.getClass().getSimpleName(),
                                     param_val, param_val.getClass().getSimpleName(),
                                     (allow ? FilterResult.ALLOW : FilterResult.SKIP)));
                    if (allow) break;
                } // FOR
                
                // If none of them are equal, then we need to halt here
                if (allow == false) break;
            } // FOR
            return (allow ? FilterResult.ALLOW : FilterResult.SKIP);
        }
        return FilterResult.ALLOW;
    }
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + "[values=" + this.values + "]");
    }
}