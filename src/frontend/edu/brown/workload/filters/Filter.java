package edu.brown.workload.filters;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ClassUtil;
import edu.brown.workload.AbstractTraceElement;

/**
 * WorkloadIterator Filter
 */
public abstract class Filter {
    public static final Logger LOG = Logger.getLogger(Filter.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private Filter next;
    
    public enum FilterResult {
        ALLOW,
        SKIP,
        HALT,
    };
    
    public Filter(Filter next) {
        this.next = next;
    }
    
    public Filter() {
        this(null);
    }
    
    /**
     * Returns an ordered list of the filters within the chain that are the same type as the
     * given search class (inclusive).
     * @param <T>
     * @param search
     * @return
     */
    public final <T extends Filter> List<T> getFilters(Class<? extends T> search) {
        return (this.getFilters(search, new ArrayList<T>()));
    }
    
    public final List<Filter> getFilters() {
        return (this.getFilters(Filter.class, new ArrayList<Filter>()));
    }
    
    @SuppressWarnings("unchecked")
    private final <T extends Filter> List<T> getFilters(Class<? extends T> search, List<T> found) {
        if (ClassUtil.getSuperClasses(this.getClass()).contains(search)) {
            found.add((T)this);
        }
        if (this.next != null) this.next.getFilters(search, found);
        return (found);
    }
    
    /**
     * Chain a Filter to be executed after this Filter 
     * @param next
     * @return This Filter
     */
    public final Filter attach(Filter next) {
        if (this.next != null) this.next.attach(next);
        else this.next = next;
        assert(this.next != null);
        return (this);
    }
    
    public Filter.FilterResult apply(AbstractTraceElement<? extends CatalogType> element) {
        Filter.FilterResult result = this.applyImpl(element);
        if (debug.val)
            LOG.debug("Filter: " + element + " => " + result);
        return (result);
    }
        
    private final Filter.FilterResult applyImpl(AbstractTraceElement<? extends CatalogType> element) {
        assert(element != null);
        Filter.FilterResult result = this.filter(element);
        if (trace.val && result != FilterResult.ALLOW)
            LOG.trace(String.format("%s Filter: %s => %s",
                      this.getClass().getSimpleName(), element, result));
        if (result == FilterResult.ALLOW && this.next != null) {
            result = this.next.applyImpl(element);
        }
        return (result);
    }
    
    public void reset() {
        this.resetImpl();
        if (this.next != null) this.next.reset();
        return;
    }
    
    protected abstract Filter.FilterResult filter(AbstractTraceElement<? extends CatalogType> element); 
    
    protected abstract void resetImpl();
    
    public abstract String debugImpl();
    
    public final String toString() {
        return (this.debugImpl() + (this.next != null ? "\n" + this.next.toString() : ""));
    }
}