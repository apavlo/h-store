package edu.brown.workload.filters;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.CatalogType;

import edu.brown.utils.ClassUtil;
import edu.brown.workload.AbstractTraceElement;

/**
 * WorkloadIterator Filter
 */
public abstract class Filter {
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
    
    @SuppressWarnings("unchecked")
    private final <T extends Filter> List<T> getFilters(Class<? extends T> search, List<T> found) {
        if (ClassUtil.getSuperClasses(this.getClass()).contains(search)) {
            found.add((T)this);
        }
        if (this.next != null) this.next.getFilters(search, found);
        return (found);
    }
    
    public final Filter attach(Filter next) {
        if (this.next != null) this.next.attach(next);
        else this.next = next;
        assert(this.next != null);
        return (this);
    }
    
    public Filter.FilterResult apply(AbstractTraceElement<? extends CatalogType> element) {
        assert(element != null);
        Filter.FilterResult result = this.filter(element); 
        if (result == FilterResult.ALLOW) {
            return (this.next != null ? this.next.apply(element) : FilterResult.ALLOW);
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
    
    public abstract String debug();
    
    public final String toString() {
        return (this.debug() + (this.next != null ? "\n" + this.next.toString() : ""));
    }
}