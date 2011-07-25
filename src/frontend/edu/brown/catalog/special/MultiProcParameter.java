package edu.brown.catalog.special;

import java.util.Collection;
import java.util.Iterator;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.ProcParameter;

/**
 * @author pavlo
 */
public class MultiProcParameter extends ProcParameter implements MultiAttributeCatalogType<ProcParameter> {
    public static final String PREFIX = "*MultiProcParameter*"; 

    private final InnerMultiAttributeCatalogType<ProcParameter> inner;
    
    /**
     * THIS SHOULD NOT BE CALLED DIRECTLY
     * Use MultiProcParameter.get()
     * @param attributes
     */
    public MultiProcParameter(Collection<ProcParameter> attributes) {
        this.inner = new InnerMultiAttributeCatalogType<ProcParameter>(MultiProcParameter.class, attributes);
    }
    
    public static MultiProcParameter get(ProcParameter...cols) {
        MultiProcParameter obj = InnerMultiAttributeCatalogType.get(MultiProcParameter.class, cols);
        return obj;
    }
    
    @Override
    public Collection<ProcParameter> getAttributes() {
        return this.inner.getAttributes();
    }
    @Override
    public Iterator<ProcParameter> iterator() {
        return this.inner.iterator();
    }
    @Override
    public String getPrefix() {
        return PREFIX;
    }
    @Override
    public int size() {
        return this.inner.size();
    }
    @Override
    public ProcParameter get(int idx) {
        return (ProcParameter)this.inner.get(idx);
    }
    @Override
    public boolean contains(ProcParameter c) {
        return this.inner.contains(c);
    }
    @SuppressWarnings("unchecked")
    @Override
    public <U extends CatalogType> U getParent() {
        return (U)this.inner.getParent();
    }
    @Override
    public Catalog getCatalog() {
        return this.inner.getCatalog();
    }
    @Override
    public String getName() {
        return this.inner.getTypeName();
    }
    @Override
    public String getTypeName() {
        return this.inner.getTypeName();
    }
    @Override
    public int hashCode() {
        return this.inner.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MultiProcParameter == false) return (false);
        return this.inner.equals(((MultiProcParameter)obj).inner);
    }
}