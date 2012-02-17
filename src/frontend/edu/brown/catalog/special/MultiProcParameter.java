package edu.brown.catalog.special;

import java.util.Collection;
import java.util.Iterator;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.ProcParameter;

/**
 * @author pavlo
 */
public class MultiProcParameter extends SpecialProcParameter implements MultiAttributeCatalogType<ProcParameter> {
    public static final String PREFIX = "*MultiProcParameter*";

    private final InnerMultiAttributeCatalogType<ProcParameter> inner;

    /**
     * THIS SHOULD NOT BE CALLED DIRECTLY Use MultiProcParameter.get()
     * 
     * @param attributes
     */
    public MultiProcParameter(Collection<ProcParameter> attributes) {
        this.inner = new InnerMultiAttributeCatalogType<ProcParameter>(MultiProcParameter.class, attributes);
    }

    public static MultiProcParameter get(ProcParameter... cols) {
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
        return (ProcParameter) this.inner.get(idx);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U extends CatalogType> U getParent() {
        return (U) this.inner.getParent();
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
        if (obj instanceof MultiProcParameter == false)
            return (false);
        return this.inner.equals(((MultiProcParameter) obj).inner);
    }

    @Override
    public boolean add(ProcParameter e) {
        return this.inner.add(e);
    }

    @Override
    public boolean addAll(Collection<? extends ProcParameter> c) {
        return this.inner.addAll(c);
    }

    @Override
    public void clear() {
        this.inner.clear();
    }

    @Override
    public boolean contains(Object o) {
        return this.inner.contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.inner.containsAll(c);
    }

    @Override
    public boolean isEmpty() {
        return this.inner.isEmpty();
    }

    @Override
    public boolean remove(Object o) {
        return this.inner.remove(o);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.inner.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.inner.retainAll(c);
    }

    @Override
    public Object[] toArray() {
        return this.inner.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.inner.toArray(a);
    }
}