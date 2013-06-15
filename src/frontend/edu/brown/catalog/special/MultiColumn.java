package edu.brown.catalog.special;

import java.util.Collection;
import java.util.Iterator;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;

/**
 * @author pavlo
 */
public class MultiColumn extends Column implements MultiAttributeCatalogType<Column> {
    public static final String PREFIX = "*MultiColumn*";

    private final InnerMultiAttributeCatalogType<Column> inner;

    /**
     * <B>THIS SHOULD NOT BE CALLED DIRECTLY.</B>
     * Use MultiColumn.get()
     * @param attributes
     */
    @SuppressWarnings("unchecked")
    public MultiColumn(Collection<? extends Column> attributes) {
        this.inner = new InnerMultiAttributeCatalogType<Column>(MultiColumn.class, (Collection<Column>) attributes);
    }

    public static MultiColumn get(Column... cols) {
        return InnerMultiAttributeCatalogType.get(MultiColumn.class, cols);
    }

    @Override
    public Collection<Column> getAttributes() {
        return this.inner.getAttributes();
    }

    @Override
    public Iterator<Column> iterator() {
        return this.inner.iterator();
    }

    @Override
    public String getPrefix() {
        return (PREFIX);
    }

    @Override
    public int size() {
        return this.inner.size();
    }

    @Override
    public Column get(int idx) {
        return (Column) this.inner.get(idx);
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
        if (obj instanceof MultiColumn == false)
            return (false);
        return this.inner.equals(((MultiColumn) obj).inner);
    }

    @Override
    public boolean add(Column e) {
        return this.inner.add(e);
    }

    @Override
    public boolean addAll(Collection<? extends Column> c) {
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
