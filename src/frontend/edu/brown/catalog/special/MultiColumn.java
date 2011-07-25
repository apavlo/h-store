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
     * THIS SHOULD NOT BE CALLED DIRECTLY
     * Use MultiColumn.get()
     * @param attributes
     */
    @SuppressWarnings("unchecked")
    public MultiColumn(Collection<? extends Column> attributes) {
        this.inner = new InnerMultiAttributeCatalogType<Column>(MultiColumn.class, (Collection<Column>)attributes);
    }
    
    public static MultiColumn get(Column...cols) {
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
        return (Column)this.inner.get(idx);
    }
    @Override
    public boolean contains(Column c) {
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
        if (obj instanceof MultiColumn == false) return (false);
        return this.inner.equals(((MultiColumn)obj).inner);
    }
}
