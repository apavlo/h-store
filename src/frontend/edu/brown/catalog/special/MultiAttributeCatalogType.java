package edu.brown.catalog.special;

import java.util.Collection;

import org.voltdb.catalog.CatalogType;

public interface MultiAttributeCatalogType<T extends CatalogType> extends Iterable<T> {
    public T get(int idx);
    public boolean contains(T obj);
    public String getPrefix();
    public int size();

    /**
     * Return the attributes stored in this object
     */
    public Collection<T> getAttributes();
}
