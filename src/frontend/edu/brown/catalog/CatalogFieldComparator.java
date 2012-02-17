package edu.brown.catalog;

import java.util.Comparator;

import org.voltdb.catalog.CatalogType;

/**
 * Comparator for CatalogTypes that examines one particular field
 */
public final class CatalogFieldComparator<T extends CatalogType> implements Comparator<T> {
    private final String field;

    public CatalogFieldComparator(String field) {
        this.field = field;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compare(T obj0, T obj1) {
        if (obj0 == null && obj1 == null)
            return (0);
        if (obj0 == null)
            return (1);
        if (obj1 == null)
            return (-1);

        Object val0 = obj0.getField(this.field);
        Object val1 = obj1.getField(this.field);
        if (val0 == null && val1 == null)
            return (0);
        if (val0 == null)
            return (1);
        if (val1 == null)
            return (1);

        return (val0 instanceof Comparable<?> ? ((Comparable) val0).compareTo(val1) : val0.toString().compareTo(val1.toString()));
    };
}