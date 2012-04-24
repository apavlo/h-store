package edu.brown.catalog.special;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.NotImplementedException;

import edu.brown.catalog.CatalogFieldComparator;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class InnerMultiAttributeCatalogType<T extends CatalogType> extends CatalogType implements MultiAttributeCatalogType<T> {

    private static final Map<Class<? extends CatalogType>, CatalogFieldComparator<CatalogType>> COMPARATORS = new HashMap<Class<? extends CatalogType>, CatalogFieldComparator<CatalogType>>();
    private static final Map<Database, Map<Collection<? extends CatalogType>, MultiAttributeCatalogType<? extends CatalogType>>> SINGLETONS = new HashMap<Database, Map<Collection<? extends CatalogType>, MultiAttributeCatalogType<? extends CatalogType>>>();

    private final Class<? extends MultiAttributeCatalogType<T>> base_class;
    private final List<T> attributes = new ArrayList<T>();

    protected InnerMultiAttributeCatalogType(Class<? extends MultiAttributeCatalogType<T>> base_class, Collection<T> attributes) {
        this.base_class = base_class;
        this.attributes.addAll(attributes);
        assert (this.attributes.isEmpty() == false);
        assert (new HashSet<T>(this.attributes).size() == this.attributes.size()) : "Duplicate Attributes: " + this.attributes;

        CatalogType last_parent = null;
        for (T c : this.attributes) {
            if (last_parent != null)
                assert (c.getParent().equals(last_parent)) : "Catalog items do not have the same parent: " + CatalogUtil.debug(this.attributes);
            last_parent = c.getParent();
        } // FOR
    }

    /**
     * @param <T>
     * @param <U>
     * @param clazz
     * @param attrs
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected static <T extends CatalogType, U extends MultiAttributeCatalogType<T>> U get(Class<U> clazz, T... attrs) {
        List<T> attributes = (List<T>) CollectionUtil.addAll(new ArrayList<T>(), attrs);
        CatalogFieldComparator<T> comparator = (CatalogFieldComparator<T>) COMPARATORS.get(clazz);
        if (comparator == null) {
            comparator = new CatalogFieldComparator<T>("index");
            COMPARATORS.put((Class<? extends CatalogType>) clazz, (CatalogFieldComparator<CatalogType>) comparator);
        }
        // Collections.sort(attributes, comparator);
        for (int i = 0; i < attrs.length; i++) {
            assert (attrs[i] != null) : String.format("The catalog attribute at offset %d is null - %s", i, Arrays.toString(attrs));
        } // FOR

        Database catalog_db = CatalogUtil.getDatabase(attrs[0]);
        if (!SINGLETONS.containsKey(catalog_db)) {
            SINGLETONS.put(catalog_db, new HashMap<Collection<? extends CatalogType>, MultiAttributeCatalogType<? extends CatalogType>>());
        }
        U obj = (U) SINGLETONS.get(catalog_db).get(attributes);
        if (obj == null) {
            obj = (U) ClassUtil.newInstance(clazz, new Object[] { attributes }, new Class<?>[] { Collection.class });
            assert (obj != null) : "Invalid MultiAttributeCatalogType for " + attributes;
            SINGLETONS.get(catalog_db).put(attributes, obj);

            // Add the parameter object to the procedure's list
            if (obj instanceof MultiProcParameter) {
                Procedure catalog_proc = ((MultiProcParameter) obj).getParent();
                ((MultiProcParameter) obj).setIndex(catalog_proc.getParameters().size());
                catalog_proc.getParameters().add((MultiProcParameter) obj);
            }
        }

        return (obj);
    }

    // --------------------------------------------------------------------------------------------
    // CATALOG TYPE METHODS
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @Override
    public <U extends CatalogType> U getParent() {
        return (U) this.attributes.get(0).getParent();
    }

    @Override
    public Catalog getCatalog() {
        return this.attributes.get(0).getCatalog();
    }

    @Override
    public String getName() {
        return (this.getTypeName());
    }

    @Override
    public String getTypeName() {
        String names[] = new String[this.size()];
        for (int i = 0; i < names.length; i++) {
            names[i] = this.attributes.get(i).getName();
        }
        return ("<" + StringUtil.join(",", names) + ">");
    }

    @Override
    public int hashCode() {
        return this.attributes.hashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof InnerMultiAttributeCatalogType))
            return (false);
        return (this.attributes.equals(((InnerMultiAttributeCatalogType) obj).attributes));
    }

    public Collection<T> getAttributes() {
        return Collections.unmodifiableCollection(this.attributes);
    }

    
    public void update() {
        assert (false);
    }

    
    public String getPrefix() {
        return ("*" + this.base_class.getSimpleName() + "*");
    }

    // --------------------------------------------------------------------------------------------
    // COLLECTION METHODS
    // --------------------------------------------------------------------------------------------

    
    public Iterator<T> iterator() {
        return this.attributes.iterator();
    }

    
    public int size() {
        return (this.attributes.size());
    }

    
    public T get(int idx) {
        assert (idx < this.attributes.size()) : "Invalid offset '" + idx + "' for " + this;
        return ((T) this.attributes.get(idx));
    }

    
    public boolean contains(Object o) {
        return (this.attributes.contains(o));
    }

    
    public boolean containsAll(Collection<?> c) {
        return (this.attributes.containsAll(c));
    }

    
    public boolean isEmpty() {
        return (this.attributes.isEmpty());
    }

    
    public Object[] toArray() {
        return this.attributes.toArray();
    }

    @SuppressWarnings("hiding")
    
    public <T> T[] toArray(T[] a) {
        return this.attributes.toArray(a);
    }

    // --------------------------------------------------------------------------------------------
    // UNIMPLEMENTED
    // --------------------------------------------------------------------------------------------

    
    public boolean add(T e) {
        throw new NotImplementedException(this + " is a read-only Catalog object");
    }

    
    public boolean addAll(Collection<? extends T> c) {
        throw new NotImplementedException(this + " is a read-only Catalog object");
    }

    
    public void clear() {
        throw new NotImplementedException(this + " is a read-only Catalog object");
    }

    
    public boolean remove(Object o) {
        throw new NotImplementedException(this + " is a read-only Catalog object");
    }

    
    public boolean removeAll(Collection<?> c) {
        throw new NotImplementedException(this + " is a read-only Catalog object");
    }

    
    public boolean retainAll(Collection<?> c) {
        throw new NotImplementedException(this + " is a read-only Catalog object");
    }
}
