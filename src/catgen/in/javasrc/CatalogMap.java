/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import edu.brown.catalog.CatalogUtil;

/**
 * A safe interface to a generic map of CatalogType instances. It is safe
 * because it is mostly read-only. All operations that modify the map are
 * either non-public, or they are convenience methods which generate
 * catalog commands and execute them on the root Catalog instance. By
 * generating commands, transactional safety is easier to assure.
 *
 * @param <T> The subclass of CatalogType that this map will contain.
 */
public final class CatalogMap<T extends CatalogType> implements Iterable<T>, Collection<T> {

    TreeMap<String, T> m_items = new TreeMap<String, T>();
    T m_fastArray[];
    Class<T> m_cls;
    Catalog m_catalog;
    CatalogType m_parent;
    String m_path;
    int m_subTreeVersion;

    CatalogMap(Catalog catalog, CatalogType parent, String path, Class<T> cls) {
        this.m_catalog = catalog;
        this.m_parent = parent;
        this.m_path = path;
        this.m_cls = cls;
        this.m_subTreeVersion = catalog.m_currentCatalogVersion;
    }

    public Class<T> getGenericClass() {
        return (m_cls);
    }
    
    public Set<String> keySet() {
        return (this.m_items.keySet());
    }
    
    @Override
    public <X> X[] toArray(X[] a) {
        return (m_items.values().toArray(a));
    }
    
    @Override
    public Object[] toArray() {
        return (m_items.values().toArray());
    }
    
    /**
     * Get an item from the map by name
     * @param name The name of the requested CatalogType instance in the map
     * @return The item found in the map, or null if not found
     */
    public T get(String name) {
        return m_items.get(name);
    }

    /**
     * Get an item from the map by name, ignoring case
     * @param name The name of the requested CatalogType instance in the map
     * @return The item found in the map, or null if not found
     */
    public T getIgnoreCase(String name) {
        T t = m_items.get(name);
        if (t == null) {
            for (Entry<String, T> e : m_items.entrySet()) {
                if (e.getKey().equalsIgnoreCase(name)) {
                    t = e.getValue();
                    break;
                }
            } // FOR
        }
        return (t);
    }

    /**
     * How many items are in the map?
     * @return The number of items in the map
     */
    public int size() {
        return m_items.size();
    }

    /**
     * Is the map empty?
     * @return A boolean indicating whether the map is empty
     */
    public boolean isEmpty() {
        return (m_items.size() == 0);
    }

    /**
     * Get an iterator for the items in the map
     * @return The iterator for the items in the map
     */
    public Iterator<T> iterator() {
        return m_items.values().iterator();
    }
    
//    public T get(int index) {
//        // HACK
//        index++;
//        int i = 0;
//        T ret = null;
//        for (T t : m_items.values()) {
//            System.err.println("[" + (i++) + "] " + t + " (index=" + t.m_relativeIndex + ")");
//            if (t.m_relativeIndex == index) ret = t; // return (t);
//        } // FOR
//        return (ret);
//    }
    
    public T get(String field, Object value) {
        T ret = null;
        for (T t : m_items.values()) {
            assert(t.m_fields.containsKey(field)) : t.getClass() + " does not contain field '" + field + "'";
            if (t.getField(field).equals(value)) {
                ret = t;
                break;
            }
        } // FOR
        return (ret);
    }
    
    public T get(int index) {
        return (this.get("index", index));
    }
    
    /**
     * Return an array of the values in this CatalogMap.
     * This will be generally faster than using an iterator because
     * we will cache the array locally
     * @return
     */
    @SuppressWarnings("unchecked")
    public T[] values() {
        if (m_fastArray == null) {
            synchronized (this) {
                if (m_fastArray == null) {
                    int capacity = this.size();
                    T arr[] = (T[])Array.newInstance(this.m_cls, capacity);
                    int i = 0;
                    for (T t : m_items.values()) {
                        arr[i++] = t;
                    } // FOR
                    m_fastArray = arr;
                }
            } // SYNCH
        }
        return m_fastArray;
    }

    public int getSubTreeVersion() {
        return m_subTreeVersion;
    }

    /**
     * Create a new instance of a CatalogType as a child of this map with a
     * given name. Note: this just makes a catalog command and calls
     * catalog.execute(..).
     * @param name The name of the new instance to create, the thing to add
     * @return The newly created CatalogType instance
     */
    public T add(String name) {
        try {
            if (m_items.containsKey(name))
                throw new CatalogException("Catalog item '" + name + "' already exists for " + m_parent);

            T x = m_cls.newInstance();
            String childPath = m_path + "[" + name + "]";
            x.setBaseValues(m_catalog, m_parent, childPath, name);
            x.m_parentMap = this;

            m_items.put(name, x);

            // update versioning if needed
            updateVersioning();

            // assign a relative index to every child item
            int index = 1;
            for (Entry<String, T> e : m_items.entrySet()) {
                e.getValue().m_relativeIndex = index++;
            }

            m_fastArray = null;
            return x;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public boolean add(T x) {
        return this.add(x, true);
    }
        
    /**
     * Add a CatalogType object into this CatalogMap
     * If initialize is set to true, then this will invoke CatalogType.setBaseValues() 
     * @param x
     * @param initialize
     * @return
     */
    public boolean add(T x, boolean initialize) {
        String name = x.getName();
        if (m_items.containsKey(name))
            throw new CatalogException("Catalog item '" + name + "' already exists for " + m_parent);
        
        String childPath = m_path + "[" + name + "]";
        if (initialize) x.setBaseValues(m_catalog, m_parent, childPath, name);
        x.m_parentMap = this;

        m_items.put(name, x);

        // update versioning if needed
        updateVersioning();

        // assign a relative index to every child item
        int index = 1;
        for (Entry<String, T> e : m_items.entrySet()) {
            e.getValue().m_relativeIndex = index++;
        }
        
        m_fastArray = null;
        return (true);
    }

    /**
     * Remove a {@link CatalogType} object from this collection.
     * @param name The name of the object to remove.
     */
    public boolean delete(String name) {
        try {
            if (m_items.containsKey(name) == false)
                throw new CatalogException("Catalog item '" + name + "' doesn't exists in " + m_parent);

            m_items.remove(name);

            // update versioning if needed
            updateVersioning();

            // assign a relative index to every child item
            int index = 1;
            for (Entry<String, T> e : m_items.entrySet()) {
                e.getValue().m_relativeIndex = index++;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        m_fastArray = null;
        return (true);
    }

    void updateVersioning() {
        if (m_subTreeVersion != m_catalog.m_currentCatalogVersion) {
            m_subTreeVersion = m_catalog.m_currentCatalogVersion;
            m_parent.updateSubTreeVersion();
        }
    }

    void writeCommandsForMembers(StringBuilder sb) {
        for (T type : this) {
            type.writeCreationCommand(sb);
            type.writeFieldCommands(sb);
            type.writeChildCommands(sb);
        }
    }

    @SuppressWarnings("unchecked")
    void copyFrom(CatalogMap<? extends CatalogType> catalogMap) {
        CatalogMap<T> castedMap = (CatalogMap<T>) catalogMap;
        for (Entry<String, T> e : castedMap.m_items.entrySet()) {
            m_items.put(e.getKey(), (T) e.getValue().deepCopy(m_catalog, m_parent));
        }
        m_subTreeVersion = catalogMap.m_subTreeVersion;
    }

    @Override
    public boolean equals(Object obj) {
        // returning false if null isn't the convention, oh well
        if (obj == null)
            return false;
        if (obj.getClass() != getClass())
            return false;

        @SuppressWarnings("unchecked")
        CatalogMap<T> other = (CatalogMap<T>) obj;

        if (other.size() != size())
            return false;

        for (Entry<String, T> e : m_items.entrySet()) {
            assert(e.getValue() != null);
            T type = other.get(e.getKey());
            if (type == null)
                return false;
            if (type.equals(e.getValue()) == false)
                return false;
        }

        return true;
    }
    
    @Override
    public String toString() {
        return CatalogUtil.debug(this);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        boolean ret = true;
        for (T t : c) {
            ret = this.add(t) || ret;
        }
        return (ret);
    }

    @Override
    public void clear() {
        m_fastArray = null;
        this.m_items.clear();
    }

    public boolean containsKey(String key) {
        return (this.m_items.containsKey(key));
    }
    
    @Override
    public boolean contains(Object o) {
        return (this.m_items.values().contains(o));
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return (this.m_items.values().containsAll(c));
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof CatalogType) {
            return this.delete(((CatalogType)o).getName());
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = true;
        for (Object o : c) {
            ret = this.remove(o) || ret;
        }
        return (ret);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }
}
