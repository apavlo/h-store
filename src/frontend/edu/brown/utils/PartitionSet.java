/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.utils;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.utils.NotImplementedException;

import edu.brown.hstore.HStoreConstants;

/**
 * Container class that represents a list of partitionIds
 * For now it's just a HashSet
 * @author pavlo
 */
public class PartitionSet implements Collection<Integer>, JSONSerializable {
    
    // private final Set<Integer> inner = new HashSet<Integer>();
    private final BitSet inner = new BitSet();
    private boolean contains_null = false;

    public PartitionSet() {
        // Nothing...
    }
    
    public PartitionSet(Collection<Integer> partitions) {
        for (Integer p : partitions)
            this.add(p);
    }
    
    public PartitionSet(Integer...partitions) {
        for (Integer p : partitions)
            this.add(p);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PartitionSet) {
            return this.inner.equals(((PartitionSet)obj).inner);
        }
        else if (obj instanceof Collection<?>) {
            Collection<?> other = (Collection<?>)obj;
            if (this.inner.size() != other.size()) return (false);
            return (this.containsAll(other));
        }
        return (false);
    }
    @Override
    public int hashCode() {
        return this.inner.hashCode();
    }
    @Override
    public String toString() {
        String s = this.inner.toString(); 
        // HACK
        if (this.contains_null) {
            s = s.substring(0, 1) +
                HStoreConstants.NULL_PARTITION_ID + ", " +
                s.substring(1);
        }
        return (s);
    }
    @Override
    public int size() {
        return (this.contains_null ? 1 : 0) + this.inner.cardinality();
    }
    @Override
    public void clear() {
        this.contains_null = false;
        this.inner.clear();
    }
    @Override
    public boolean isEmpty() {
        return (this.contains_null == false && this.inner.isEmpty());
    }
    @Override
    public boolean contains(Object o) {
        if (o instanceof Integer) {
            Integer p = (Integer)o;
            return this.contains(p.intValue());
        }
        return (false);
    }
    public boolean contains(Integer p) {
        return this.contains(p.intValue());
    }
    public boolean contains(int p) {
        if (p == HStoreConstants.NULL_PARTITION_ID) {
            return (this.contains_null);
        }
        return (this.inner.get(p));
    }
    @Override
    public Iterator<Integer> iterator() {
        return new Itr();
    }
    @Override
    public Object[] toArray() {
        int length = this.inner.cardinality();
        Object arr[] = new Object[length];
        int idx = 0;
        for (Integer p : this) {
            arr[idx++] = p;
        }
        return (arr);
    }
    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] a) {
        int length = this.inner.cardinality();
        if (a.length != length) {
            a = (T[])new Object[length];
        }
        int idx = 0;
        for (Integer p : this) {
            a[idx++] = (T)p;
        } // FOR
        return (a);
    }
    @Override
    public boolean add(Integer e) {
        return (this.add(e.intValue()));
    }
    public boolean add(int p) {
        if (p == HStoreConstants.NULL_PARTITION_ID) {
            this.contains_null = true;
        } else {
            this.inner.set(p);
        }
        return (true);
    }
    @Override
    public boolean remove(Object o) {
        if (o instanceof Integer) {
            Integer p = (Integer)o;
            return (this.remove(p.intValue()));
        }
        return (false);
    }
    public boolean remove(int p) {
        if (p == HStoreConstants.NULL_PARTITION_ID) {
            this.contains_null = false;
        } else {
            this.inner.set(p, false);            
        }
        return (true);
    }
    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (this.contains(o) == false) {
                return (false);
            }
        } // FOR
        return (true);
    }
    @Override
    public boolean addAll(Collection<? extends Integer> c) {
        boolean ret = true;
        for (Integer o : c) {
            ret = this.add(o) && ret;
        }
        return (ret);
    }
    public boolean addAll(PartitionSet partitions) {
        if (partitions.contains_null) this.contains_null = true;
        for (int p = 0, cnt = partitions.inner.size(); p < cnt; p++) {
            if (partitions.inner.get(p)) this.inner.set(p);
        } // FOR
        return (true);
    }
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = true;
        for (Object o : c) {
            if (o instanceof Integer) {
                ret = this.remove((Integer)o) && ret;
            } else {
                ret = false;
            }
        }
        return (ret);
    }
    @Override
    public boolean retainAll(Collection<?> c) {
        for (Integer p : this) {
            if (c.contains(p) == false) {
                this.remove(p);
            }
        }
        return (true);
    }
    
    // ----------------------------------------------------------------------------
    // STATIC METHODS
    // ----------------------------------------------------------------------------
    
    public static PartitionSet singleton(int p) {
        PartitionSet ret = new PartitionSet();
        ret.add(p);
        return (ret);
    }
    
    // ----------------------------------------------------------------------------
    // ITERATOR
    // ----------------------------------------------------------------------------
    
    private class Itr implements Iterator<Integer> {
        int idx = 0;
        boolean shown_null = false;
        boolean need_seek = true;
        @Override
        public boolean hasNext() {
            this.need_seek = false;
            if (contains_null && this.shown_null == false) {
                return (true);
            }
            for (int cnt = inner.size(); this.idx < cnt; this.idx++) {
                if (inner.get(this.idx)) {
                    return (true);
                }
            } // FOR
            return (false);
        }
        @Override
        public Integer next() {
            if (this.need_seek) {
                if (this.hasNext() == false) return (null);
            }
            if (contains_null && this.shown_null == false) {
                this.shown_null = true;
                return (HStoreConstants.NULL_PARTITION_ID);
            }
            return Integer.valueOf(this.idx++);
        }
        @Override
        public void remove() {
            throw new NotImplementedException("PartitionSet Iterator remove is not implemented");
        }
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        stringer.key("P").array();
        for (Integer p : this) {
            stringer.value(p);
        } // FOR
        stringer.endArray();
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONArray json_arr = json_object.getJSONArray("P");
        for (int i = 0, cnt = json_arr.length(); i < cnt; i++) {
            this.inner.set(json_arr.getInt(i));
        }
    }
}
