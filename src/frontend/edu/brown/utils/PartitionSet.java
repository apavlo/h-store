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
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.NotImplementedException;

import edu.brown.hstore.HStoreConstants;

/**
 * Container class that represents a list of partitionIds
 * For now it's just a HashSet
 * @author pavlo
 */
public class PartitionSet implements Collection<Integer>, JSONSerializable, FastSerializable {
    
    // private final Set<Integer> inner = new HashSet<Integer>();
    private final BitSet inner = new BitSet();
    private boolean contains_null = false;
    private int[] values = null;

    public PartitionSet() {
        // Nothing...
    }
    
    public PartitionSet(Collection<Integer> partitions) {
        this.addAll(partitions);
    }
    
    public PartitionSet(PartitionSet partitions) {
        this.addAll(partitions);
    }
    
    public PartitionSet(Integer...partitions) {
        for (Integer partition : partitions)
            this.add(partition);
    }
    
    public int[] values() {
        if (this.values == null) {
            int size = this.inner.cardinality() + (this.contains_null ? 1 : 0);
            this.values = new int[size];
            int idx = 0;
            for (Integer partition : this) {
                this.values[idx++] = partition.intValue();
            } // FOR
        }
        return (this.values);
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
        this.values = null;
    }
    @Override
    public boolean isEmpty() {
        return (this.contains_null == false && this.inner.isEmpty());
    }
    @Override
    public boolean contains(Object o) {
        if (o instanceof Integer) {
            return this.contains(((Integer)o).intValue());
        }
        return (false);
    }
    public boolean contains(Integer partition) {
        return this.contains(partition.intValue());
    }
    public boolean contains(int partition) {
        if (partition == HStoreConstants.NULL_PARTITION_ID) {
            return (this.contains_null);
        }
        return (this.inner.get(partition));
    }
    @Override
    public Object[] toArray() {
        int length = this.inner.cardinality();
        Object arr[] = new Object[length];
        int idx = 0;
        for (Integer partition : this) {
            arr[idx++] = partition;
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
        for (Integer partition : this) {
            a[idx++] = (T)partition;
        } // FOR
        return (a);
    }
    @Override
    public boolean add(Integer e) {
        return (this.add(e.intValue()));
    }
    public boolean add(int partition) {
        if (partition == HStoreConstants.NULL_PARTITION_ID) {
            this.contains_null = true;
        } else {
            this.inner.set(partition);
        }
        this.values = null;
        return (true);
    }
    @Override
    public boolean remove(Object o) {
        if (o instanceof Integer) {
            Integer partition = (Integer)o;
            return (this.remove(partition.intValue()));
        }
        return (false);
    }
    public boolean remove(int partition) {
        if (partition == HStoreConstants.NULL_PARTITION_ID) {
            this.contains_null = false;
        } else {
            this.inner.set(partition, false);            
        }
        this.values = null;
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
    public boolean addAll(Collection<? extends Integer> partitions) {
        boolean ret = true;
        for (Integer partition : partitions) {
            ret = this.add(partition.intValue()) && ret;
        }
        return (ret);
    }
    public boolean addAll(PartitionSet partitions) {
        if (partitions.contains_null) this.contains_null = true;
        for (int partition = 0, cnt = partitions.inner.size(); partition < cnt; partition++) {
            if (partitions.inner.get(partition)) this.inner.set(partition);
        } // FOR
        return (true);
    }
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = true;
        for (Object o : c) {
            if (o instanceof Number) {
                ret = this.remove(((Number)o).intValue()) && ret;
            } else {
                ret = false;
            }
        }
        return (ret);
    }
    @Override
    public boolean retainAll(Collection<?> c) {
        for (Integer partition : this) {
            if (c.contains(partition) == false) {
                this.remove(partition);
            }
        }
        return (true);
    }
    @Override
    public Iterator<Integer> iterator() {
        return new Itr();
    }
    
    // ----------------------------------------------------------------------------
    // STATIC METHODS
    // ----------------------------------------------------------------------------
    
    public static PartitionSet singleton(int partition) {
        PartitionSet ret = new PartitionSet();
        ret.add(partition);
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
    public void readExternal(FastDeserializer in) throws IOException {
        this.clear();
        this.contains_null = in.readBoolean();
        int cnt = in.readShort();
        for (int i = 0; i < cnt; i++) {
            int partition = in.readInt();
            this.add(partition);
        } // FOR
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeBoolean(this.contains_null);
        out.writeShort(this.inner.cardinality());
        for (Integer partition : this) {
            out.writeInt(partition.intValue());
        } // FOR
    }
    
    
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
        for (Integer partition : this) {
            stringer.value(partition);
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
