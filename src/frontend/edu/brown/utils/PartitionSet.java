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
import java.util.regex.Pattern;

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
 * This is the fastest way to represent a list of partitions in the system.
 * @author pavlo
 */
public class PartitionSet implements Collection<Integer>, JSONSerializable, FastSerializable {
    
    private final BitSet inner = new BitSet();
    private boolean contains_null = false;
    private int[] values = null;

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------
    
    public PartitionSet() {
        // Nothing...
    }
    
    /**
     * Initialize PartitionSet from Integer Collection 
     * @param partitions
     */
    public PartitionSet(Collection<Integer> partitions) {
        this.addAll(partitions);
    }
    
    /**
     * Initialize PartitionSet from an Integer array
     * @param partitions
     */
    public PartitionSet(Integer...partitions) {
        for (Integer partition : partitions)
            this.add(partition);
    }
    
    /**
     * Initialize PartitionSet from an int array
     * @param partitions
     */
    public PartitionSet(int partitions[]) {
        for (int partition : partitions)
            this.add(partition);
    }
    
    /**
     * Copy constructor
     * @param partitions
     */
    public PartitionSet(PartitionSet partitions) {
        this.addAll(partitions);
    }
    
    // ----------------------------------------------------------------------------
    // API METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return a cached int array of the partition ids in this PartitionSet.
     * This is the preferred way (i.e., faster) to iterate over the contents of the set. You will
     * want to use this instead of using its iterator.
     * @return
     */
    public final int[] values() {
        if (this.values == null) {
            int remaining = this.inner.cardinality();
            int size = remaining + (this.contains_null ? 1 : 0);
            this.values = new int[size];
            int idx = 0;
            if (this.contains_null) {
                this.values[idx++] = HStoreConstants.NULL_PARTITION_ID;
            }
            for (int i = 0; idx < size; i++) {
                if (this.inner.get(i)) this.values[idx++] = i;
            } // FOR
        }
        return (this.values);
    }
    /**
     * Return first partition found in this PartitionSet. This is primarily
     * useful for single-partition txns when you just want the only partition in
     * this set.
     * @return
     * @throws IndexOutOfBoundsException
     */
    public int get() throws IndexOutOfBoundsException {
        for (int partition = 0, cnt = this.inner.size(); partition < cnt; partition++) {
            if (this.inner.get(partition)) return (partition);
        } // FOR
        if (this.contains_null) return HStoreConstants.NULL_PARTITION_ID;
        throw new IndexOutOfBoundsException();
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
        // return (this.values().length);
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
        boolean ret = false;
        if (partition == HStoreConstants.NULL_PARTITION_ID) {
            ret = this.contains_null;
            this.contains_null = false;
        } else if (this.inner.get(partition)) {
            ret = true;
            this.inner.set(partition, false);            
        }
        this.values = null;
        return (ret);
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
    public boolean containsAll(PartitionSet partitions) {
        if (this.contains_null != partitions.contains_null) return (false);
        if (this.inner.intersects(partitions.inner)) {
            for (int partition = 0, cnt = partitions.inner.size(); partition < cnt; partition++) {
                if (partitions.inner.get(partition) && this.inner.get(partition) == false) return (false);
            } // FOR
            return (true);
        }
        return (false);
    }
    public boolean addAll(int partitions[]) {
        boolean ret = true;
        for (int partition : partitions) {
            ret = this.add(partition) && ret;
        } // FOR
        return (ret);
    }
    @Override
    public boolean addAll(Collection<? extends Integer> partitions) {
        boolean ret = true;
        for (Integer partition : partitions) {
            ret = this.add(partition.intValue()) && ret;
        } // FOR
        return (ret);
    }
    public boolean addAll(PartitionSet partitions) {
        if (partitions.contains_null) this.contains_null = true;
        this.inner.or(partitions.inner);
        return (true);
    }
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = false;
        for (Object o : c) {
            if (o instanceof Number) {
                ret = this.remove(((Number)o).intValue()) || ret;
            }
        } // FOR
        return (ret);
    }
    public boolean removeAll(PartitionSet partitions) {
        boolean ret = false;
        if (partitions.contains_null) {
            ret = this.contains_null;
            this.contains_null = false;
        }
        if (this.inner.intersects(partitions.inner)) {
            this.inner.andNot(partitions.inner);
            ret = true;
        }
        return (ret);
    }
    @Override
    public boolean retainAll(Collection<?> c) {
        for (Integer partition : this) {
            if (c.contains(partition) == false) {
                this.remove(partition);
            }
        } // FOR
        return (true);
    }
    public boolean retainAll(PartitionSet partitions) {
        if (this.contains_null != partitions.contains_null) this.contains_null = false;
        this.inner.and(partitions.inner);
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
    
    /**
     * Convert a bitmap into a PartitionSet and print it out
     * @param bitmap
     * @return
     */
    public static String toString(boolean bitmap[]) {
        PartitionSet ps = new PartitionSet();
        for (int i = 0; i < bitmap.length; i++) {
            if (bitmap[i]) ps.add(i);
        } // FOR
        return (ps.toString());
    }
    
    /**
     * Parse a description of partition ids and populate a PartitionSet.
     * This supports comma-separated lists (e.g., "1,2,3,4") and 
     * ranges (e.g., "1-4"). You can also mix and match them together 
     * (e.g., "1,2-3,4").
     * @param partitionList
     * @return
     */
    public static PartitionSet parse(String partitionList) {
        Pattern HYPHEN_SPLIT = Pattern.compile(Pattern.quote("-"));
        
        // Partition Ranges
        PartitionSet partitions = new PartitionSet();
        for (String p : StringUtil.splitList(partitionList)) {
            int start = -1;
            int stop = -1;
            String range[] = HYPHEN_SPLIT.split(p);
            if (range.length == 2) {
                start = Integer.parseInt(range[0].trim());
                stop = Integer.parseInt(range[1].trim());
            } else {
                start = Integer.parseInt(p.trim());
                stop = start;
            }
            for (int partition = start; partition < stop + 1; partition++) {
                partitions.add(partition);
            } // FOR
        } // FOR
        return (partitions);
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
