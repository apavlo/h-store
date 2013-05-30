/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.statistics;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Database;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;

/**
 * A very nice and simple generic Histogram
 * 
 * @author svelagap
 * @author pavlo
 */
public class ObjectHistogram<X> implements Histogram<X> {
    private static final Logger LOG = Logger.getLogger(ObjectHistogram.class);
    
    public enum Members {
        VALUE_TYPE,
        HISTOGRAM,
        NUM_SAMPLES,
        KEEP_ZERO_ENTRIES,
    }
    
    protected VoltType value_type = VoltType.INVALID;
    protected final Map<X, Long> histogram = new HashMap<X, Long>();
    protected int num_samples = 0;
    private transient boolean dirty = false;
    
    private transient Map<Object, String> debug_names;
    private transient boolean debug_percentages = false;
    
    /**
     * The Min/Max values are the smallest/greatest values we have seen based
     * on some natural ordering
     */
    private transient Comparable<X> min_value;
    protected transient Comparable<X> max_value;
    
    /**
     * The Min/Max counts are the values that have the smallest/greatest number of
     * occurrences in the histogram
     */
    protected transient long min_count = 0;
    protected transient List<X> min_count_values;
    protected transient long max_count = 0;
    protected transient List<X> max_count_values;
    
    /**
     * A switchable flag that determines whether non-zero entries are kept or removed
     */
    protected boolean keep_zero_entries = false;
    
    /**
     * Constructor
     */
    public ObjectHistogram() {
        // Nothing...
    }
    
    /**
     * Constructor
     * @param keepZeroEntries
     */
    public ObjectHistogram(boolean keepZeroEntries) {
        this.keep_zero_entries = keepZeroEntries;
    }
    
    /**
     * Copy Constructor.
     * This is the same as calling putHistogram()
     * @param other
     */
    public ObjectHistogram(ObjectHistogram<X> other) {
        assert(other != null);
        this.put(other);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ObjectHistogram<?>) {
            ObjectHistogram<?> other = (ObjectHistogram<?>)obj;
            return (this.histogram.equals(other.histogram));
        }
        return (false);
    }

    @Override
    public ObjectHistogram<X> setKeepZeroEntries(boolean flag) {
        // When this option is disabled, we need to remove all of the zeroed entries
        if (!flag && this.keep_zero_entries) {
            synchronized (this) {
                Iterator<X> it = this.histogram.keySet().iterator();
                int ctr = 0;
                while (it.hasNext()) {
                    X key = it.next();
                    if (this.histogram.get(key) == 0) {
                        it.remove();
                        ctr++;
                        this.dirty = true;
                    }
                } // WHILE
                if (ctr > 0)
                    LOG.debug("Removed " + ctr + " zero entries from histogram");
            } // SYNCHRONIZED
        }
        this.keep_zero_entries = flag;
        return (this);
    }
    
    @Override
    public boolean isZeroEntriesEnabled() {
        return this.keep_zero_entries;
    }
    
    /**
     * The main method that updates a value in the histogram with a given sample count
     * This should be called by one of the public interface methods that are synchronized
     * This method is not synchronized on purpose for performance
     * @param value
     * @param count
     * @return Return the new count for the given key
     */
    private long _put(X value, long count) {
        // If we're giving a null value, then the count will always be zero
        if (value == null) return (0);
        
        // HACK: Try to infer the internal type if we don't have it already
        if (this.value_type == VoltType.INVALID) {
            try {
                this.value_type = VoltType.typeFromClass(value.getClass());
            } catch (VoltTypeException ex) {
                this.value_type = VoltType.NULL;
            }
        }

        this.num_samples += count;
        
        // If we already have this value in our histogram, then add the new
        // count to its existing total
        Long existing = this.histogram.get(value);
        if (existing != null) {
            count += existing.longValue();
        }
        
        // We can't have a negative value
        if (count < 0) {
            String msg = String.format("Invalid negative count for key '%s' [count=%d]", value, count);
            throw new IllegalArgumentException(msg);
        }
        // If the new count is zero, then completely remove it if we're not
        // allowed to have zero entries
        else if (count == 0 && this.keep_zero_entries == false) {
            this.histogram.remove(value);
        }
        // Otherwise throw it into our map
        else {
            this.histogram.put(value, Long.valueOf(count));
        }
        // Mark ourselves as dirty so that we will always recompute
        // internal values (min/max) when they ask for them
        this.dirty = true;
        return (count);
    }

    /**
     * Recalculate the min/max count value sets
     * Since this is expensive, this should only be done whenever that information is needed 
     */
    @SuppressWarnings("unchecked")
    private synchronized void calculateInternalValues() {
        // Do this before we check before we're dirty
        if (this.min_count_values == null) this.min_count_values = new ArrayList<X>();
        if (this.max_count_values == null) this.max_count_values = new ArrayList<X>();
        
        if (this.dirty == false) return;
        
        // New Min/Max Counts
        // The reason we have to loop through and check every time is that our 
        // value may be the current min/max count and thus it may or may not still
        // be after the count is changed
        this.max_count = 0;
        this.min_count = Long.MAX_VALUE;
        this.min_value = null;
        this.max_value = null;
        
        for (Entry<X, Long> e : this.histogram.entrySet()) {
            X value = e.getKey();
            long cnt = e.getValue().longValue();
            
            // Is this value the new min/max values?
            if (this.min_value == null || this.min_value.compareTo(value) > 0) {
                this.min_value = (Comparable<X>)value;
            } else if (this.max_value == null || this.max_value.compareTo(value) < 0) {
                this.max_value = (Comparable<X>)value;
            }
            
            if (cnt <= this.min_count) {
                if (cnt < this.min_count) this.min_count_values.clear();
                this.min_count_values.add(value);
                this.min_count = cnt;
            }
            if (cnt >= this.max_count) {
                if (cnt > this.max_count) this.max_count_values.clear();
                this.max_count_values.add(value);
                this.max_count = cnt;
            }
        } // FOR
        this.dirty = false;
    }
    
    /**
     * Get the number of samples entered into the histogram using the put methods
     * @return
     */
    @Override
    public int getSampleCount() {
        return (this.num_samples);
    }
    /**
     * Get the number of unique values entered into the histogram 
     * @return
     */
    @Override
    public int getValueCount() {
        return (this.histogram.values().size());
    }
    
    /**
     * Return the internal variable for what we "think" the type is for this
     * Histogram Use this at your own risk
     * 
     * @return
     */
    public VoltType getEstimatedType() {
        return (this.value_type);
    }

    @Override
    public Collection<X> values() {
        return (Collections.unmodifiableCollection(this.histogram.keySet()));
    }
    @Override
    public Collection<X> getValuesForCount(long count) {
        Set<X> ret = new HashSet<X>();
        for (Entry<X, Long> e : this.histogram.entrySet()) {
            if (e.getValue().longValue() == count)
                ret.add(e.getKey());
        } // FOR
        return (ret);
    }
    @Override
    public synchronized void clear() {
        this.histogram.clear();
        this.num_samples = 0;
        this.min_count = 0;
        if (this.min_count_values != null) this.min_count_values.clear();
        this.min_value = null;
        this.max_count = 0;
        if (this.max_count_values != null) this.max_count_values.clear();
        this.max_value = null;
        assert(this.histogram.isEmpty());
        this.dirty = true;
    }
    @Override
    public synchronized void clearValues() {
        if (this.keep_zero_entries) {
            Long zero = Long.valueOf(0);
            for (Entry<X, Long> e : this.histogram.entrySet()) {
                this.histogram.put(e.getKey(), zero);
            } // FOR
            this.num_samples = 0;
            this.min_count = 0;
            if (this.min_count_values != null) this.min_count_values.clear();
            this.min_value = null;
            this.max_count = 0;
            if (this.max_count_values != null) this.max_count_values.clear();
            this.max_value = null;
        } else {
            this.clear();
        }
        this.dirty = true;
    }
    
    @Override
    public boolean isEmpty() {
        return (this.histogram.isEmpty());
    }
    
    // ----------------------------------------------------------------------------
    // PUT METHODS
    // ----------------------------------------------------------------------------

    @Override
    public synchronized long put(X value, long delta) {
        return this._put(value, delta);
    }
    @Override
    public synchronized long put(X value) {
        return this._put(value, 1);
    }
    @Override
    public void putAll() {
        this.put(this.histogram.keySet(), 1);
    }
    @Override
    public void put(Collection<X> values) {
        this.put(values, 1);
    }
    @Override
    public synchronized void put(Collection<X> values, long count) {
        for (X v : values) {
            this._put(v, count);
        } // FOR
    }
    @Override
    public synchronized void put(Histogram<X> other) {
        if (other == this || other == null) return;
        if (other instanceof ObjectHistogram) {
            ObjectHistogram<X> objHistogram = (ObjectHistogram<X>)other;
            for (Entry<X, Long> e : objHistogram.histogram.entrySet()) {
                if (e.getValue().longValue() > 0)
                    this._put(e.getKey(), e.getValue());
            } // FOR
        } else {
            for (X value : other.values()) {
                this._put(value, other.get(value));
            } // FOR
        }
    }
    
    // ----------------------------------------------------------------------------
    // DECREMENT METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public synchronized long dec(X value, long delta) {
        assert(this.histogram.containsKey(value));
        return this._put(value, delta * -1);
    }
    @Override
    public synchronized long dec(X value) {
        return this._put(value, -1);
    }
    @Override
    public synchronized void dec(Collection<X> values) {
        this.dec(values, 1);
    }
    @Override
    public synchronized void dec(Collection<X> values, long delta) {
        for (X v : values) {
            this._put(v, -1 * delta);
        } // FOR
    }
    @Override
    public synchronized void dec(Histogram<X> other) {
        if (other instanceof ObjectHistogram) {
            ObjectHistogram<X> objHistogram = (ObjectHistogram<X>)other;
            for (Entry<X, Long> e : objHistogram.histogram.entrySet()) {
                if (e.getValue().longValue() > 0) {
                    this._put(e.getKey(), -1 * e.getValue().longValue());
                }
            } // FOR
        }
        else {
            for (X value : other.values()) {
                this._put(value, -1 * other.get(value));
            } // FOR
        }
    }
    
    // ----------------------------------------------------------------------------
    // MIN/MAX METHODS
    // ----------------------------------------------------------------------------
    
    @SuppressWarnings("unchecked")
    @Override
    public X getMinValue() {
        this.calculateInternalValues();
        return ((X)this.min_value);
    }
    @SuppressWarnings("unchecked")
    @Override
    public X getMaxValue() {
        this.calculateInternalValues();
        return ((X)this.max_value);
    }
    @Override
    public long getMinCount() {
        this.calculateInternalValues();
        return (this.min_count);
    }
    @Override
    public Collection<X> getMinCountValues() {
        this.calculateInternalValues();
        return (this.min_count_values);
    }
    @Override
    public long getMaxCount() {
        this.calculateInternalValues();
        return (this.max_count);
    }
    @Override
    public Collection<X> getMaxCountValues() {
        this.calculateInternalValues();
        return (this.max_count_values);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public synchronized long set(X value, long i) {
        Long orig = this.get(value);
        if (orig != null && orig != i) {
            i = (orig > i ? -1*(orig - i) : i - orig);
        }
        return this._put(value, i);
    }
    @Override
    public synchronized long remove(X value) {
        Long cnt = this.histogram.get(value);
        if (cnt != null && cnt.longValue() > 0) {
            return this._put(value, cnt * -1);
        }
        return 0l;
    }
    
    // ----------------------------------------------------------------------------
    // GET METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns the current count for the given value
     * If the value was never entered into the histogram, then the count will be null
     * @param value
     * @return
     */
    public Long get(X value) {
        return (this.histogram.get(value)); 
    }
    
    /**
     * Returns the current count for the given value.
     * If that value was nevered entered in the histogram, then the value returned will be value_if_null 
     * @param value
     * @param value_if_null
     * @return
     */
    public long get(X value, long value_if_null) {
        Long count = this.histogram.get(value);
        return (count == null ? value_if_null : count.longValue());
    }
    
    /**
     * Returns true if this histogram contains the specified key.
     * @param value
     * @return
     */
    public boolean contains(X value) {
        return (this.histogram.containsKey(value));
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    @Override
    public Histogram<X> setDebugLabels(Map<?, String> names_map) {
        if (names_map == null) {
            this.debug_names = null;
        }
        else {
            if (this.debug_names == null) {
                synchronized (this) {
                    if (this.debug_names == null) {
                        this.debug_names = new HashMap<Object, String>();
                    }
                } // SYNCH
            }
            this.debug_names.putAll(names_map);
        }
        return (this);
    }
    @Override
    public boolean hasDebugLabels() {
        return (this.debug_names != null && this.debug_names.isEmpty() == false);
    }
    @Override
    public Map<Object, String> getDebugLabels() {
        return (this.debug_names);
    }
    @Override
    public String getDebugLabel(Object key) {
        return (this.debug_names.get(key));
    }
    @Override
    public void enablePercentages() {
        this.debug_percentages = true;
    }
    @Override
    public boolean hasDebugPercentages() {
        return (this.debug_percentages);
    }
    
    @Override
    public String toString() {
        return HistogramUtil.toString(this);
    }
    @Override
    public String toString(int max_chars) {
        return HistogramUtil.toString(this, max_chars);
    }
    @Override
    public String toString(int max_chars, int max_len) {
        return HistogramUtil.toString(this, max_chars, max_len);
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    public void load(File input_path) throws IOException {
        JSONUtil.load(this, null, input_path);
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
        for (Members element : ObjectHistogram.Members.values()) {
            try {
                Field field = ObjectHistogram.class.getDeclaredField(element.toString().toLowerCase());
                switch (element) {
                    case HISTOGRAM: {
                        if (this.histogram.isEmpty() == false) {
                            stringer.key(element.name()).object();
                            synchronized (this) {
                                for (Object value : this.histogram.keySet()) {
                                    stringer.key(value.toString())
                                            .value(this.histogram.get(value));
                                } // FOR
                            } // SYNCH
                            stringer.endObject();
                        }
                        break;
                    }
                    case KEEP_ZERO_ENTRIES: {
                        if (this.keep_zero_entries) {
                            stringer.key(element.name())
                                    .value(this.keep_zero_entries);
                        }
                        break;
                    }
                    case VALUE_TYPE: {
                        VoltType vtype = (VoltType)field.get(this); 
                        stringer.key(element.name()).value(vtype.name());
                        break;
                    }
                    default:
                        stringer.key(element.name())
                                .value(field.get(this));
                } // SWITCH
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize '" + element + "'", ex);
            }
        } // FOR
    }
    
    @Override
    public void fromJSON(JSONObject object, Database catalog_db) throws JSONException {
        this.value_type = VoltType.typeFromString(object.get(Members.VALUE_TYPE.name()).toString());
        assert (this.value_type != null);

        if (object.has(Members.KEEP_ZERO_ENTRIES.name())) {
            this.setKeepZeroEntries(object.getBoolean(Members.KEEP_ZERO_ENTRIES.name()));
        }
        
        // This code sucks ass...
        for (Members element : ObjectHistogram.Members.values()) {
            if (element == Members.VALUE_TYPE || element == Members.KEEP_ZERO_ENTRIES)
                continue;
            try {
                String field_name = element.toString().toLowerCase();
                Field field = ObjectHistogram.class.getDeclaredField(field_name);
                if (element == Members.HISTOGRAM) {
                    if (object.has(element.name()) == false) {
                        continue;
                    }
                    JSONObject jsonObject = object.getJSONObject(element.name());
                    for (String key_name : CollectionUtil.iterable(jsonObject.keys())) {
                        Object key_value = VoltTypeUtil.getObjectFromString(this.value_type, key_name);
                        Long count = Long.valueOf(jsonObject.getLong(key_name));
                        @SuppressWarnings("unchecked")
                        X x = (X)key_value;
                        this.histogram.put(x, count);
                    } // WHILE
                } else if (field_name.endsWith("_count_value")) {
                    @SuppressWarnings("unchecked")
                    Set<Object> set = (Set<Object>) field.get(this);
                    JSONArray arr = object.getJSONArray(element.name());
                    for (int i = 0, cnt = arr.length(); i < cnt; i++) {
                        Object val = VoltTypeUtil.getObjectFromString(this.value_type, arr.getString(i));
                        set.add(val);
                    } // FOR
                } else if (field_name.endsWith("_value")) {
                    if (object.isNull(element.name())) {
                        field.set(this, null);
                    } else {
                        Object value = object.get(element.name());
                        field.set(this, VoltTypeUtil.getObjectFromString(this.value_type, value.toString()));
                    }
                } else {
                    field.set(this, object.getInt(element.name()));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
        
        this.dirty = true;
        this.calculateInternalValues();
    }
}
