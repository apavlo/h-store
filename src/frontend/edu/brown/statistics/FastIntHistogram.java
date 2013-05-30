package edu.brown.statistics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;

/**
 * Fixed-size histogram that only stores integers that are greater than zero.
 * It uses a long array to maintain counts. It can automatically
 * grow as you add new values that are greater than the size of the internal array.
 * @author pavlo
 */
public class FastIntHistogram implements Histogram<Integer> {
    
    public enum Members {
        HISTOGRAM,
        DEBUG,
    }

    private static final int NULL_COUNT = -1;
    private static final int GROW_INCREMENT = 10;
    
    private long histogram[];
    private int num_values = 0;
    private int num_samples = 0;
    
    private transient Map<Object, String> debug_names;
    private transient boolean debug_percentages = false;
    private boolean keep_zero_entries = false;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public FastIntHistogram(boolean keepZeroEntries) {
        this(keepZeroEntries, GROW_INCREMENT); // HACK
    }
    
    public FastIntHistogram() {
        this(false, GROW_INCREMENT); // HACK
    }
    
    public FastIntHistogram(int size) {
        this(false, size);
    }
    
    public FastIntHistogram(boolean keepZeroEntries, int size) {
        this.keep_zero_entries = keepZeroEntries;
        this.histogram = new long[size];
        this.clearValues();
    }
    
    /**
     * Copy Constructor
     * @param copy
     */
    public FastIntHistogram(FastIntHistogram copy) {
        this(copy.keep_zero_entries, copy.histogram.length);
        this.debug_percentages = copy.debug_percentages;
        if (copy.hasDebugLabels()) this.setDebugLabels(copy.debug_names);
        this.put(copy);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    private void grow(int newSize) {
        assert(newSize >= this.histogram.length);
        long temp[] = new long[newSize + GROW_INCREMENT];
        Arrays.fill(temp, this.histogram.length, temp.length, NULL_COUNT);
        System.arraycopy(this.histogram, 0, temp, 0, this.histogram.length);
        this.histogram = temp;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FastIntHistogram) {
            FastIntHistogram other = (FastIntHistogram) obj;
            if (this.histogram.length != other.histogram.length)
                return (false);
            if (this.num_values != other.num_values || this.num_samples != other.num_samples)
                return (false);
            for (int i = 0; i < this.histogram.length; i++) {
                if (this.histogram[i] != other.histogram[i])
                    return (false);
            } // FOR
            return (true);
        }
        return (false);
    }
    
    public void ensureSize(int size) {
        if (this.histogram.length < size) {
            this.grow(size);
        }
    }
    
    // ----------------------------------------------------------------------------
    // FAST ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the internal capacity of this histogram.
     * You probably don't want this value and instead want getSampleCount()
     * @return
     */
    protected int size() {
        return (this.histogram.length);
    }
    /**
     * Return an array of the values in this histogram
     * @return
     */
    public int[] fastValues() {
        int num_values = 0;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != NULL_COUNT) num_values++;
        }
        
        int values[] = new int[num_values];
        int idx = 0;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != NULL_COUNT) {
                values[idx++] = i;
            }
        } // FOR
        return (values);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL DATA CONTROL METHODS
    // ----------------------------------------------------------------------------

    @Override
    public Histogram<Integer> setKeepZeroEntries(boolean flag) {
        this.keep_zero_entries = flag;
        return (this);
    }
    @Override
    public boolean isZeroEntriesEnabled() {
        return (this.keep_zero_entries);
    }
    @Override
    public int getSampleCount() {
        return (this.num_samples);
    }
    @Override
    public boolean isEmpty() {
        return (this.num_samples == 0);
    }
    
    // ----------------------------------------------------------------------------
    // VALUE METHODS
    // ----------------------------------------------------------------------------
    
    public long get(int value) {
        if (value >= this.histogram.length) {
            return (NULL_COUNT);
        }
        return (this.histogram[value] != NULL_COUNT ? this.histogram[value] : NULL_COUNT);
    }
    public long get(int idx, long value_if_null) {
        if (idx >= this.histogram.length) {
            return (value_if_null);
        }
        else if (this.histogram[idx] == NULL_COUNT) {
            return (value_if_null);
        }
        else {
            return (this.histogram[idx]);
        }
    }
    @Override
    public Long get(Integer value) {
        return Long.valueOf(this.get(value.intValue()));
    }

    @Override
    public long get(Integer value, long value_if_null) {
        return this.get(value.intValue(), value_if_null);
    }
    @Override
    public int getValueCount() {
        return this.num_values;
    }
    @Override
    public Collection<Integer> values() {
        List<Integer> values = new ArrayList<Integer>();
        for (int idx : this.fastValues()) {
            values.add(idx);
        } // FOR
        return (values);
    }
    @Override
    public Collection<Integer> getValuesForCount(long count) {
        List<Integer> values = new ArrayList<Integer>();
        for (int idx : this.fastValues()) {
            if (this.get(idx) == count) {
                values.add(idx);    
            }
        } // FOR
        return (values);
    }
    
    // ----------------------------------------------------------------------------
    // PUT METHODS
    // ----------------------------------------------------------------------------
    
    public long put(int value) {
        return this.put(value, 1);
    }
    public long put(int value, long delta) {
        if (value >= this.histogram.length) {
            this.grow(value);
        }
        if (this.histogram[value] == NULL_COUNT) {
            this.histogram[value] = delta;
            this.num_values++;
        } else {
            this.histogram[value] += delta;
        }
        this.num_samples += delta;
        if (this.histogram[value] == 0 && this.keep_zero_entries == false) {
            this.histogram[value] = NULL_COUNT;
            this.num_values--;
        }
        return (this.histogram[value]);
    }
    
    public void put(FastIntHistogram fast) {
        if (fast.histogram.length >= this.histogram.length) {
            this.grow(fast.histogram.length);
        }
        for (int value = 0; value < fast.histogram.length; value++) {
            if (fast.histogram[value] != NULL_COUNT) {
                if (this.histogram[value] == NULL_COUNT) {
                    this.histogram[value] = fast.histogram[value];
                    this.num_values++;
                } else {
                    this.histogram[value] += fast.histogram[value];
                }
                this.num_samples += fast.histogram[value];
            }
        } // FOR
    }
    @Override
    public long put(Integer value) {
        return this.put(value.intValue());
    }
    @Override
    public long put(Integer value, long delta) {
        return this.put(value.intValue(), delta);
    }
    @Override
    public void put(Collection<Integer> values) {
        for (Integer v : values)
            this.put(v.intValue());
    }
    public void put(int values[]) {
        for (int idx : values) {
            this.put(idx, 1);
        } // FOR
    }
    @Override
    public void put(Collection<Integer> values, long delta) {
        for (Integer value : values)
            this.put(value.intValue(), delta);
    }
    public void put(int values[], long delta) {
        for (int value : values) {
            this.put(value, delta);
        } // FOR
    }
    @Override
    public void put(Histogram<Integer> other) {
        if (other instanceof FastIntHistogram) {
            this.put((FastIntHistogram)other);
        } else {
            for (Integer value : other.values()) {
                Long cnt = other.get(value);
                if (cnt != null) {
                    this.put(value.intValue(), cnt.longValue());
                }
            } // FOR
        }
    }
    @Override
    public void putAll() {
        for (int value = 0; value < this.histogram.length; value++) {
            if (this.histogram[value] != NULL_COUNT) {
                this.histogram[value]++;
                this.num_samples++;
            }
        } // FOR
    }
    
    // ----------------------------------------------------------------------------
    // DECREMENT METHODS
    // ----------------------------------------------------------------------------
    
    public long dec(int idx) {
        return this.dec(idx, 1);
    }
    public long dec(int idx, long count) {
        if (this.histogram[idx] == NULL_COUNT || this.histogram.length <= idx) {
            throw new IllegalArgumentException("No value exists for " + idx);
        } else if (this.histogram[idx] < count) {
            throw new IllegalArgumentException("Count for " + idx + " cannot be negative");
        }
        this.histogram[idx] -= count;
        if (this.histogram[idx] == 0 && this.keep_zero_entries == false) {
            this.histogram[idx] = NULL_COUNT;
            this.num_values--;
        }
        this.num_samples -= count;
        return (this.histogram[idx]);
    }
    
    @Override
    public long dec(Integer value) {
        return this.dec(value, 1);
    }
    @Override
    public long dec(Integer value, long count) {
        return this.dec(value.intValue(), count);
    }
    @Override
    public void dec(Collection<Integer> values) {
        this.dec(values, 1);
    }
    @Override
    public void dec(Collection<Integer> values, long delta) {
        this.dec(values, delta);
    }
    public void dec(Histogram<Integer> other) {
        if (other instanceof FastIntHistogram) {
            this.dec((FastIntHistogram)other);
        } else {
            for (Integer v : other.values()) {
                long cnt = other.get(v, NULL_COUNT);
                if (cnt != NULL_COUNT) this.dec(v.intValue(), cnt);
            } // FOR
        }
    }
    public void dec(FastIntHistogram fast) {
        for (int i = 0; i < fast.histogram.length; i++) {
            if (fast.histogram[i] != NULL_COUNT && this.histogram[i] != NULL_COUNT) {
                if (this.histogram[i] <= fast.histogram[i]) {
                    this.num_samples -= this.histogram[i];
                    if (this.keep_zero_entries) {
                        this.histogram[i] = 0;
                    }
                    else {
                        this.histogram[i] = NULL_COUNT;
                        this.num_values--;
                    }
                }
                else {
                    this.num_samples -= fast.histogram[i];
                    this.histogram[i] -= fast.histogram[i];
                }
            }
        } // FOR
    }
    
    // ----------------------------------------------------------------------------
    // CLEAR METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void clear() {
        Arrays.fill(this.histogram, NULL_COUNT);
        this.num_values = 0;
        this.num_samples = 0;
    }
    @Override
    public void clearValues() {
        if (this.keep_zero_entries) {
            for (int i = 0; i < this.histogram.length; i++) {
                this.histogram[i] = 0;
            } // FOR
        } else {
            this.clear();
        }
        this.num_samples = 0;
    }
    @Override
    public long remove(Integer value) {
        return this.remove(value.intValue());
    }
    public long remove(int value) {
        if (value < this.histogram.length) {
            this.histogram[value] = NULL_COUNT;
        }
        return (0);
    }
    
    // ----------------------------------------------------------------------------
    // MIN/MAX METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public Integer getMinValue() {
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1)
                return (i);
        } // FOR
        return (null);
    }
    @Override
    public long getMinCount() {
        long min_cnt = Integer.MAX_VALUE;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1 && this.histogram[i] < min_cnt) {
                min_cnt = this.histogram[i];
            }
        } // FOR
        return (min_cnt);
    }
    @Override
    public Collection<Integer> getMinCountValues() {
        List<Integer> min_values = new ArrayList<Integer>();
        long min_cnt = Integer.MAX_VALUE;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != NULL_COUNT) {
                if (this.histogram[i] == min_cnt) {
                    min_values.add(i);
                } else if (this.histogram[i] < min_cnt) {
                    min_values.clear();
                    min_values.add(i);
                    min_cnt = this.histogram[i];
                }
            }
        } // FOR
        return (min_values);
    }
    @Override
    public Integer getMaxValue() {
        for (int i = this.histogram.length - 1; i >= 0; i--) {
            if (this.histogram[i] != NULL_COUNT)
                return (i);
        } // FOR
        return (null);
    }
    @Override
    public long getMaxCount() {
        long max_cnt = 0;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1 && this.histogram[i] > max_cnt) {
                max_cnt = this.histogram[i];
            }
        } // FOR
        return (max_cnt);
    }
    @Override
    public Collection<Integer> getMaxCountValues() {
        List<Integer> max_values = new ArrayList<Integer>();
        long max_cnt = 0;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1) {
                if (this.histogram[i] == max_cnt) {
                    max_values.add(i);
                } else if (this.histogram[i] > max_cnt) {
                    max_values.clear();
                    max_values.add(i);
                    max_cnt = this.histogram[i];
                }
            }
        } // FOR
        return (max_values);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public long set(Integer value, long i) {
        return (this.histogram[value.intValue()] = i);
    }
    public boolean contains(int idx) {
        return (this.histogram[idx] != NULL_COUNT);
    }
    @Override
    public boolean contains(Integer value) {
        return (this.histogram[value.intValue()] != NULL_COUNT);
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    
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
    
    @Override
    public Histogram<Integer> setDebugLabels(Map<?, String> names_map) {
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
        
        // Go through once and find the greatest position where
        // there are no more non-null values
        int maxSize = 0;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != NULL_COUNT) {
                maxSize = i;
            }
        } // FOR
        stringer.key(Members.HISTOGRAM.name()).array();
        for (int i = 0; i <= maxSize; i++) {
            stringer.value(this.histogram[i]);
        } // FOR
        stringer.endArray();
        
        if (this.debug_names != null && this.debug_names.isEmpty() == false) {
            stringer.key(Members.DEBUG.name()).object();
            for (Entry<Object, String> e : this.debug_names.entrySet()) {
                stringer.key(e.getKey().toString())
                        .value(e.getValue().toString());
            } // FOR
            stringer.endObject();
        }
    }
    
    @Override
    public void fromJSON(JSONObject object, Database catalog_db) throws JSONException {
        JSONArray jsonArr = object.getJSONArray(Members.HISTOGRAM.name());
        this.histogram = new long[jsonArr.length()];
        this.clear();
        for (int i = 0; i < this.histogram.length; i++) {
            long delta = jsonArr.getLong(i);
            if (delta != NULL_COUNT) this.put(i, delta);
        } // FOR
        
        if (object.has(Members.DEBUG.name())) {
            if (this.debug_names == null) {
                this.debug_names = new TreeMap<Object, String>();
            } else {
                this.debug_names.clear();
            }
            JSONObject jsonObj = object.getJSONObject(Members.DEBUG.name());
            for (String key : CollectionUtil.iterable(jsonObj.keys())) {
                String label = jsonObj.getString(key);
                this.debug_names.put(Integer.valueOf(key), label);
            }
        }
    }
}
