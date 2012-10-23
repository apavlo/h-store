package edu.brown.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;

import edu.brown.utils.CollectionUtil;

/**
 * Fixed-size histogram that only stores integers
 * 
 * @author pavlo
 */
public class FastIntHistogram extends Histogram<Integer> {

    private static final int NULL_COUNT = -1;
    
    private long histogram[];
    private int value_count = 0;
    
    public FastIntHistogram() {
        this(10); // HACK
    }
    
    public FastIntHistogram(int size) {
        this.histogram = new long[size];
        this.clearValues();
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    private void grow(int newSize) {
        assert(newSize >= this.histogram.length);
        long temp[] = new long[newSize+10];
        Arrays.fill(temp, this.histogram.length, temp.length, NULL_COUNT);
        System.arraycopy(this.histogram, 0, temp, 0, this.histogram.length);
        this.histogram = temp;
    }
    
    // ----------------------------------------------------------------------------
    // FAST ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    public int size() {
        return (this.histogram.length);
    }
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
    public long get(int value) {
        if (value >= this.histogram.length) {
            return (0);
        }
        return (this.histogram[value] != NULL_COUNT ? this.histogram[value] : 0);
    }
    public long put(int idx) {
        if (idx >= this.histogram.length) {
            this.grow(idx);
        }
        if (this.histogram[idx] == NULL_COUNT) {
            this.histogram[idx] = 1;
            this.value_count++;
        } else {
            this.histogram[idx]++;
        }
        this.num_samples++;
        return (this.histogram[idx]);
    }
    public void put(FastIntHistogram fast) {
        assert(fast.histogram.length <= this.histogram.length);
        if (fast.histogram.length >= this.histogram.length) {
            this.grow(fast.histogram.length);
        }
        for (int i = 0; i < fast.histogram.length; i++) {
            if (fast.histogram[i] != NULL_COUNT) {
                if (this.histogram[i] == NULL_COUNT) {
                    this.histogram[i] = fast.histogram[i];
                    this.value_count++;
                } else {
                    this.histogram[i] += fast.histogram[i];
                }
            }
        } // FOR
    }
    
    public long fastDec(int idx) {
        return this.fastDec(idx, 1);
    }
    public long fastDec(int idx, long count) {
        if (this.histogram[idx] == NULL_COUNT || this.histogram.length <= idx) {
            throw new IllegalArgumentException("No value exists for " + idx);
        } else if (this.histogram[idx] < count) {
            throw new IllegalArgumentException("Count for " + idx + " cannot be negative");
        }
        this.histogram[idx] -= count;
        if (this.histogram[idx] == 0 && this.keep_zero_entries == false) {
            this.histogram[idx] = NULL_COUNT;
            this.value_count--;
        }
        this.num_samples -= count;
        return (this.histogram[idx]);
    }
    
    // ----------------------------------------------------------------------------
    // OVERRIDEN METHODS
    // ----------------------------------------------------------------------------

    @Override
    public Long get(Integer value) {
        return Long.valueOf(this.get(value.intValue()));
    }

    @Override
    public long get(Integer value, long value_if_null) {
        int idx = value.intValue();
        if (this.histogram[idx] == NULL_COUNT) {
            return (value_if_null);
        } else {
            return (this.histogram[idx]);
        }
    }

    @Override
    public long put(Integer value) {
        return this.put(value.intValue());
    }
        
    @Override
    public synchronized long put(Integer value, long i) {
        int idx = value.intValue();
        if (this.histogram[idx] == NULL_COUNT) {
            this.histogram[idx] = i;
            this.value_count++;
        } else {
            this.histogram[idx] += i;
        }
        this.num_samples += i;
        return (this.histogram[idx]);
    }

    @Override
    public void put(Collection<Integer> values) {
        for (Integer v : values)
            this.put(v);
    }

    @Override
    public synchronized void put(Collection<Integer> values, long count) {
        for (Integer v : values)
            this.put(v, count);
    }

    @Override
    public synchronized void put(Histogram<Integer> other) {
        if (other instanceof FastIntHistogram) {
            this.put((FastIntHistogram)other);
        } else {
            for (Integer v : other.values()) {
                this.put(v, other.get(v));
            }
        }
    }

    @Override
    public synchronized long dec(Integer value) {
        return this.dec(value, 1);
    }

    @Override
    public synchronized long dec(Integer value, long count) {
        return this.fastDec(value.intValue(), count);
    }

    @Override
    public synchronized long remove(Integer value) {
        return super.remove(value);
    }

    @Override
    public synchronized void dec(Collection<Integer> values) {
        super.dec(values);
    }

    @Override
    public synchronized void dec(Collection<Integer> values, long delta) {
        super.dec(values, delta);
    }

    @Override
    public synchronized void dec(Histogram<Integer> other) {
        super.dec(other);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FastIntHistogram) {
            FastIntHistogram other = (FastIntHistogram) obj;
            if (this.histogram.length != other.histogram.length)
                return (false);
            if (this.value_count != other.value_count || this.num_samples != other.num_samples)
                return (false);
            for (int i = 0; i < this.histogram.length; i++) {
                if (this.histogram[i] != other.histogram[i])
                    return (false);
            } // FOR
            return (true);
        }
        return (false);
    }

    @Override
    public VoltType getEstimatedType() {
        return VoltType.INTEGER;
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
    public int getValueCount() {
        return this.value_count;
    }

    @Override
    public boolean isEmpty() {
        return (this.value_count == 0);
    }

    @Override
    public boolean contains(Integer value) {
        return (this.histogram[value.intValue()] != -1);
    }

    @Override
    public synchronized void clear() {
        for (int i = 0; i < this.histogram.length; i++) {
            this.histogram[i] = 0;
        } // FOR
        this.num_samples = 0;
    }

    @Override
    public synchronized void clearValues() {
        Arrays.fill(this.histogram, -1);
        this.value_count = 0;
        this.num_samples = 0;
    }

    @Override
    public int getSampleCount() {
        return (this.num_samples);
    }

    @Override
    public Integer getMinValue() {
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1)
                return (i);
        } // FOR
        return (null);
    }

    @Override
    public Integer getMaxValue() {
        for (int i = this.histogram.length - 1; i >= 0; i--) {
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
            if (this.histogram[i] != -1) {
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
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        stringer.key(Members.HISTOGRAM.name()).array();
        for (int i = 0; i < this.histogram.length; i++) {
            stringer.value(this.histogram[i]);
        } // FOR
        stringer.endArray();
        
        stringer.key("VALUE_COUNT").value(this.value_count);
        
        if (this.debug_names != null && this.debug_names.isEmpty() == false) {
            stringer.key("DEBUG_NAMES").object();
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
        for (int i = 0; i < this.histogram.length; i++) {
            this.histogram[i] = jsonArr.getLong(i);
        } // FOR
        this.value_count = object.getInt("VALUE_COUNT");
        
        if (object.has("DEBUG_NAMES")) {
            if (this.debug_names == null) {
                this.debug_names = new TreeMap<Object, String>();
            } else {
                this.debug_names.clear();
            }
            JSONObject jsonObj = object.getJSONObject("DEBUG_NAMES");
            for (String key : CollectionUtil.iterable(jsonObj.keys())) {
                String label = jsonObj.getString(key);
                this.debug_names.put(Integer.valueOf(key), label);
            }
        }
    }

}
