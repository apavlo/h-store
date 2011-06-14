package edu.brown.statistics;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.*;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Database;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.MathUtil;

/**
 * This class provides a way to visualize the variation in use of a variable.
 * 
 * @author svelagap
 *
 */
public class Histogram<X> implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(Histogram.class);
    
    public static final String DELIMITER = "\t";
    public static final String MARKER = "*";
    public static final Integer MAX_CHARS = 80;
    public static final Integer MAX_VALUE_LENGTH = 20;
    
    public enum Members {
        VALUE_TYPE,
        HISTOGRAM,
        NUM_SAMPLES,
        MIN_VALUE,
//        MIN_COUNT,
//        MIN_COUNT_VALUE,
        MAX_VALUE,
//        MAX_COUNT,
//        MAX_COUNT_VALUE
    }
    
    protected VoltType value_type = VoltType.INVALID;
    protected final SortedMap<X, Long> histogram = new TreeMap<X, Long>();
    protected long num_samples = 0;
    
    /**
     * 
     */
    protected final transient Map<Object, String> debug_names = new HashMap<Object, String>(); 
    
    /**
     * The Min/Max values are the smallest/greatest values we have seen based
     * on some natural ordering
     */
    protected Comparable<Object> min_value;
    protected Comparable<Object> max_value;
    
    /**
     * The Min/Max counts are the values that have the smallest/greatest number of
     * occurences in the histogram
     */
    protected long min_count = 0;
    protected final Set<X> min_count_values = new HashSet<X>();
    protected long max_count = 0;
    protected final Set<X> max_count_values = new HashSet<X>();
    
    /**
     * A switchable flag that determines whether non-zero entries are kept or removed
     */
    private transient boolean keep_zero_entries = false;
    
    /**
     * Constructor
     */
    public Histogram() {
        // Nothing...
    }
    
    /**
     * Copy Constructor
     * @param other
     */
    public Histogram(Histogram<X> other) {
        assert(other != null);
        this.putHistogram(other);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Histogram<?>) {
            Histogram<?> other = (Histogram<?>)obj;
            return (this.histogram.equals(other.histogram));
        }
        return (false);
    }
    
    /**
     * Helper method used for replacing the object's toString() output with labels
     * @param names_map
     */
    public Histogram<X> setDebugLabels(Map<Object, String> names_map) {
        this.debug_names.putAll(names_map);
        return (this);
    }
    public boolean hasDebugLabels() {
        return (!this.debug_names.isEmpty());
    }
    

    /**
     * Set whether this histogram is allowed to retain zero count entries
     * If the flag switches from true to false, then all zero count entries will be removed
     * Default is false
     * @param flag
     */
    public void setKeepZeroEntries(boolean flag) {
        // When this option is disabled, we need to remove all of the zeroed entries
        if (!flag && this.keep_zero_entries) {
            synchronized (this) {
                Iterator<X> it = this.histogram.keySet().iterator();
                long ctr = 0;
                while (it.hasNext()) {
                    X key = it.next();
                    if (this.histogram.get(key) == 0) {
                        it.remove();
                        ctr++;
                    }
                } // WHILE
                if (ctr > 0) {
                    LOG.debug("Removed " + ctr + " zero entries from histogram");
                    this.calculateInternalValues();
                }
            } // SYNCHRONIZED
        }
        this.keep_zero_entries = flag;
    }
    
    public boolean isZeroEntriesEnabled() {
        return this.keep_zero_entries;
    }
    
    /**
     * The main method that updates a value in the histogram with a given sample count
     * This should be called by one of the public interface methods that are synchronized
     * This method is not synchronized on purpose for performance
     * @param value
     * @param count
     */
    @SuppressWarnings("unchecked")
    private void _put(X value, long count) {
        if (value == null) return;
        if (this.value_type == VoltType.INVALID) {
            try {
                this.value_type = VoltType.typeFromClass(value.getClass());
            } catch (VoltTypeException ex) {
                this.value_type = VoltType.NULL;
            }
        }
        
        this.num_samples += count;
        
        // If we already have this value in our histogram, then add the new count
        // to its existing total
        if (this.histogram.containsKey(value)) {
            count += this.histogram.get(value);
        } else if (this.histogram.isEmpty()) {
            this.min_count = count;
            this.min_count_values.add(value);
            this.max_count = count;
            this.max_count_values.add(value);
        }
        assert(count >= 0) : "Invalid negative count for '" + value + "' [count=" + count + "]";
        // If the new count is zero, then completely remove it if we're not allowed to have zero entries
        if (count == 0 && !this.keep_zero_entries) {
            this.histogram.remove(value);
        } else {
            this.histogram.put(value, count);
        }
            
        // Is this value the new min/max values?
        if (this.min_value == null || this.min_value.compareTo(value) > 0) {
            this.min_value = (Comparable<Object>)value;
        } else if (this.max_value == null || this.max_value.compareTo(value) < 0) {
            this.max_value = (Comparable<Object>)value;
        }
    }

    /**
     * Recalculate the min/max count value sets
     * Since this is expensive, this should only be done whenever that information is needed 
     */
    private synchronized void calculateInternalValues() {
        // New Min/Max Counts
        // The reason we have to loop through and check every time is that our 
        // value may be the current min/max count and thus it may or may not still
        // be after the count is changed
        this.max_count = 0;
        this.min_count = Integer.MAX_VALUE;
        for (Entry<X, Long> e : this.histogram.entrySet()) {
            long cnt = e.getValue(); 
            if (cnt <= this.min_count) {
                if (cnt < this.min_count) this.min_count_values.clear();
                this.min_count_values.add(e.getKey());
                this.min_count = cnt;
            }
            if (cnt >= this.max_count) {
                if (cnt > this.max_count) this.max_count_values.clear();
                this.max_count_values.add(e.getKey());
                this.max_count = cnt;
            }
        } // FOR
    }
    
    
    /**
     * Get the number of samples entered into the histogram using the put methods
     * @return
     */
    public long getSampleCount() {
        return (this.num_samples);
    }
    /**
     * Get the number of unique values entered into the histogram 
     * @return
     */
    public int getValueCount() {
        return (this.histogram.values().size());
    }
    
    /**
     * Get the smallest value entered into the histogram
     * This assumes that the values implement the Comparable interface
     * @return
     */
    public Object getMinValue() {
        return (this.min_value);
    }
    /**
     * Get the largest value entered into the histogram
     * This assumes that the values implement the Comparable interface
     * @return
     */
    public Object getMaxValue() {
        return (this.max_value);
    }

    /**
     * Return the number of samples for the value with the smallest number of samples in the histogram
     * @return
     */
    public long getMinCount() {
        return (this.min_count);
    }
    /**
     * Return the value with the smallest number of samples
     * TODO: There might be more than one value with the samples. This return a set
     * @return
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public <T> T getMinCountValue() {
        this.calculateInternalValues();
        return ((T)CollectionUtil.getFirst(this.min_count_values));
    }
    /**
     * Return the set values with the smallest number of samples
     * @return
     */
    public Set<X> getMinCountValues() {
        this.calculateInternalValues();
        return (this.min_count_values);
    }
    /**
     * Return the number of samples for the value with the greatest number of samples in the histogram
     * @return
     */
    public long getMaxCount() {
        this.calculateInternalValues();
        return (this.max_count);
    }
    /**
     * Return the value with the greatest number of samples
     * TODO: There might be more than one value with the samples. This return a set
     * @return
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public <T> T getMaxCountValue() {
        this.calculateInternalValues();
        return ((T)CollectionUtil.getFirst(this.max_count_values));
    }
    /**
     * Return the set values with the greatest number of samples
     * @return
     */
    public Set<X> getMaxCountValues() {
        this.calculateInternalValues();
        return (this.max_count_values);
    }
    /**
     * Return the internal variable for what we "think" the type is for this Histogram
     * Use this at your own risk
     * @return
     */
    public VoltType getEstimatedType() {
        return (this.value_type);
    }

    /**
     * Return all the values stored in the histogram
     * @return
     */
    public Set<X> values() {
        return (Collections.unmodifiableSet(this.histogram.keySet()));
    }
    
    /**
     * Returns the list of values sorted in descending order by cardinality
     * @return
     */
    public SortedSet<X> sortedValues() {
        SortedSet<X> sorted = new TreeSet<X>(new Comparator<X>() {
            public int compare(final X item0, final X item1) {
                final Long v0 = Histogram.this.get(item0);
                final Long v1 = Histogram.this.get(item1);
                if (v0.equals(v1)) return (-1);
                return (v1.compareTo(v0));
              }
        });
        sorted.addAll(this.histogram.keySet());
        return (sorted);
    }
    
    /**
     * Return the set of values from the histogram that have the matching count in the histogram
     * @param <T>
     * @param count
     * @return
     */
    public Set<X> getValuesForCount(long count) {
        Set<X> ret = new HashSet<X>();
        for (Entry<X, Long> e : this.histogram.entrySet()) {
            if (e.getValue() == count) ret.add(e.getKey());
        } // FOR
        return (ret);
    }
    
    /**
     * Reset the histogram's internal data
     */
    public synchronized void clear() {
        this.histogram.clear();
        this.num_samples = 0;
        this.min_count = 0;
        this.min_count_values.clear();
        this.min_value = null;
        this.max_count = 0;
        this.max_count_values.clear();
        this.max_value = null;
        assert(this.histogram.isEmpty());
    }
    
    /**
     * Clear all the values stored in the histogram. The keys are only kept if
     * KeepZeroEntries is enabled, otherwise it does the same thing as clear()
     */
    public synchronized void clearValues() {
        if (this.keep_zero_entries) {
            for (Entry<X, Long> e : this.histogram.entrySet()) {
                this.histogram.put(e.getKey(), 0l);
            } // FOR
            this.num_samples = 0;
            this.min_count = 0;
            this.min_count_values.clear();
            this.min_value = null;
            this.max_count = 0;
            this.max_count_values.clear();
            this.max_value = null;
        } else {
            this.clear();
        }
    }
    
    /**
     * 
     * @return
     */
    public boolean isEmpty() {
        return (this.histogram.isEmpty());
    }
    
    public boolean isSkewed(double skewindication) {
        return (this.getStandardDeviation() > skewindication);
    }
    
    /**
     * Increments the number of occurrences of this particular value i
     * @param value the value to be added to the histogram
     * 
     */
    public synchronized void put(X value, long i) {
        this._put(value, i);
    }
    
    /**
     * Increments the number of occurrences of this particular value i
     * @param value the value to be added to the histogram
     * 
     */
    public synchronized void put(X value) {
        this._put(value, 1);
    }
    
    /**
     * Increment multiple values
     * @param <T>
     * @param values
     */
    public void putAll(Collection<X> values) {
        this.putAll(values, 1);
    }
    
    /**
     * Increment multiple values by the given count
     * @param <T>
     * @param values
     * @param count
     */
    public synchronized void putAll(Collection<X> values, long count) {
        for (X v : values) {
            this._put(v, count);
        } // FOR
    }
    
    /**
     * Add all the entries from the provided Histogram into this objects totals
     * @param other
     */
    public synchronized void putHistogram(Histogram<X> other) {
        for (Entry<X, Long> e : other.histogram.entrySet()) {
            if (e.getValue() > 0) this._put(e.getKey(), e.getValue());
        } // FOR
    }
    
    /**
     * Remove the given count from the total of the value
     * @param value
     * @param count
     */
    public synchronized void remove(X value, long count) {
        assert(this.histogram.containsKey(value));
        this._put(value, count * -1);
//        this.calculateInternalValues();
    }
    
    /**
     * Decrement the count for the given value by one in the histogram
     * @param value
     */
    public synchronized void remove(X value) {
        this._put(value, -1);
        this.calculateInternalValues();
    }
    
    /**
     * Remove the entrie count for the given value
     * @param value
     */
    public synchronized void removeAll(X value) {
        long cnt = this.histogram.get(value);
        if (cnt > 0) {
            this._put(value, cnt * -1);
//            this.calculateInternalValues();
        }
    }
    
    /**
     * For each value in the given collection, decrement their count by one for each
     * @param <T>
     * @param values
     */
    public synchronized void removeValues(Collection<X> values) {
        for (X v : values) {
            this._put(v, -1);
        } // FOR
//        this.calculateInternalValues();
    }
    
    /**
     * Decrement all the entries in the other histogram by their counter
     * @param <T>
     * @param values
     */
    public synchronized void removeHistogram(Histogram<X> other) {
        for (Entry<X, Long> e : other.histogram.entrySet()) {
            if (e.getValue() > 0) this._put(e.getKey(), -1 * e.getValue());
        } // FOR
//        this.calculateInternalValues();
    }

    /**
     * Returns the current count for the given value
     * If the value was never entered into the histogram, then the count will be null
     * @param value
     * @return
     */
    public Long get(X value) {
        Long count = histogram.get(value); 
        return (count); //  == null ? 0 : count);
    }
    
    /**
     * Returns the current count for the given value.
     * If that value was nevered entered in the histogram, then the value returned will be value_if_null 
     * @param value
     * @param value_if_null
     * @return
     */
    public long get(X value, long value_if_null) {
        Long count = histogram.get(value);
        return (count == null ? value_if_null : count);
    }
    
    /**
     * Returns true if this histogram contains the specified key.
     * @param value
     * @return
     */
    public boolean contains(X value) {
        return (this.histogram.containsKey(value));
    }
    
    /**
     * 
     * @return the standard deviation of the number of occurrences of each value
     * so for a histogram:
     * 4 *
     * 5 **
     * 6 ****
     * 7 *******
     * It would give the mean:(1+2+4+7)/4 = 3.5 and deviations:
     * 4 6.25
     * 5 2.25
     * 6 0.25
     * 7 12.5
     * Giving us a standard deviation of (drum roll):
     * 2.3
     */
    public double getStandardDeviation() {
        double average = getMeanOfOccurences();
        double[] deviance = new double[histogram.values().size()];
        int index = 0;
        double sumdeviance = 0;
        for(long i : histogram.values()) {
            deviance[index] = Math.pow(i*1.0-average,2);
            sumdeviance += deviance[index];
            index++;
        }
        return (Math.sqrt(sumdeviance/deviance.length));
    }
    /**
     * 
     * @return The mean of the occurrences of the particular histogram.
     */
    private double getMeanOfOccurences() {
        int sum = 0;
        for(long i : this.histogram.values()) {
            sum += i;
        }
        return (sum / (double)this.histogram.values().size());
    }

    /**
     * Return a map where the values of the Histogram are mapped to 
     * doubles in the range [-1.0, 1.0] 
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> SortedMap<T, Double> normalize() {
        final boolean trace = LOG.isTraceEnabled();
        
        double delta = 2.0d / (double)(this.getValueCount() - 1);
        if (trace) {
            LOG.trace("# of Values = " + this.getValueCount());
            LOG.trace("Delta Step  = " + delta);
        }

        // We only want to round the values that we put into the map. If you round
        // the current counter than we will always be off at the end
        SortedMap<T, Double> normalized = new TreeMap<T, Double>();
        int precision = 10;
        double current = -1.0d;
        for (T k : (Set<T>)this.histogram.keySet()) {
            normalized.put(k, MathUtil.roundToDecimals(current, precision));
            if (trace) LOG.trace(k + " => " + current + " / " + normalized.get(k));
            current += delta;
        } // FOR
        assert(this.histogram.size() == normalized.size());
        
        return (normalized);
    }
    
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * @return Uses the following template for the visualization of a histogram:
     * 4 *
     * 5 **
     * 6 ****
     * 7 *******
     * 
    */
    public String toString() {
        return (this.toString(MAX_CHARS, MAX_VALUE_LENGTH));
    }
    
    /**
     * 
     * @param max_chars size of the bars
     * @return
     */
    public String toString(Integer max_chars) {
        return (this.toString(max_chars, MAX_VALUE_LENGTH));
    }
        
    public String toString(Integer max_chars, Integer max_length) {
        StringBuilder s = new StringBuilder();
        if (max_length == null) max_length = MAX_VALUE_LENGTH;
        
        this.calculateInternalValues();
        
        // Don't let anything go longer than MAX_VALUE_LENGTH chars
        String f = "%-" + max_length + "s [%5d] ";
        boolean first = true;
        boolean has_labels = this.hasDebugLabels();
        for (Object value : this.histogram.keySet()) {
            if (!first) s.append("\n");
            String str = null;
            if (has_labels) str = this.debug_names.get(value);
            if (str == null) str = (value != null ? value.toString() : "null");
            int value_str_len = str.length();
            if (value_str_len > max_length) str = str.substring(0, max_length - 3) + "...";
            
            long cnt = (value != null ? this.histogram.get(value) : 0);
            int chars = (int)((cnt / (double)this.max_count) * max_chars);
            s.append(String.format(f, str, cnt));
            for (int i = 0; i < chars; i++) s.append(MARKER);
            first = false;
        } // FOR
        if (this.histogram.isEmpty()) s.append("<EMPTY>");
        return (s.toString());
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        for (Members element : Histogram.Members.values()) {
            try {
                Field field = Histogram.class.getDeclaredField(element.toString().toLowerCase());
                if (element == Members.HISTOGRAM) {
                    stringer.key(Members.HISTOGRAM.name()).object();
                    for (Object value : this.histogram.keySet()) {
                        stringer.key(value.toString()).value(this.histogram.get(value));
                    } // FOR
                    stringer.endObject();
//                } else if (element == Members.MAX_COUNT_VALUE || element == Members.MIN_COUNT_VALUE) {
//                    stringer.key(element.name()).array();
//                    for (Object o : (Set<Object>)field.get(this)) {
//                        stringer.value(o);
//                    } // FOR
//                    stringer.endArray();
                } else {
                    stringer.key(element.name()).value(field.get(this));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromJSON(JSONObject object, Database catalog_db) throws JSONException {
        this.value_type = VoltType.typeFromString(object.get(Members.VALUE_TYPE.name()).toString());
        assert(this.value_type != null);

        // This code sucks ass...
        for (Members element : Histogram.Members.values()) {
            if (element == Members.VALUE_TYPE) continue;
            try {
                String field_name = element.toString().toLowerCase();
                Field field = Histogram.class.getDeclaredField(field_name);
                if (element == Members.HISTOGRAM) {
                    JSONObject jsonObject = object.getJSONObject(Members.HISTOGRAM.name());
                    Iterator<String> keys = jsonObject.keys();
                    while (keys.hasNext()) {
                        String key_name = keys.next();
                        Object key_value = VoltTypeUtil.getObjectFromString(this.value_type, key_name);
                        Long count = jsonObject.getLong(key_name);
                        this.histogram.put((X)key_value, count);
                    } // WHILE
                } else if (field_name.endsWith("_count_value")) {
                    Set<Object> set = (Set<Object>)field.get(this);
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
                    field.set(this, object.getLong(element.name()));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
        
        this.calculateInternalValues();
    }
}
