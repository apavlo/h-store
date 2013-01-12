package edu.brown.statistics;

import java.util.Collection;
import java.util.Map;

import edu.brown.utils.JSONSerializable;

public interface Histogram<X> extends JSONSerializable {

    // ----------------------------------------------------------------------------
    // INTERNAL DATA CONTROL METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Set whether this histogram is allowed to retain zero count entries
     * If the flag switches from true to false, then all zero count entries will be removed
     * Default is false
     * @param flag
     */
    public Histogram<X> setKeepZeroEntries(boolean flag);
    
    /**
     * Returns true if this histogram is set to keep any entries whose
     * count reaches zero.
     * @return
     */
    public boolean isZeroEntriesEnabled();
    
    /**
     * Get the number of samples entered into the histogram using the put methods
     * @return
     */
    public int getSampleCount();
    
    /**
     * Returns true if there are no entries in this histogram
     * @return
     */
    public boolean isEmpty();
    
    // ----------------------------------------------------------------------------
    // VALUE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns the current count for the given value
     * If the value was never entered into the histogram, then the count will be null
     * @param value
     * @return
     */
    public Long get(X value);
    
    /**
     * Returns the current count for the given value.
     * If that value was never entered in the histogram, then the value returned will be value_if_null 
     * @param value
     * @param value_if_null
     * @return
     */
    public long get(X value, long value_if_null);
    
    /**
     * Get the number of unique values entered into the histogram 
     * @return
     */
    public int getValueCount();
    
    /**
     * Return all the values stored in the histogram
     * @return
     */
    public Collection<X> values();
    
    /**
     * Return the set of values from the histogram that have the matching count in the histogram
     * @param <T>
     * @param count
     * @return
     */
    public Collection<X> getValuesForCount(long count);
    
    // ----------------------------------------------------------------------------
    // PUT METHODS
    // ----------------------------------------------------------------------------

    /**
     * Increments the number of occurrences of this particular value by delta
     * @param value the value to be added to the histogram
     * @param delta
     * @return the new count for value
     */
    public long put(X value, long delta);
    
    /**
     * Increments the number of occurrences of this particular value i
     * @param value the value to be added to the histogram
     * @return the new count for value
     */
    public long put(X value);
    
    /**
     * Increment all values in the histogram by one
     */
    public void putAll();
    
    /**
     * Increment multiple values by one
     * @param values
     */
    public void put(Collection<X> values);
    
    /**
     * Increment multiple values by the given count
     * @param values
     * @param delta
     */
    public void put(Collection<X> values, long delta);
    
    /**
     * Add all the entries from the provided Histogram into this objects totals
     * @param other
     */
    public void put(Histogram<X> other);
    
    // ----------------------------------------------------------------------------
    // DECREMENT METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Remove the given count from the total of the value by delta
     * @param value
     * @param delta
     * @return the new count for value
     */
    public long dec(X value, long delta);
    
    /**
     * Decrement the count for the given value by one in the histogram
     * @param value
     * @return the new count for value
     */
    public long dec(X value);
    
    /**
     * For each value in the given collection, decrement their count by one for each
     * @param <T>
     * @param values
     */
    public void dec(Collection<X> values);

    /**
     * For each value in the given collection, decrement their count by the given delta
     * @param values
     * @param delta
     */
    public void dec(Collection<X> values, long delta);
    
    /**
     * Decrement all the entries in the other histogram by their counter
     * @param <T>
     * @param values
     */
    public void dec(Histogram<X> other);
    
    // ----------------------------------------------------------------------------
    // CLEAR METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Reset the histogram's internal data
     */
    public void clear();
    
    /**
     * Clear all the values stored in the histogram. The keys are only kept if
     * KeepZeroEntries is enabled, otherwise it does the same thing as clear()
     */
    public void clearValues();
    
    /**
     * Remove the entire count for the given value
     * @param value
     * @return the new count for value
     */
    public long remove(X value);
    
    // ----------------------------------------------------------------------------
    // MIN/MAX METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Get the smallest value entered into the histogram
     * This assumes that the values implement the Comparable interface
     * @return
     */
    public X getMinValue();
    
    /**
     * Return the number of samples for the value with the smallest number of samples in the histogram
     * @return
     */
    public long getMinCount();
    
    /**
     * Return the set values with the smallest number of samples
     * @return
     */
    public Collection<X> getMinCountValues();

    /**
     * Get the largest value entered into the histogram
     * This assumes that the values implement the Comparable interface
     * @return
     */
    public X getMaxValue();
    
    /**
     * Return the number of samples for the value with the greatest number of
     * samples in the histogram
     * @return
     */
    public long getMaxCount();
    
    /**
     * Return the set values with the greatest number of samples
     * @return
     */
    public Collection<X> getMaxCountValues();
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Set the number of occurrences of this particular value i
     * @param value the value to be added to the histogram
     * @return the new count for value
     */
    public long set(X value, long i);
    
    /**
     * Returns true if this histogram contains the specified key.
     * @param value
     * @return
     */
    public boolean contains(X value);
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Histogram Pretty Print
     * @param max_chars size of the bars
     * @return
     */
    public String toString(int max_chars);
    
    /**
     * Histogram Pretty Print
     * @param max_chars size of the bars
     * @param max_len size of the value labels
     * @return
     */
    public String toString(int max_chars, int max_len);
    
    /**
     * Helper method used for replacing the object's toString() output with labels
     * @param names_map
     */
    public Histogram<X> setDebugLabels(Map<?, String> names_map);
    
    public boolean hasDebugLabels();
    public Map<Object, String> getDebugLabels();
    public String getDebugLabel(Object key);
    
    /**
     * Enable percentages in toString() output
     * @return
     */
    public void enablePercentages();
    public boolean hasDebugPercentages();
    
}
