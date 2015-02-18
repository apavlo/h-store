package edu.brown.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.MathUtil;

public abstract class HistogramUtil {
    private static final Logger LOG = Logger.getLogger(HistogramUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static final String DELIMITER = "\t";
    public static final String MARKER = "*";
    public static final Integer MAX_CHARS = 80;
    public static final int MAX_VALUE_LENGTH = 20;
    
    /**
     * Histogram Pretty Print
     * @return
     */
    public static <X> String toString(Histogram<X> histogram) {
        return toString(histogram, MAX_CHARS, MAX_VALUE_LENGTH);
    }
    
    public static <X> String toString(Histogram<X> histogram, int max_chars) {
        return toString(histogram, max_chars, MAX_VALUE_LENGTH);
    }
    
    /**
     * Histogram Pretty Print
     * @param max_chars
     * @param max_length
     * @return
     */
    public static <X> String toString(Histogram<X> histogram, int max_chars, int max_length) {
        StringBuilder s = new StringBuilder();
        
        // Figure out the max size of the counts
        int max_ctr_length = 4;
        long total = 0;
        for (X value : histogram.values()) {
            Long ctr = histogram.get(value);
            if (ctr != null) {
                total += ctr.longValue();
                max_ctr_length = Math.max(max_ctr_length, ctr.toString().length());
            }
        } // FOR
        
        boolean debug_percentages = histogram.hasDebugPercentages();
        long max_count = histogram.getMaxCount();
        
        // Don't let anything go longer than MAX_VALUE_LENGTH chars
        String f = "%-" + max_length + "s [%" + max_ctr_length + "d";
        if (debug_percentages) {
            f += " - %4.1f%%";
        }
        f += "] ";
        
        boolean first = true;
        Map<Object, String> debug_names = histogram.getDebugLabels();
        boolean has_labels = histogram.hasDebugLabels();
        for (X value : histogram.values()) {
            if (!first) s.append("\n");
            String str = null;
            if (has_labels) str = debug_names.get(value);
            if (str == null) str = (value != null ? value.toString() : "null");
            int value_str_len = str.length();
            if (value_str_len > max_length) str = str.substring(0, max_length - 3) + "...";
            
            // Value Label + Count
            long cnt = (value != null ? histogram.get(value).longValue() : 0);
            if (debug_percentages) {
                double percent = (cnt / (double)total) * 100;
                s.append(String.format(f, str, cnt, percent));
            } else {
                s.append(String.format(f, str, cnt));
            }
            
            // Histogram Bar
            int barSize = (int)((cnt / (double)max_count) * max_chars);
            for (int i = 0; i < barSize; i++) s.append(MARKER);
            
            first = false;
        } // FOR
        if (histogram.isEmpty()) s.append("<EMPTY>");
        return (s.toString());
    }
    
    /**
     * Return all the instances of the values stored in the histogram.
     * This means that if there is a value that has a count of three in the histogram,
     * then it will appear three times in the returned collection
     */
    public static <X> Collection<X> weightedValues(final Histogram<X> h) {
        List<X> all = new ArrayList<X>();
        for (X x : h.values()) {
            long cnt = h.get(x, 0l);
            for (int i = 0; i < cnt; i++) {
                all.add(x);
            } // FOR
        } // FOR
        return (all);
    }
    
    /**
     * Returns the list of values sorted in descending order by cardinality
     * @param h
     * @return
     */
    public static <X> Collection<X> sortedValues(final Histogram<X> h) {
        SortedSet<X> sorted = new TreeSet<X>(new Comparator<X>() {
            public int compare(final X item0, final X item1) {
                final Long v0 = h.get(item0);
                final Long v1 = h.get(item1);
                if (v0.equals(v1))
                    return (-1);
                return (v1.compareTo(v0));
              }
        });
        sorted.addAll(h.values());
        return (sorted);
    }

    /**
     * Return the weighted sum of the values within the histogram
     * @param h
     * @return
     */
    public static <T extends Number> long sum(Histogram<T> h) {
        long total = 0;
        for (T val : h.values()) {
            long value = val.longValue();
            long weight = h.get(val, 0l);
            total += (value * weight);
        } // FOR
        return (total);
    }

    /**
     * Return the percentile of the values within the histogram
     * @param h
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T extends Number> double[] percentile(Histogram<T> h, int[] percentiles) {
        List list = new ArrayList(h.values());
        Collections.sort(list);
        List<T> typedList = new ArrayList<T>(list);  
        List<T> values = new ArrayList<T>();
        for(T t : typedList){
            Long count = h.get(t);
            for (int i = 0; i< count; i++) {
                values.add(t);
            }
        }
        double[] res = new double[percentiles.length]; 
        for(int i =0 ; i < percentiles.length; i++){
            int percentile = percentiles[i];
            if (percentile > 100) {
                percentile = 100;
            }
            if (percentile < 1) {
                percentile = 1;
            }
            if (values.size()==0) {
                res[i] = Double.NaN;
            }
            else if (values.size()==1){
                res[i] = values.get(0).doubleValue();
            }
            else{
                double position = percentile * (values.size()-1)/ 100.0;
                if (position < 1){
                    res[i] = values.get(0).doubleValue();
                }
                else if(position >= values.size()-1){
                    res[i] = values.get(values.size()-1).doubleValue();
                }
                else{
                    Double floor = Math.floor(position);
                    double d = position - floor;
                    double v1 = (values.get(floor.intValue())).doubleValue();
                    double v2 = (values.get(floor.intValue()+1)).doubleValue();
                    res[i] = (v1 + d * (v2-v1));
                }                
            }
        }
        return res;
    }
    
    public static <T extends Number> double stdev(Histogram<T> h) {
        double values[] = new double[h.getSampleCount()];
        int idx = 0;
        for (T val : h.values()) {
            double value = val.doubleValue();
            long weight = h.get(val, 0l);
            for (int i = 0; i < weight; i++) {
                values[idx++] = value;
            }
        } // FOR
        assert(idx == values.length) : idx + "!=" + values.length;
        return (MathUtil.stdev(values));
    }
    
    /**
     * Return a map where the values of the Histogram are mapped to doubles in
     * the range [-1.0, 1.0]
     * @return
     */
    public static <T> Map<T, Double> normalize(Histogram<T> h) {
        double delta = 2.0d / (double) (h.getValueCount() - 1);
        if (trace.val) {
            LOG.trace("# of Values = " + h.getValueCount());
            LOG.trace("Delta Step  = " + delta);
        }

        // We only want to round the values that we put into the map. If you
        // round the current counter than we will always be off at the end
        Map<T, Double> normalized = new HashMap<T, Double>();
        int precision = 10;
        double current = -1.0d;
        for (T k : h.values()) {
            normalized.put(k, MathUtil.roundToDecimals(current, precision));
            if (trace.val)
                LOG.trace(k + " => " + current + " / " + normalized.get(k));
            current += delta;
        } // FOR
        assert(h.getValueCount() == normalized.size());
        return (normalized);
    }
    
}
