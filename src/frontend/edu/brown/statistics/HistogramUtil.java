package edu.brown.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class HistogramUtil {
    
    public static final String DELIMITER = "\t";
    public static final String MARKER = "*";
    public static final Integer MAX_CHARS = 80;
    public static final int MAX_VALUE_LENGTH = 20;
    
    public static <X> String toString(Histogram<X> histogram) {
        return toString(histogram, MAX_CHARS, MAX_VALUE_LENGTH);
    }
    
    public static <X> String toString(Histogram<X> histogram, Integer max_chars) {
        return toString(histogram, max_chars, MAX_VALUE_LENGTH);
    }

    /**
     * Histogram Pretty Print
     * @param max_chars
     * @param max_length
     * @return
     */
    public static <X> String toString(Histogram<X> histogram, Integer max_chars, Integer _max_length) {
        StringBuilder s = new StringBuilder();
        int max_length = (_max_length != null ? _max_length.intValue() : MAX_VALUE_LENGTH);
        
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
            int barSize = (int)((cnt / (double)max_count) * max_chars.intValue());
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
    public static <X> SortedSet<X> sortedValues(final Histogram<X> h) {
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
    
}
