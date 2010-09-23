package edu.brown.utils;

import java.util.Comparator;
import java.util.Map;

/**
 * @author pavlo
 * @param <K>
 * @param <V>
 */
public class MapValueComparator<K, V extends Comparable<V>> implements Comparator<K> {

    private final Map<K, V> data;
    private final boolean reverse;
    
    public MapValueComparator(Map<K, V> data, boolean reverse) {
        this.data = data;
        this.reverse = reverse;
    }
    
    public MapValueComparator(Map<K, V> data) {
        this(data, false);
    }
    
    public int compare(K key0, K key1) {
        V v0 = this.data.get(key0);
        V v1 = this.data.get(key1);
        if (v0 == null && v1 == null) return (0);
        if (v0 == null) return (1);
        return (this.reverse ? v1.compareTo(v0) : v0.compareTo(v1));
    };
}