package edu.brown.utils;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections15.set.ListOrderedSet;

/**
 * 
 * @author pavlo
 */
public abstract class CollectionUtil {
    
    private static final Random RANDOM = new Random();
    
    /**
     * Put all of the elements in items into the given array
     * This assumes that the array has been pre-allocated
     * @param <T>
     * @param items
     * @param array
     */
    public static <T> void toArray(Collection<T> items, Object array[], boolean convert_to_primitive) {
        assert(items.size() == array.length);
        
        int i = 0;
        for (T t : items) {
            if (convert_to_primitive) {
                if (t instanceof Long) {
                    array[i] = ((Long)t).longValue();
                } else if (t instanceof Integer) {
                    array[i] = ((Integer)t).intValue();
                } else if (t instanceof Double) {
                    array[i] = ((Double)t).doubleValue();
                } else if (t instanceof Boolean) {
                    array[i] = ((Boolean)t).booleanValue();
                }
            } else {
                array[i] = t;
            }
        }
        return;
    }
    
    /**
     * Put all the values of an Iterator into a List
     * @param <T>
     * @param it
     * @return
     */
    public static <T> List<T> toList(Iterator<T> it) {
        List<T> list = new ArrayList<T>();
        CollectionUtil.addAll(list, it);
        return (list);
    }
    /**
     * Put all of the values of an Iterable into a new List
     * @param <T>
     * @param it
     * @return
     */
    public static <T> List<T> toList(Iterable<T> it) {
        return (toList(it.iterator()));
    }
    
    /**
     * Put all the values of an Iterator into a Set
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Set<T> toSet(Iterator<T> it) {
        Set<T> set = new HashSet<T>();
        CollectionUtil.addAll(set, it);
        return (set);
    }
    /**
     * Put all of the values of an Iterable into a new Set
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Set<T> toSet(Iterable<T> it) {
        return (toSet(it.iterator()));
    }
    
    /**
     * Returns a list containing the string representations of the elements in the collection
     * @param <T>
     * @param data
     * @return
     */
    public static <T> List<String> toStringList(Collection<T> data) {
        List<String> ret = new ArrayList<String>();
        for (T t : data) ret.add(t.toString());
        return (ret);
    }
    
    /**
     * Returns a set containing the string representations of the elements in the collection 
     * @param <T>
     * @param data
     * @return
     */
    public static <T> Set<String> toStringSet(Collection<T> data) {
        Set<String> ret = new HashSet<String>();
        for (T t : data) ret.add(t.toString());
        return (ret);
    }
    
    /**
     * Return a random value from the given Collection
     * @param <T>
     * @param items
     */
    public static <T> T random(Collection<T> items) {
        return (CollectionUtil.random(items, RANDOM));
    }
    
    /**
     * Return a random value from the given Collection
     * @param <T>
     * @param items
     * @param rand
     * @return
     */
    public static <T> T random(Collection<T> items, Random rand) {
        int idx = rand.nextInt(items.size());
        return (CollectionUtil.get(items, idx));
    }
    
    /**
     * Return a random value from the given Iterable
     * @param <T>
     * @param it
     * @return
     */
    public static <T> T random(Iterable<T> it) {
        return (CollectionUtil.random(it, RANDOM));
    }
    
    /**
     * Return a random value from the given Iterable
     * @param <T>
     * @param it
     * @param rand
     * @return
     */
    public static <T> T random(Iterable<T> it, Random rand) { 
        List<T> list = new ArrayList<T>();
        for (T t : it) {
            list.add(t);
        } // FOR
        return (CollectionUtil.random(list, rand));
    }
    
    public static <E extends Enum<?>> Set<E> getAllExcluding(E elements[], E...excluding) {
        Set<E> exclude_set = new HashSet<E>();
        for (E e : excluding) exclude_set.add(e);
        
        Set<E> elements_set = new HashSet<E>();
        for (int i = 0; i < elements.length; i++) {
            if (!exclude_set.contains(elements[i])) elements_set.add(elements[i]);
        } // FOR
        return (elements_set);
//      Crappy java....
//        Object new_elements[] = new Object[elements_set.size()];
//        elements_set.toArray(new_elements);
//        return ((E[])new_elements);
    }
    
    /**
     * Add all the items in the array to a Collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, T...items) {
        for (T i : (T[])items) {
            data.add(i);
        }
        return (data);
    }
    
    /**
     * Add all the items in the Enumeration into a Collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Enumeration<T> items) {
        while (items.hasMoreElements()) {
            data.add(items.nextElement());
        } // WHILE
        return (data);
    }
    
    /**
     * Add all of the items from the Iterable into the given collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Iterable<T> items) {
        return (CollectionUtil.addAll(data, items.iterator()));
    }
    
    /**
     * Add all of the items from the Iterator into the given collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Iterator<T> items) {
        while (items.hasNext()) {
            data.add(items.next());
        } // WHILE
        return (data);
    }
    
    /**
     * 
     * @param <T>
     * @param <U>
     * @param map
     * @return
     */
    public static <T, U extends Comparable<U>> T getGreatest(Map<T, U> map) {
        T max_key = null;
        U max_value = null;
        for (T key : map.keySet()) {
            U value = map.get(key);
            if (max_value == null || value.compareTo(max_value) > 0) {
                max_value = value;
                max_key = key;
            }
        } // FOR
        return (max_key);
    }
    
    /**
     * Return the first item in a Iterable
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Iterable<T> items) {
        return (CollectionUtil.get(items, 0));
    }
    
    /**
     * Return the first item in a Iterator
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Iterator<T> items) {
        return (items.hasNext() ? items.next() : null);
    }
    
    /**
     * Returns the first item in an Enumeration
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Enumeration<T> items) {
        return (items.hasMoreElements() ? items.nextElement() : null);
    }
    
    /**
     * Return the ith element of a set. Super lame
     * @param <T>
     * @param items
     * @param idx
     * @return
     */
    public static <T> T get(Iterable<T> items, int idx) {
        if (items instanceof AbstractList<?>) {
            return ((AbstractList<T>)items).get(idx);
        } else if (items instanceof ListOrderedSet<?>) {
            return ((ListOrderedSet<T>)items).get(idx);
        }
        int ctr = 0;
        for (T t : items) {
            if (ctr++ == idx) return (t);
        }
        return (null);
    }
    
    /**
     * Return the last item in an Iterable
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T last(Iterable<T> items) {
        T last = null;
        if (items instanceof AbstractList<?>) {
            AbstractList<T> list = (AbstractList<T>)items;
            last = (list.isEmpty() ? null : list.get(list.size() - 1));
        } else {
            for (T t : items) {
                last = t;
            }
        }
        return (last);
    }
    
    /**
     * Return the last item in an array
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T last(T...items) {
        if (items != null && items.length > 0) {
            return (items[items.length-1]);
        }
        return (null);
    }

    /**
     * Return a new map sorted by the values of the given original map 
     * @param <K>
     * @param <V>
     * @param map
     * @return
     */
    public static <K, V extends Comparable<V>> Map<K, V> sortByValues(Map<K, V> map) {
        return sortByValues(map, false);
    }
    
    /**
     * 
     * @param <K>
     * @param <V>
     * @param map
     * @param reverse
     * @return
     */
    public static <K, V extends Comparable<V>> Map<K, V> sortByValues(Map<K, V> map, boolean reverse) {
        SortedMap<K, V> sorted = new TreeMap<K, V>(new MapValueComparator<K, V>(map, reverse));
        sorted.putAll(map);
        return (sorted);
    }
    
//    public static <K, V extends Comparable<? super V>> List<K> getKeysSortedByValue(Map<K, V> map) {
//        final int size = map.size();
//        final List<K> keys = new ArrayList<K>(size);
//        if (true || size == 1) {
//            keys.addAll(map.keySet());
//        } else {
//            final List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>(size);
//            list.addAll(map.entrySet());
//            final ValueComparator<V> cmp = new ValueComparator<V>();
//            Collections.sort(list, cmp);
//            for (int i = 0; i < size; i++) {
//                keys.set(i, list.get(i).getKey());
//            }
//        }
//        return keys;
//    }
//    
//    private static final class ValueComparator<V extends Comparable<? super V>> implements Comparator<Map.Entry<?, V>> {
//        public int compare(Map.Entry<?, V> o1, Map.Entry<?, V> o2) {
//            return o1.getValue().compareTo(o2.getValue());
//        }
//    }
    
    /**
     * 
     * @param <K>
     * @param <V>
     * @param map
     * @return
     */
    public static <K extends Comparable<?>, V> List<V> getSortedList(SortedMap<K, Collection<V>> map) {
        List<V> ret = new ArrayList<V>();
        for (K key : map.keySet()) {
            ret.addAll(map.get(key));
        } // FOR
        return (ret);
    }
    
    /**
     * Wrap an Iterable around an Iterator
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Iterable<T> wrapIterator(final Iterator<T> it) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return (it);
            }
        });
    }
    
    public static <T> T pop(Collection<T> items) {
        T t = CollectionUtil.first(items);
        if (t != null) {
            boolean ret = items.remove(t);
            assert(ret);
        }
        return (t);
    }
    
//    
//    /**
//     * Create a set of all possible permutations of the given size for the elements in the data set
//     * @param <T>
//     * @param size
//     * @return
//     */
//    public static <T> Set<Set<T>> createCombinations(Collection<T> data, final int size) {
//        assert(size > 1);
//        assert(size < data.size());
//
//        final int total_num_elements = data.size();
//        final List<T> list = new ArrayList<T>(data);
//        
//        Set<Set<T>> ret = new HashSet<Set<T>>();
//        for (int i = 0; i < total_num_elements; i++) {
//            ListOrderedSet<T> buffer = new ListOrderedSet<T>();
//            T t = list.get(i);
//            buffer.add(t);
//            populatePermutation(ret, list, buffer, size - 1, total_num_elements, i+1);
//        } // FOR
//        return (ret);
//    }
//    
//    private static <T> void populatePermutation(Set<Set<T>> sets, List<T> list, ListOrderedSet<T> buffer, int size, int num_elements, int i) {
//        for ( ; i < num_elements; i++) {
//            buffer.add(list.get(i));
//            if (size > 0 && (i+size) < num_elements) {
//                populatePermutation(sets, list, buffer, size-1, num_elements, i+1);
//            } else if (size == 0) {
//                
//                
//            }
//            
//            // Remove ourselves from the buffer
//            buffer.remove(num_elements-size);
//            
//        } // FOR
//    }
    
}
