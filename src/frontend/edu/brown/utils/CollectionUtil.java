package edu.brown.utils;

import java.util.*;

public abstract class CollectionUtil {
    
    private static final Random rand = new Random();
    
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
        while (it.hasNext()) {
            list.add(it.next());
        } // WHILE
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
     * Return a random value from the given list
     * @param <T>
     * @param list
     */
    public static <T> T getRandomValue(List<T> list) {
        int size = list.size();
        int idx = rand.nextInt(size);
        return (list.get(idx));
    }
    
    public static <T> T getRandomValue(Iterable<T> it) {
        List<T> list = new ArrayList<T>();
        for (T t : it) {
            list.add(t);
        }
        return getRandomValue(list);
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
        for (T t : items) {
            data.add(t);
        } // FOR
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
    public static <T> T getFirst(Iterable<T> items) {
        if (items instanceof List) {
            return ((List<T>)items).get(0);
        }
        for (T t : items) {
            return (t);
        }
        return (null);
    }
    
    /**
     * Return the first item in a Iterator
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T getFirst(Iterator<T> items) {
        while (items.hasNext()) {
            return items.next();
        }
        return (null);
    }
    
    /**
     * Returns the first item in an Enumeration
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T getFirst(Enumeration<T> items) {
        assert(items.hasMoreElements());
        return items.nextElement();
    }
    
    /**
     * Return the ith element of a set. Super lame
     * @param <T>
     * @param items
     * @param idx
     * @return
     */
    public static <T> T get(Iterable<T> items, int idx) {
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
    public static <T> T getLast(Iterable<T> items) {
        T last = null;
        if (items instanceof AbstractList) {
            last = ((AbstractList<T>)items).get(((AbstractList<T>)items).size() - 1);
        } else {
            for (T t : items) {
                last = t;
            }
        }
        return (last);
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
    
    public static <K, V extends Comparable<? super V>> List<K> getKeysSortedByValue(Map<K, V> map) {
        final int size = map.size();
        final List<K> keys = new ArrayList<K>(size);
        if (true || size == 1) {
            keys.addAll(map.keySet());
        } else {
            final List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>(size);
            list.addAll(map.entrySet());
            final ValueComparator<V> cmp = new ValueComparator<V>();
            Collections.sort(list, cmp);
            for (int i = 0; i < size; i++) {
                keys.set(i, list.get(i).getKey());
            }
        }
        return keys;
    }
    
    private static final class ValueComparator<V extends Comparable<? super V>> implements Comparator<Map.Entry<?, V>> {
        public int compare(Map.Entry<?, V> o1, Map.Entry<?, V> o2) {
            return o1.getValue().compareTo(o2.getValue());
        }
    }
    
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
    
}
