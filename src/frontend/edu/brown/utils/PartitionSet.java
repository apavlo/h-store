/**
 * 
 */
package edu.brown.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Container class that represents a list of partitionIds
 * For now it's just a HashSet
 * @author pavlo
 */
public class PartitionSet implements Collection<Integer> {
    
    private final List<Integer> inner = new ArrayList<Integer>();
    
    public PartitionSet() {
        // Nothing...
    }
    
    public PartitionSet(Collection<Integer> partitions) {
        this.inner.addAll(partitions);
    }
    
    public PartitionSet(Integer...partitions) {
        for (Integer p : partitions)
            this.inner.add(p);
    }
    
    @Override
    public int size() {
        return this.inner.size();
    }
    @Override
    public void clear() {
        this.inner.clear();
    }
    @Override
    public boolean isEmpty() {
        return this.inner.isEmpty();
    }
    @Override
    public boolean contains(Object o) {
        return this.inner.contains(o);
    }
    @Override
    public Iterator<Integer> iterator() {
        return this.inner.iterator();
    }
    @Override
    public Object[] toArray() {
        return this.inner.toArray();
    }
    @Override
    public <T> T[] toArray(T[] a) {
        return this.inner.toArray(a);
    }
    @Override
    public boolean add(Integer e) {
        if (this.inner.contains(e) == false) {
            return this.inner.add(e);
        }
        return (false);
    }
    @Override
    public boolean remove(Object o) {
        return this.inner.remove(o);
    }
    @Override
    public boolean containsAll(Collection<?> c) {
        return this.inner.containsAll(c);
    }
    @Override
    public boolean addAll(Collection<? extends Integer> c) {
        boolean ret = true;
        for (Integer i : c) {
            ret = ret && this.add(i);
        }
        return ret;
    }
    @Override
    public boolean removeAll(Collection<?> c) {
        return this.inner.removeAll(c);
    }
    @Override
    public boolean retainAll(Collection<?> c) {
        return this.retainAll(c);
    }
    
    // ----------------------------------------------------------------------------
    // UNMODIFIABLE WRAPPER (DEBUGGING)
    // ----------------------------------------------------------------------------
    
    public static PartitionSet umodifiable(PartitionSet ps) {
        return new UnmodifiablePartitionSet(ps);
    }

    private static class UnmodifiablePartitionSet extends PartitionSet {
        final Collection<Integer> inner;
        
        private UnmodifiablePartitionSet(PartitionSet ps) {
            this.inner = Collections.unmodifiableCollection(new PartitionSet(ps));
        }
        
        @Override
        public Iterator<Integer> iterator() {
            return this.inner.iterator();
        }
        @Override
        public int size() {
            return this.inner.size();
        }
        @Override
        public boolean isEmpty() {
            return this.inner.isEmpty();
        }
        @Override
        public boolean contains(Object o) {
            return this.inner.contains(o);
        }
        @Override
        public boolean add(Integer e) {
            return this.inner.add(e);
        }
        @Override
        public boolean addAll(Collection<? extends Integer> c) {
            return this.inner.addAll(c);
        }
        @Override
        public boolean remove(Object o) {
            return this.inner.remove(o);
        }
        @Override
        public boolean removeAll(Collection<?> c) {
            return this.inner.removeAll(c);
        }
        @Override
        public void clear() {
            this.inner.clear();
        }
        @Override
        public boolean equals(Object o) {
            return this.inner.equals(o);
        }
        @Override
        public int hashCode() {
            return this.inner.hashCode();
        }
        @Override
        public Object[] toArray() {
            return this.inner.toArray();
        }
        @Override
        public <T> T[] toArray(T[] a) {
            return this.inner.toArray(a);
        }
        @Override
        public boolean containsAll(Collection<?> c) {
            return this.inner.containsAll(c);
        }
    } // CLASS
}
