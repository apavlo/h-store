/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Container class that represents a list of partitionIds
 * For now it's just a HashSet
 * @author pavlo
 */
public class PartitionSet implements Collection<Integer> {
    
//    private final List<Integer> inner = new ArrayList<Integer>();
    private final Set<Integer> inner = new HashSet<Integer>();
    
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
    public boolean equals(Object obj) {
        if (obj instanceof PartitionSet) {
            return this.inner.equals(((PartitionSet)obj).inner);
        }
        else if (obj instanceof Collection<?>) {
            Collection<?> other = (Collection<?>)obj;
            if (this.inner.size() != other.size()) return (false);
            return (this.inner.containsAll(other));
        }
        return (false);
    }
    @Override
    public int hashCode() {
        return this.inner.hashCode();
    }
    @Override
    public String toString() {
        return this.inner.toString();
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
        return this.inner.add(e);
//        if (this.inner.contains(e) == false) {
//            return this.inner.add(e);
//        }
//        return (false);
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
        return this.inner.addAll(c);
//        boolean ret = true;
//        for (Integer i : c) {
//            ret = ret && this.add(i);
//        }
//        return ret;
    }
    @Override
    public boolean removeAll(Collection<?> c) {
        return this.inner.removeAll(c);
    }
    @Override
    public boolean retainAll(Collection<?> c) {
        return this.inner.retainAll(c);
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
