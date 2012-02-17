/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
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
        if (v0 == null && v1 == null)
            return (0);
        if (v0 == null)
            return (1);
        return (this.reverse ? v1.compareTo(v0) : v0.compareTo(v1));
    };
}