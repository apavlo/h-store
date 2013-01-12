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
package edu.brown.statistics;

import java.util.Collection;
import java.util.HashSet;
import java.util.Random;

import org.json.JSONObject;

import edu.brown.BaseTestCase;

/**
 * 
 * @author pavlo
 */
public class TestFastIntHistogram extends BaseTestCase {

    private static final int NUM_SAMPLES = 1000;
    private static final int RANGE = 32;
    
    private ObjectHistogram<Integer> h = new ObjectHistogram<Integer>();
    private FastIntHistogram fast_h = new FastIntHistogram(RANGE);
    private Random rand = new Random(1);
    
    protected void setUp() throws Exception {
        // Cluster a bunch in the center
        int min = Math.min(1, RANGE / 3);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            int val = rand.nextInt(min) + min; 
            h.put(val);
            fast_h.put(val);
        }
        for (int i = 0; i < NUM_SAMPLES; i++) {
            int val = rand.nextInt(RANGE); 
            h.put(val);
            fast_h.put(val);
        }
    }

    // ----------------------------------------------------------------------------
    // BASE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testCopyConstructor
     */
    public void testCopyConstructor() {
        FastIntHistogram clone = new FastIntHistogram(fast_h);
        assertEquals(fast_h.getSampleCount(), clone.getSampleCount());
        assertEquals(fast_h.getValueCount(), clone.getValueCount());
        for (int val : fast_h.values()) {
            assertEquals(fast_h.get(val), clone.get(val));
        } // FOR
        assertEquals(fast_h.hasDebugLabels(), clone.hasDebugLabels());
        assertEquals(fast_h.hasDebugPercentages(), clone.hasDebugPercentages());
        assertEquals(fast_h.getDebugLabels(), clone.getDebugLabels());
    }
    
    /**
     * testGrowing
     */
    public void testGrowing() throws Exception {
        Histogram<Integer> origH = new ObjectHistogram<Integer>();
        FastIntHistogram fastH = new FastIntHistogram(1);
        for (int i = 0; i < 100; i++) {
            fastH.put(i);
            origH.put(i);
        } // FOR
        assertEquals(origH.getValueCount(), origH.getValueCount());
        assertEquals(origH.getSampleCount(), fastH.getSampleCount());
        assertEquals(new HashSet<Integer>(origH.values()), new HashSet<Integer>(fastH.values()));
        
        for (int i = 100; i < 500; i+=19) {
            fastH.put(i);
            origH.put(i);
        } // FOR
        assertEquals(origH.getValueCount(), origH.getValueCount());
        assertEquals(origH.getSampleCount(), fastH.getSampleCount());
        assertEquals(new HashSet<Integer>(origH.values()), new HashSet<Integer>(fastH.values()));
    }
    
    /**
     * testIfEmpty
     */
    public void testIfEmpty() {
        h.clear();
        fast_h.clear();
        assertTrue(h.isEmpty());
        assertTrue(fast_h.isEmpty());
    }
    
    /**
     * testToString
     */
    public void testToString() {
        int size = 20;
        FastIntHistogram fast_h = new FastIntHistogram();
        for (int i = 0; i < size; i++)
            fast_h.put(i);
        for (int i = 0; i < size; i++)
            fast_h.dec(i);
        
        String str = fast_h.toString();
        assertEquals("<EMPTY>", str.toUpperCase());
    }
    
    /**
     * testToStringAfterClear
     */
    public void testToStringAfterClear() {
        int size = 20;
        FastIntHistogram fast_h = new FastIntHistogram();
        for (int i = 0; i < size; i++)
            fast_h.put(i, 100);
        fast_h.clear();
        String str = fast_h.toString();
        assertEquals("<EMPTY>", str.toUpperCase());
    }
    
    /**
     * testKeepZeroEntries
     */
    public void testKeepZeroEntries() {
        h.setKeepZeroEntries(true);
        fast_h.setKeepZeroEntries(true);
        
        for (int i = 0; i < RANGE; i++) {
            long cnt = h.get(i, 0l);
            if (cnt > 0) {
                h.dec(i, cnt);
                fast_h.dec(i, cnt);
            }
        } // FOR
        assertEquals(h.toString(), RANGE, h.getValueCount());
        assertEquals(fast_h.toString(), RANGE, fast_h.getValueCount());
        
        for (Integer val : h.values()) {
            assertEquals(val.toString(), 0, h.get(val).intValue());
            assertEquals(h.get(val), fast_h.get(val));
            assertEquals(h.get(val).intValue(), fast_h.get(val.intValue()));
        } // FOR
    }

    
    // ----------------------------------------------------------------------------
    // VALUE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testGet
     */
    public void testGet() {
        int val = rand.nextInt(RANGE);
        h.put(val);
        fast_h.put(val);
        
        Long hCnt = h.get(val);
        Long fastCnt = fast_h.get(val);
        assertEquals(Integer.toString(val), hCnt, fastCnt);
    }

    /**
     * testGetIfNull
     */
    public void testGetIfNull() {
        long expected = 99999;
        int val = 12345;
        long hCnt = h.get(val, expected);
        long fastCnt = fast_h.get(val, expected);
        assertEquals(Integer.toString(val), hCnt, fastCnt);
    }

    /**
     * testValues
     */
    public void testValues() {
        Collection<Integer> vals0 = h.values();
        Collection<Integer> vals1 = fast_h.values();
        assertEquals(vals0.size(), vals1.size());
        assertTrue(vals0.containsAll(vals1));
    }
    
    /**
     * testGetValueCount
     */
    public void testGetValueCount() {
        assertEquals(h.getValueCount(), fast_h.getValueCount());
    }
    
    // ----------------------------------------------------------------------------
    // PUT METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testPutWithDelta
     */
    public void testPutWithDelta() {
        long expected = 999;
        int val = RANGE+1;
        h.put(val, expected);
        fast_h.put(val, expected);
        assertEquals(expected, h.get(val).longValue());
        assertEquals(expected, fast_h.get(val));
    }
    
    /**
     * testPutAll
     */
    public void testPutAll() {
        h = new ObjectHistogram<Integer>();
        fast_h = new FastIntHistogram();
        for (int val = 0; val < RANGE; val++) {
            h.put(val);
            assertEquals(1, h.get(val).longValue());
            fast_h.put(val);
            assertEquals(1, fast_h.get(val));
        } // FOR
        
        h.putAll();
        fast_h.putAll();
        
        for (int val = 0; val < RANGE; val++) {
            assertEquals(2, h.get(val).longValue());
            assertEquals(2, fast_h.get(val));
        } // FOR
    }
    
    /**
     * testPutHistogram
     */
    public void testPutHistogram() {
        FastIntHistogram clone = new FastIntHistogram();
        clone.put(h);
        
        assertEquals(h.getSampleCount(), clone.getSampleCount());
        assertEquals(h.getValueCount(), clone.getValueCount());
        for (int val : h.values()) {
            assertEquals(fast_h.get(val), clone.get(val));
        } // FOR
    }
    
    /**
     * testPutFastIntHistogram
     */
    public void testPutFastIntHistogram() {
        FastIntHistogram clone = new FastIntHistogram();
        clone.put(fast_h);

        assertEquals(fast_h.getSampleCount(), clone.getSampleCount());
        assertEquals(fast_h.getValueCount(), clone.getValueCount());
        for (int val : fast_h.values()) {
            assertEquals(fast_h.get(val), clone.get(val));
        } // FOR
    }
    
    // ----------------------------------------------------------------------------
    // DECREMENT METHODS
    // ----------------------------------------------------------------------------

    /**
     * testDec
     */
    public void testDec() {
        h.setKeepZeroEntries(false);
        fast_h.setKeepZeroEntries(false);
        
        int to_remove = rand.nextInt(NUM_SAMPLES);
        for (int i = 0; i < to_remove; i++) {
            int val = rand.nextInt(RANGE);
            if (h.get(val) != null) {
                h.dec(val);
                fast_h.dec(val);
            }
        } // FOR
        
        for (Integer val : h.values()) {
            assertEquals(h.get(val), fast_h.get(val));
            assertEquals(h.get(val).intValue(), fast_h.get(val.intValue()));
        } // FOR
    }
    
    /**
     * testDecHistogram
     */
    public void testDecHistogram() {
        FastIntHistogram clone = new FastIntHistogram(fast_h);
        assertEquals(h.getSampleCount(), clone.getSampleCount());
        assertEquals(h.getValueCount(), clone.getValueCount());

        clone.put(h);
        clone.dec(h);
        
        assertEquals(h.getSampleCount(), clone.getSampleCount());
        assertEquals(h.getValueCount(), clone.getValueCount());
        for (int val : h.values()) {
            assertEquals(fast_h.get(val), clone.get(val));
        } // FOR
    }
    
    /**
     * testDecFastIntHistogram
     */
    public void testDecFastIntHistogram() {
        FastIntHistogram clone = new FastIntHistogram(fast_h);
        assertEquals(h.getSampleCount(), clone.getSampleCount());
        assertEquals(h.getValueCount(), clone.getValueCount());
        
        clone.put(fast_h);
        clone.dec(fast_h);

        assertEquals(fast_h.getSampleCount(), clone.getSampleCount());
        assertEquals(fast_h.getValueCount(), clone.getValueCount());
        for (int val : fast_h.values()) {
            assertEquals(fast_h.get(val), clone.get(val));
        } // FOR
    }

    // ----------------------------------------------------------------------------
    // CLEAR METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testRemove
     */
    public void testRemove() {
        int to_remove = rand.nextInt(RANGE);
        h.put(to_remove);
        fast_h.put(to_remove);
        assertEquals(h.get(to_remove).longValue(), fast_h.get(to_remove));
        
        h.remove(to_remove);
        fast_h.remove(to_remove);
        
        assertNull(h.get(to_remove));
        assertEquals(-1, fast_h.get(to_remove));
        assertFalse(fast_h.contains(to_remove));
    }

    // ----------------------------------------------------------------------------
    // MIN/MAX METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testMinCount
     */
    public void testMinCount() throws Exception {
        assertEquals(h.getMinCount(), fast_h.getMinCount());
    }
    
    /**
     * testMinValue
     */
    public void testMinValue() {
        assertEquals(h.getMinValue(), fast_h.getMinValue());
    }
    
    /**
     * testMinCountValues
     */
    public void testMinCountValues() {
        Collection<Integer> vals0 = h.getMinCountValues();
        Collection<Integer> vals1 = fast_h.getMinCountValues();
        assertEquals(vals0.size(), vals1.size());
        assertTrue(vals0.containsAll(vals1));
    }
    
    /**
     * testMaxCount
     */
    public void testMaxCount() throws Exception {
        assertEquals(h.getMaxCount(), fast_h.getMaxCount());
    }
    
    /**
     * testMaxValue
     */
    public void testMaxValue() {
        assertEquals(h.getMaxValue(), fast_h.getMaxValue());
    }
    
    /**
     * testMaxCountValues
     */
    public void testMaxCountValues() {
        Collection<Integer> vals0 = h.getMaxCountValues();
        Collection<Integer> vals1 = fast_h.getMaxCountValues();
        assertEquals(vals0.size(), vals1.size());
        assertTrue(vals0.containsAll(vals1));
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * testSerialization
     */
    public void testSerialization() throws Exception {
        String json = fast_h.toJSONString();
        assertFalse(json.isEmpty());
        
        FastIntHistogram clone = new FastIntHistogram();
        JSONObject jsonObj = new JSONObject(json);
        clone.fromJSON(jsonObj, null);
        
        // Note that we now allow the size to shrink save space
        assert(clone.size() <= fast_h.size());
        assertEquals(fast_h.getValueCount(), clone.getValueCount());
        assertEquals(fast_h.getSampleCount(), clone.getSampleCount());
        for (int i : fast_h.values()) {
            assertEquals(fast_h.get(i), clone.get(i));
        } // FOR
    }    
}
