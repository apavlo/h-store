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
    
    private Histogram<Integer> h = new Histogram<Integer>();
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

    /**
     * testGrowing
     */
    public void testGrowing() throws Exception {
        Histogram<Integer> origH = new Histogram<Integer>();
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
     * testSerialization
     */
    public void testSerialization() throws Exception {
        String json = fast_h.toJSONString();
        assertFalse(json.isEmpty());
        
        FastIntHistogram clone = new FastIntHistogram();
        JSONObject jsonObj = new JSONObject(json);
        clone.fromJSON(jsonObj, null);
        
        assertEquals(fast_h.size(), clone.size());
        for (int i = 0, cnt = fast_h.size(); i < cnt; i++) {
            assertEquals(fast_h.get(i), clone.get(i));
        } // FOR
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
                fast_h.fastDec(val);
            }
        } // FOR
        
        for (Integer val : h.values()) {
            assertEquals(h.get(val), fast_h.get(val));
            assertEquals(h.get(val).intValue(), fast_h.get(val.intValue()));
        } // FOR
    }
    
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
    
    /**
     * testValues
     */
    public void testValues() throws Exception {
        Collection<Integer> vals0 = h.values();
        Collection<Integer> vals1 = fast_h.values();
        assertEquals(vals0.size(), vals1.size());
        assertTrue(vals0.containsAll(vals1));
    }
    
    /**
     * testValueCount
     */
    public void testValueCount() {
        assertEquals(h.getValueCount(), fast_h.getValueCount());
    }
    
    
    

}
