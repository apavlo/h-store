package edu.brown.statistics;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import junit.framework.TestCase;

public class TestHistogramUtil extends TestCase {

    private static final int NUM_SAMPLES = 100;
    private static final int RANGE = 20;
    
    private Histogram<Long> h = new ObjectHistogram<Long>();
    private Random rand = new Random(1);
    
    protected void setUp() throws Exception {
        // Cluster a bunch in the center
        int min = RANGE / 3;
        for (int i = 0; i < NUM_SAMPLES; i++) {
            h.put((long)(rand.nextInt(min) + min));
        }
        for (int i = 0; i < NUM_SAMPLES; i++) {
            h.put((long)(rand.nextInt(RANGE)));
        }
    }
    
    /**
     * testNormalize
     */
    @Test
    public void testNormalize() throws Exception {
        int size = 1;
        int rounds = 8;
        int min = 1000;
        while (rounds-- > 0) {
            ObjectHistogram<Long> h = new ObjectHistogram<Long>();
            for (int i = 0; i < size; i++) {
                h.put((long)(rand.nextInt(min) + min));
            }    
            
            Map<Long, Double> n = HistogramUtil.normalize(h);
            assertNotNull(n);
            assertEquals(h.getValueCount(), n.size());
//            System.err.println(size + " => " + n);
            
            Set<Double> normalized_values = new HashSet<Double>();
            for (Long k : h.values()) {
                assert(n.containsKey(k)) : "[" + rounds +"] Missing " + k;
                Double normalized = n.get(k);
                assertNotNull(normalized);
                assertFalse(normalized_values.contains(normalized));
                normalized_values.add(normalized);
            } // FOR
            
//            assertEquals(-1.0d, n.get(n.firstKey()));
//            if (size > 1) assertEquals(1.0d, n.get(n.lastKey()));
            
            size += size;
        } // FOR
    }
    
}
