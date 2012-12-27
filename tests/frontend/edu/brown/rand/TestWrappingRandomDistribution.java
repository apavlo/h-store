/**
 * 
 */
package edu.brown.rand;

import java.util.Random;

import edu.brown.statistics.Histogram;
import edu.brown.statistics.HistogramUtil;
import edu.brown.statistics.ObjectHistogram;

import junit.framework.TestCase;

/**
 * @author pavlo
 *
 */
public class TestWrappingRandomDistribution extends TestCase {
    
    private final Random rand = new Random(0);
    
    private final int min = 0;
    private final int max = 20;
    private final int start = 10;
    private final double sigma = 1.5d;
    
    private final int num_records = 500000;
    private final int num_rounds = 5;
    
    public void testNextInt() {
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, this.min, this.max, this.sigma);
        WrappingRandomDistribution wrapping = new WrappingRandomDistribution(zipf, this.start);
        
        int round = num_rounds;
        while (round-- > 0) {
            Histogram<Integer> hist = new ObjectHistogram<Integer>();
            // System.out.println("Round #" + Math.abs(num_rounds - 10) + " [sigma=" + sigma + "]");
            for (int i = 0; i < num_records; i++) {
                int value = wrapping.nextInt();
                assert(value >= this.min);
                assert(value <= this.max);
                hist.put(value);
            } // FOR
            
            // System.out.println(hist);
            // System.out.println("----------------------------------------------");
            
            Long last = null;
            int current_idx = this.start;
            // System.out.println("SORTED: " + hist.sortedValues());
            for (Object obj : HistogramUtil.sortedValues(hist)) {
                Integer value = (Integer)obj;
                assertNotNull(value);
                assertEquals(current_idx, value.intValue());
                
                long count = hist.get(value);
                if (last != null) assertTrue("Last value " + last + " is less than current value " + count, last >= count);
                last = count;
                if (++current_idx >= this.max) current_idx = this.min;
            }
        } // FOR
    }
}
