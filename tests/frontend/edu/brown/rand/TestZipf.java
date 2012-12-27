package edu.brown.rand;

import java.util.Random;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

public class TestZipf extends BaseTestCase {

    private final Random rand = new Random(0);
    private final int min = 0;
    private final int max = 32000;
    
    /**
     * testSigmaValues
     */
    public void testSigmaValues() throws Exception {
        double delta = 0.1;
        double sigma = 1.0000000001d;
        for (int i = 0; i < 5; i++) {
            RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, sigma);
            Histogram<Integer> h = new ObjectHistogram<Integer>(); 
            for (int j = 0; j < 10000000; j++) {
                h.put(zipf.nextInt());
            } // FOR
            System.err.println(h);
            System.err.println("==========================================");
            sigma += delta;
        } // FOR
    }
}
