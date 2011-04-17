package edu.brown.rand;

import java.util.Random;

import edu.brown.statistics.Histogram;

import junit.framework.TestCase;

public class TestRandomDistribution extends TestCase {

    private final Random rand = new Random(0);
    
    private final int min = 0;
    private final int max = 20;
    
    private final int num_records = 100000;
    private final int num_rounds = 10;
    
    /**
     * testCalculateMean
     */
    public void testCalculateMean() throws Exception {
        final int expected = ((max - min) / 2) + min;
        final int samples = 10000;
        
        RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(this.rand, min, max);
        double mean = gaussian.calculateMean(samples);
        System.err.println("mean="+ mean);
        assert((expected - 1) <= mean) : (expected - 1) + " <= " + mean; 
        assert((expected + 1) >= mean) : (expected - 1) + " >= " + mean;
    }
    
    /**
     * testHistory
     */
    public void testHistory() throws Exception {
        double sigma = 1.0000001d;
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, sigma);
        zipf.enableHistory();
        
        Histogram<Long> hist = new Histogram<Long>();
        for (int i = 0; i < num_records; i++) {
            hist.put((long)zipf.nextInt());
        } // FOR
        
        Histogram<Long> tracking_hist = zipf.getHistory();
        assertEquals(hist.getSampleCount(), tracking_hist.getSampleCount());
        for (Long value : hist.values()) {
            assert(tracking_hist.contains(value));
            assertEquals(hist.get(value), tracking_hist.get(value));
        } // FOR
    }
    
    /**
     * testGaussianInt
     */
    public void testGaussian() throws Exception {
        int expected = ((max - min) / 2) + min;
        
        int round = num_rounds;
        while (round-- > 0) {
            RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(this.rand, min, max);
            Histogram hist = new Histogram();
            for (int i = 0; i < num_records; i++) {
                int value = gaussian.nextInt();
                // double value = rand.nextGaussian();
                hist.put(value);
            } // FOR
            // System.out.println(hist);
            int max_count_value = (Integer)hist.getMaxCountValue(); 
            // System.out.println("expected=" + expected + ", max_count_value=" + max_count_value);
            assertTrue((expected - 1) <= max_count_value);
            assertTrue((expected + 1) >= max_count_value);
        } // WHILE
    }
    
    /**
     * testGaussianLong
     */
    public void testGaussianLong() throws Exception {
        int expected = ((max - min) / 2) + min;
        
        int round = num_rounds;
        while (round-- > 0) {
            RandomDistribution.Gaussian gaussian = new RandomDistribution.Gaussian(this.rand, min, max);
            Histogram hist = new Histogram();
            for (int i = 0; i < num_records; i++) {
                long value = gaussian.nextLong();
                // double value = rand.nextGaussian();
                hist.put(value);
            } // FOR
            // System.out.println(hist);
            long max_count_value = (Long)hist.getMaxCountValue(); 
            // System.out.println("expected=" + expected + ", max_count_value=" + max_count_value);
            assertTrue((expected - 1) <= max_count_value);
            assertTrue((expected + 1) >= max_count_value);
        } // WHILE
    }
    
    /**
     * testZipfian
     */
    public void testZipfian() throws Exception {
        double sigma = 1.0000001d;
        
        int round = num_rounds;
        while (round-- > 0) {
            RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, sigma);
            Histogram hist = new Histogram();
            // System.out.println("Round #" + Math.abs(num_rounds - 10) + " [sigma=" + sigma + "]");
            for (int i = 0; i < num_records; i++) {
                int value = zipf.nextInt();
                hist.put(value);
            } // FOR
            Long last = null;
            for (Object value : hist.values()) {
                long current = hist.get(value);
                if (last != null) {
                    // assertTrue(last >= current);
                }
                last = current;
            }
//            System.out.println(hist);
//            System.out.println("----------------------------------------------");
            sigma += 0.5d;
        } // FOR
    }
    
    /**
     * testFlatHistogramInt
     */
    public void testFlatHistogramInt() throws Exception {
        Histogram hist = new Histogram();
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, 1.0000001d);
        for (int i = 0; i < num_records; i++) {
            hist.put(zipf.nextInt());
        } // FOR
        
        RandomDistribution.FlatHistogram flat = new RandomDistribution.FlatHistogram(this.rand, hist);
        Histogram hist2 = new Histogram();
        for (int i = 0; i < num_records; i++) {
            hist2.put(flat.nextInt());
        } // FOR
        assertEquals(hist.getMaxCountValue(), hist2.getMaxCountValue());
    }
    
    /**
     * testFlatHistogramLong
     */
    public void testFlatHistogramLong() throws Exception {
        Histogram hist = new Histogram();
        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, min, max, 1.0000001d);
        for (int i = 0; i < num_records; i++) {
            hist.put(zipf.nextLong());
        } // FOR
        
        RandomDistribution.FlatHistogram flat = new RandomDistribution.FlatHistogram(this.rand, hist);
        Histogram hist2 = new Histogram();
        for (int i = 0; i < num_records; i++) {
            hist2.put(flat.nextLong());
        } // FOR
        assertEquals(hist.getMaxCountValue(), hist2.getMaxCountValue());
    }
}
