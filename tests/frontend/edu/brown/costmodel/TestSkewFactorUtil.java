package edu.brown.costmodel;

import java.util.Random;

import edu.brown.BaseTestCase;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

/**
 * @author pavlo
 */
public class TestSkewFactorUtil extends BaseTestCase {

    private static final int NUM_PARTITIONS = 300;
    private static final int QUERIES_PER_PARTITION = 1000;
    
    private final Random rand = new Random(100);
    
    /**
     * testPartitionCounts
     */
    public void testPartitionCounts() throws Exception {
        // Simple check to make sure varying numbers of partitions with slight randomness
        // always works and gives us expected results
        for (int num_partitions = 2; num_partitions < NUM_PARTITIONS; num_partitions++) {
            Histogram<Integer> h = new ObjectHistogram<Integer>();
            for (int i = 0; i < num_partitions; i++) {
                int count = 1000 + (rand.nextInt(20) - 10); // +/- 10
                h.put(i, count);
            } // FOR
            
            // Just check that it's slightly higher than the best skew value
            double skew = SkewFactorUtil.calculateSkew(num_partitions, h.getSampleCount(), h);
//            System.err.println(String.format("[%03d] %.05f", num_partitions, skew));
//            System.err.println(h);
            assert(skew < 0.1) : "Invalid skew value " + skew;
            assert(skew > 0.0) : "Invalid skew value " + skew;
        }
    }
    
    /**
     * testSanityCheck
     */
    public void testSanityCheck() throws Exception {
        // Add a bunch of partitions and then make sure that making one of the partitions zero
        // and making one of the partitions having an ass load both have worst cost
        Histogram<Integer> h = new ObjectHistogram<Integer>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            h.put(i, QUERIES_PER_PARTITION);
        }
        double base_skew = SkewFactorUtil.calculateSkew(NUM_PARTITIONS, h.getSampleCount(), h);
        
        h.dec(0, QUERIES_PER_PARTITION);
        double skew = SkewFactorUtil.calculateSkew(NUM_PARTITIONS, h.getSampleCount(), h);
        assert(base_skew < skew) : "Invalid skew value " + skew;

        h.put(0, QUERIES_PER_PARTITION * 3);
        skew = SkewFactorUtil.calculateSkew(NUM_PARTITIONS, h.getSampleCount(), h);
        assert(base_skew < skew) : "Invalid skew value " + skew;
    }
    
    /**
     * testAlternating
     */
    public void testAlternating() throws Exception {
        Histogram<Integer> h = new ObjectHistogram<Integer>();
        int delta = (int)Math.round(QUERIES_PER_PARTITION * 0.50);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            int count = QUERIES_PER_PARTITION + (i % 2 == 0 ? -delta : delta);
            h.put(i, count);
        }
//        System.err.println(h);
        
        double expected = 0.40d;
        double skew = SkewFactorUtil.calculateSkew(NUM_PARTITIONS, h.getSampleCount(), h);
//        System.err.println("skew = " + skew);
        assert(skew < 1.0) : "Invalid skew value " + skew;
        assert(skew >= expected) : "Invalid skew value " + skew;
    }
    
    /**
     * testCalculateSkewZipf
     */
    public void testCalculateSkewZipf() throws Exception {
        int rounds = 5;
        Double last = null;
        double sigma = 1.0000001d;
        
        // For each round, increase the sigma value. We are checking that our skew value
        // gets worse as the distribution of the histogram gets more skewed
        while (--rounds > 0) {
            Histogram<Integer> h = new ObjectHistogram<Integer>();
            RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, 0, NUM_PARTITIONS, sigma);
            for (int i = 0, cnt = (NUM_PARTITIONS * QUERIES_PER_PARTITION); i < cnt; i++) {
                h.put(zipf.nextInt());
            } // FOR

            double skew = SkewFactorUtil.calculateSkew(NUM_PARTITIONS, h.getSampleCount(), h);
            assert(skew >= 0.0) : "Invalid skew value " + skew;
            assert(skew <= 1.0) : "Invalid skew value " + skew;
            if (last != null) assert(last < skew) : last + " < " + skew;
            last = skew;
            sigma += 1d;
//            System.err.println(h);
//            System.err.println("[" + sigma + "] skew: " + skew);
//            System.err.println("-------------------------------------");
        } // WHILE
    }
    
    /**
     * testCalculateSkewBest
     */
    public void testCalculateSkewBest() throws Exception {
        Histogram<Integer> h = new ObjectHistogram<Integer>();
        int num_queries = NUM_PARTITIONS * QUERIES_PER_PARTITION;
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            h.put(partition, QUERIES_PER_PARTITION);
        } // FOR
        double skew = SkewFactorUtil.calculateSkew(NUM_PARTITIONS, num_queries, h);
        assertEquals("Invalid skew value " + skew, 0.0, skew);
    }
    
    /**
     * testCalculateSkewWorst
     */
    public void testCalculateSkewWorst() throws Exception {
        Histogram<Integer> h = new ObjectHistogram<Integer>();
        int num_queries = QUERIES_PER_PARTITION;
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            h.put(partition, (partition == 0 ? QUERIES_PER_PARTITION : 0));
        } // FOR
        double skew = SkewFactorUtil.calculateSkew(NUM_PARTITIONS, num_queries, h);
        assertEquals("Invalid skew value " + skew, 1.0, skew, 0.000001);
    }
}