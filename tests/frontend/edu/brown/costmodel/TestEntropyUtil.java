package edu.brown.costmodel;

import java.util.Random;

import edu.brown.BaseTestCase;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;

/**
 * @author pavlo
 */
public class TestEntropyUtil extends BaseTestCase {

    private static final int NUM_PARTITIONS = 300;
    private static final int QUERIES_PER_PARTITION = 1000;
    
    private final Random rand = new Random(0);
    
    /**
     * testPartitionCounts
     */
    public void testPartitionCounts() throws Exception {
        // Simple check to make sure varying numbers of partitions with slight randomness
        // always works and gives us expected results
        for (int num_partitions = 2; num_partitions < NUM_PARTITIONS; num_partitions++) {
            Histogram<Integer> h = new Histogram<Integer>();
            for (int i = 0; i < num_partitions; i++) {
                long count = 1000 + (rand.nextInt(20) - 10); // +/- 10
                h.put(i, count);
            } // FOR
            
            // Just check that it's slightly higher than the best entropy value
            double entropy = EntropyUtil.calculateEntropy(num_partitions, h.getSampleCount(), h);
//            System.err.println(String.format("[%03d] %.05f", num_partitions, entropy));
//            System.err.println(h);
            assert(entropy < 0.1) : "Invalid entropy value " + entropy;
            assert(entropy > 0.0) : "Invalid entropy value " + entropy;
        }
    }
    
    /**
     * testSanityCheck
     */
    public void testSanityCheck() throws Exception {
        // Add a bunch of partitions and then make sure that making one of the partitions zero
        // and making one of the partitions having an ass load both have worst cost
        Histogram<Integer> h = new Histogram<Integer>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            h.put(i, QUERIES_PER_PARTITION);
        }
        double base_entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
        
        h.remove(0, QUERIES_PER_PARTITION);
        double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
        assert(base_entropy < entropy) : "Invalid entropy value " + entropy;

        h.put(0, QUERIES_PER_PARTITION * 3);
        entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
        assert(base_entropy < entropy) : "Invalid entropy value " + entropy;
    }
    
    /**
     * testAlternating
     */
    public void testAlternating() throws Exception {
        Histogram<Integer> h = new Histogram<Integer>();
        long delta = Math.round(QUERIES_PER_PARTITION * 0.50);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            long count = QUERIES_PER_PARTITION + (i % 2 == 0 ? -delta : delta);
            h.put(i, count);
        }
//        System.err.println(h);
        
        double expected = 0.40d;
        double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
//        System.err.println("entropy = " + entropy);
        assert(entropy < 1.0) : "Invalid entropy value " + entropy;
        assert(entropy >= expected) : "Invalid entropy value " + entropy;
    }
    
    /**
     * testCalculateEntropyZipf
     */
    public void testCalculateEntropyZipf() throws Exception {
        int rounds = 5;
        Double last = null;
        double sigma = 1.0000001d;
        
        // For each round, increase the sigma value. We are checking that our entropy value
        // gets worse as the distribution of the histogram gets more skewed
        while (--rounds > 0) {
            Histogram<Integer> h = new Histogram<Integer>();
            RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, 0, NUM_PARTITIONS, sigma);
            for (int i = 0, cnt = (NUM_PARTITIONS * QUERIES_PER_PARTITION); i < cnt; i++) {
                h.put(zipf.nextInt());
            } // FOR

            double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
            assert(entropy >= 0.0) : "Invalid entropy value " + entropy;
            assert(entropy <= 1.0) : "Invalid entropy value " + entropy;
            if (last != null) assert(last < entropy) : last + " < " + entropy;
            last = entropy;
            sigma += 1d;
//            System.err.println(h);
//            System.err.println("[" + sigma + "] Entropy: " + entropy);
//            System.err.println("-------------------------------------");
        } // WHILE
    }
    
    /**
     * testCalculateEntropyBest
     */
    public void testCalculateEntropyBest() throws Exception {
        Histogram<Integer> h = new Histogram<Integer>();
        int num_queries = NUM_PARTITIONS * QUERIES_PER_PARTITION;
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            h.put(partition, QUERIES_PER_PARTITION);
        } // FOR
        double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, num_queries, h);
        assertEquals("Invalid entropy value " + entropy, 0.0, entropy);
    }
    
    /**
     * testCalculateEntropyWorst
     */
    public void testCalculateEntropyWorst() throws Exception {
        Histogram<Integer> h = new Histogram<Integer>();
        int num_queries = QUERIES_PER_PARTITION;
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            h.put(partition, (partition == 0 ? QUERIES_PER_PARTITION : 0));
        } // FOR
        double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, num_queries, h);
        assertEquals("Invalid entropy value " + entropy, 1.0, entropy);
    }
}