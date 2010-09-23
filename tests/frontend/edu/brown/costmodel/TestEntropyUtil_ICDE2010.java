package edu.brown.costmodel;

import java.util.Random;

import edu.brown.BaseTestCase;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;

/**
 * @author pavlo
 */
public class TestEntropyUtil_ICDE2010 extends BaseTestCase {

    private static final int NUM_PARTITIONS = 100;
    private static final int QUERIES_PER_PARTITION = 10000;
    
    private final Random rand = new Random(0);
        
    /**
     * testAlternating
     */
    public void testAlternating() throws Exception {
        Histogram h = new Histogram();
        long delta = Math.round(NUM_PARTITIONS * 0.75);
        boolean flag = true;
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            long count = 100 + (flag ? -delta : delta);
            if (i > 0 && i % 10 == 0) {
                flag = !flag;
            }
//            long count = QUERIES_PER_PARTITION + (i < 50 ? -delta : delta);
            h.put(i, count);
        }
//        System.err.println(h);
        
        double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
//        System.err.println(h);
        for (Object o : h.values()) {
            System.err.println(h.get(o));
        }
        System.err.println("-------------------------------------");
        System.err.println("Entropy: " + entropy);
        // 0.38240806470607336
    }
    
//  /**
//  * testCalculateEntropyRandom
//  */
// public void testCalculateEntropyRandom() throws Exception {
//     
//     Histogram h = new Histogram();
//     // RandomDistribution.DiscreteRNG rd = new RandomDistribution.Flat(this.rand, 0, NUM_PARTITIONS);
//     Random rd = new Random(100);
//     for (int i = 0, cnt = NUM_PARTITIONS; i < cnt; i++) {
//         h.put(new Integer(i), (long)rd.nextInt(50) + 25);
//     } // FOR
//
//     double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
//     assert(entropy >= 0.0) : "Invalid entropy value " + entropy;
//     assert(entropy <= 1.0) : "Invalid entropy value " + entropy;
//     
////     System.err.println(h);
//     for (Object o : h.values()) {
//         System.err.println(h.get(o));
//     }
//     System.err.println("-------------------------------------");
//     System.err.println("Entropy: " + entropy);
//     //  0.06059952585595973
// }

//    /**
//     * testCalculateEntropyGaussian
//     */
//    public void testCalculateEntropyGaussian() throws Exception {
//        
//        Histogram h = new Histogram();
//        RandomDistribution.Gaussian rd = new RandomDistribution.Gaussian(this.rand, 0, NUM_PARTITIONS);
//        for (int i = 0, cnt = (NUM_PARTITIONS * QUERIES_PER_PARTITION); i < cnt; i++) {
//            h.put(rd.nextInt());
//        } // FOR
//
//        double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
//        assert(entropy >= 0.0) : "Invalid entropy value " + entropy;
//        assert(entropy <= 1.0) : "Invalid entropy value " + entropy;
//        
////        System.err.println(h);
//        for (Object o : h.values()) {
//            System.err.println(h.get(o));
//        }
//        System.err.println("-------------------------------------");
//        System.err.println("Entropy: " + entropy);
//        // 0.414531018667955
//
//    }
    
//    /**
//     * testCalculateEntropyZipf
//     */
//    public void testCalculateEntropyZipf() throws Exception {
//        double sigma = 1.0000001d;
//        
//        Histogram h = new Histogram();
//        RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(this.rand, 0, NUM_PARTITIONS, sigma);
//        for (int i = 0, cnt = (NUM_PARTITIONS * QUERIES_PER_PARTITION); i < cnt; i++) {
//            h.put(zipf.nextInt());
//        } // FOR
//
//        double entropy = EntropyUtil.calculateEntropy(NUM_PARTITIONS, h.getSampleCount(), h);
//        assert(entropy >= 0.0) : "Invalid entropy value " + entropy;
//        assert(entropy <= 1.0) : "Invalid entropy value " + entropy;
//        
////        System.err.println(h);
//        for (Object o : h.values()) {
//            System.err.println(h.get(o));
//        }
//        System.err.println("-------------------------------------");
//        System.err.println("[" + sigma + "] Entropy: " + entropy);
//        // 0.7438131640472553
//    }
    

}