package edu.brown.correlations;

import java.util.Random;

import junit.framework.TestCase;

public class TestPearsonCorrelation extends TestCase {
    
    private final Random rand = new Random(0);
    private final int num_samples = 1000;
    
    public static double roundToDecimals(double d, int c) {
        return (Double.parseDouble(String.format("%." + c + "g", d)));
    }
    
    /**
     * testNoSamples
     */
    public void testNoSamples() {
        PearsonCorrelation p = new PearsonCorrelation();
        assertNull(p.calculate());
    }
    
    /**
     * testClear
     */
    public void testClear() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(i, i);
        } // FOR
        p.clear();
        assertEquals(0, p.size());
        assertNull(p.calculate());
    }
    
    /**
     * testSingleSample
     */
    public void testSingleSample() {
        PearsonCorrelation p = new PearsonCorrelation();
        p.addOccurrence(1, 1);
        Double result = p.calculate();
        assertNotNull(result);
        assertEquals(1.0d, roundToDecimals(result, 1));
    }
    
    /**
     * testIntegers
     */
    public void testIntegers() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
        assertEquals(1.0d, roundToDecimals(p.calculate(), 1));
        
        p.clear();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(Integer.MIN_VALUE, Integer.MIN_VALUE);
        }
        assertEquals(1.0d, roundToDecimals(p.calculate(), 1));
    }

    /**
     * testLongs
     */
    public void testLongs() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(Long.MAX_VALUE, Long.MAX_VALUE);
        }
        assertEquals(1.0d, roundToDecimals(p.calculate(), 1));
        
        p.clear();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(Long.MIN_VALUE, Long.MIN_VALUE);
        }
        assertEquals(1.0d, roundToDecimals(p.calculate(), 1));
    }
    
    /**
     * testDoubles
     */
    public void testDoubles() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(Double.MAX_VALUE, Double.MAX_VALUE);
        }
        assertEquals(1.0d, roundToDecimals(p.calculate(), 1));
        
        p.clear();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(Double.MIN_VALUE, Double.MIN_VALUE);
        }
        assertEquals(1.0d, roundToDecimals(p.calculate(), 1));
    }
    
    /**
     * testAbsolutePositive
     */
    public void testAbsolutePositive() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(i, i);
        }
        assertEquals(1.0d, roundToDecimals(p.calculate(), 1));
    }
    
    /**
     * testAbsoluteNegative
     */
    public void testAbsoluteNegative() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(i, -i);
        }
        assertEquals(-1.0d, roundToDecimals(p.calculate(), 1));
    }
    
    /**
     * testRandomCorrelation
     */
    public void testRandomCorrelation() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(i, (this.rand.nextDouble() - 0.5) * i);
        }
        assertTrue(Math.abs(p.calculate()) < 0.1);
    }

    /**
     * testPositiveCorrelation
     */
    public void testPositiveCorrelation() {
        PearsonCorrelation p = new PearsonCorrelation();
        for (int i = 0; i < num_samples; i++) {
            p.addOccurrence(i, (this.rand.nextDouble()) * i);
        }
        assertTrue(Math.abs(p.calculate()) > 0.4);
    }
   
}
