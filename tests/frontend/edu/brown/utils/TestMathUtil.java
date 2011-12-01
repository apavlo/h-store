package edu.brown.utils;

import org.junit.Test;

import junit.framework.TestCase;

public class TestMathUtil extends TestCase {

    private static final int NUM_VALUES = 10;
    
    /**
     * Just some know values that I already have the known geometric mean for
     */
    private static final double TEST_VALUES[] = {
        1.0,
        0.01130203601213331,
        0.01823143760522339,
        0.017141472718601114,
        0.002007288849070199,
        0.008572316547717063,
        0.008176750277889333,
        0.011508064996154976,
        0.00688530755354444,
        0.011432267059707457
    };
    private static final double TEST_VALUES_GEOMETRIC_MEAN = 0.015d;
    private static final double TEST_VALUES_STDDEV = 0.29686d;

    /**
     * testFudgeyEquals
     */
    public void testFudgeyEquals() {
        // So dirty...
        double val0 = 1.110;
        double val1 = 1.150;
        
        assert(MathUtil.equals(val0, val1, 2, 0.04));
        assertFalse(MathUtil.equals(val0, val1, 2, 0.01));
    }

    /**
     * testGeometricMean
     */
    @Test
    public void testGeometricMean() {
        double mean = MathUtil.geometricMean(TEST_VALUES);
        assertEquals(TEST_VALUES_GEOMETRIC_MEAN, MathUtil.roundToDecimals(mean, 3));
        mean = MathUtil.geometricMean(TEST_VALUES, MathUtil.GEOMETRIC_MEAN_ZERO);
        assertEquals(TEST_VALUES_GEOMETRIC_MEAN, MathUtil.roundToDecimals(mean, 3));
    }
    
    /**
     * testGeometricMeanOnes
     */
    @Test
    public void testGeometricMeanOnes() {
        double values[] = new double[NUM_VALUES];
        for (int i = 0; i < NUM_VALUES; i++) {
            values[i] = 1.0;
        } // FOR
        double mean = MathUtil.geometricMean(values);
        assertEquals(1.0, mean);
    }

    /**
     * testGeometricMeanZeroes
     */
    @Test
    public void testGeometricMeanZeroes() {
        double values[] = new double[NUM_VALUES];
        for (int i = 0; i < NUM_VALUES; i++) {
            values[i] = 0.0d;
        } // FOR
        double mean = MathUtil.geometricMean(values, MathUtil.GEOMETRIC_MEAN_ZERO);
        assertEquals(0.0, MathUtil.roundToDecimals(mean, 2));
    }
    
    /**
     * testStandardDeviation
     */
    @Test
    public void testStandardDeviation() {
        double stddev = MathUtil.stdev(TEST_VALUES);
        assertEquals(TEST_VALUES_STDDEV, stddev, 0.001);
    }
}