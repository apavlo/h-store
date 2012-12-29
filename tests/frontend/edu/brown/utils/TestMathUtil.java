package edu.brown.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.HistogramUtil;
import edu.brown.statistics.ObjectHistogram;

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
        double expected = 0.015d;
        double mean = MathUtil.geometricMean(TEST_VALUES);
        assertEquals(expected, MathUtil.roundToDecimals(mean, 3));
        mean = MathUtil.geometricMean(TEST_VALUES, MathUtil.GEOMETRIC_MEAN_ZERO);
        assertEquals(expected, MathUtil.roundToDecimals(mean, 3));
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
     * testStandardDeviation1
     */
    @Test
    public void testStandardDeviation1() {
        double expected = 0.3129164048d;
        double stddev = MathUtil.stdev(TEST_VALUES);
        assertEquals(expected, stddev, 0.001);
    }
    
    /**
     * testStandardDeviation2
     */
    @Test
    public void testStandardDeviation2() {
        List<Double> values = new ArrayList<Double>();
        for (int i = 0; i < 100; i++) {
            double v = Double.parseDouble(String.format("%d.%d", i, i));
            values.add(v);
        } // FOR
        
        double expected = 29.2410446405d;
        double stddev = MathUtil.stdev(CollectionUtil.toDoubleArray(values));
        assertEquals(expected, stddev, 0.001);
    }
    
    /**
     * testStandardDeviationHistogram
     */
    @Test
    public void testStandardDeviationHistogram() {
        Random rand = new Random(0);
        List<Integer> values = new ArrayList<Integer>();
        FastIntHistogram h0 = new FastIntHistogram(100);
        Histogram<Integer> h1 = new ObjectHistogram<Integer>(); 
        for (int i = 0; i < 1000; i++) {
            int val = rand.nextInt(100);
            values.add(val);
            h0.put(val);
            h1.put(val);
        } // FOR
        
        double expected = MathUtil.stdev(CollectionUtil.toDoubleArray(values));
        assertEquals(expected, HistogramUtil.stdev(h0), 0.001);
        assertEquals(expected, HistogramUtil.stdev(h1), 0.001);
    }

}