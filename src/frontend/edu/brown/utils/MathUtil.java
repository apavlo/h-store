package edu.brown.utils;

/**
 * @author pavlo
 */
public abstract class MathUtil {

    public static final double GEOMETRIC_MEAN_ZERO = 0.001d;

    /**
     * Fudgey equals for doubles. Round both d0 and d1 to precision decimal points
     * and return true if they are within the given fudge factor away from each other
     * @param d0
     * @param d1
     * @param percision
     * @param fudge
     * @return
     */
    public static final boolean equals(double d0, double d1, int percision, double fudge) {
        if (d0 > d1) {
            double temp = d1;
            d1 = d0;
            d0 = temp;
        }
        double r0 = MathUtil.roundToDecimals(d0, percision);
        double r1 = MathUtil.roundToDecimals(d1, percision);
        return (r0 >= (r1 - fudge)) && (r0 <= (r1 + fudge));
    }
    
    public static final boolean equals(float val0, float val1, float fudge) {
        return (Math.abs(val0 - val1) < fudge);
    }
    
    public static final boolean greaterThanEquals(float val0, float val1, float fudge) {
        return (val0 > val1 || MathUtil.equals(val0, val1, fudge));
    }
    
    public static final boolean greaterThan(float val0, float val1, float fudge) {
        return (Math.abs(val0 - val1) > fudge);
    }
    
    public static final boolean lessThanEquals(float val0, float val1, float fudge) {
        return (val0 < val1 || MathUtil.equals(val0, val1, fudge));
    }
    
    /**
     * Returns the geometric mean of the entries in the input array.
     * @param values
     * @return
     */
    public static final double geometricMean(final double[] values) {
        return (geometricMean(values, null));
    }
    
    /**
     * Returns the geometric mean of the entries in the input array.
     * If the zero_value is not null, all zeroes will be replaced with that value
     * @param values
     * @param zero_value
     * @return
     */
    public static final double geometricMean(final double[] values, final Double zero_value) {
        double sumLog = 0.0d;
        for (double v : values) {
            if (v == 0 && zero_value != null) v = zero_value;
            sumLog += Math.log(v); 
        }
        return Math.exp(sumLog / (double)values.length);
    }
    
    public static final double arithmeticMean(final double[] values) {
        double sum = 0.0d;
        for (double v : values) {
            sum += v;
        }
        return sum / (double)values.length;
    }

    public static final double weightedMean(final double[] values, final double[] weights) {
        double total = 0.0d;
        double weight_sum = 0.0d;
        assert(values.length == weights.length);
        for (int i = 0; i < values.length; i++) {
            total += (values[i] * weights[i]);
            weight_sum += weights[i];
        } // FOR
        return (total / (double)weight_sum);
    }
    
    /**
     * Round a double to the given number decimal places
     * @param d
     * @param percision
     * @return
     */
    public static double roundToDecimals(double d, int percision) {
        double p = (double)Math.pow(10, percision);
        return (double)Math.round(d * p) / p;
    }
    
    /**
     * Round a float to the given number decimal places
     * @param d
     * @param percision
     * @return
     */
    public static float roundToDecimals(float d, int percision) {
        float p = (float)Math.pow(10, percision);
        return (float)Math.round(d * p) / p;
    }

}
