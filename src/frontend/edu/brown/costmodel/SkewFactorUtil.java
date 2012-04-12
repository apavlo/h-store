package edu.brown.costmodel;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.brown.statistics.Histogram;
import edu.brown.utils.MathUtil;

/**
 * @author pavlo
 */
public abstract class SkewFactorUtil {
    private static final Logger LOG = Logger.getLogger(SkewFactorUtil.class);

    private static final int PERCISION = 6;

    private static final String DEBUG_F = "[%02d] current=%d, orig_r=%.05f, r=%.05f, log=%.05f, skew=%.05f\n";

    /**
     * Calculate the skew factor of a histogram. This is an estimation of how
     * uniformly the partitions are accessed
     * 
     * @param num_partitions
     * @param total_ctr
     * @param h
     * @return
     */
    public static double calculateSkew(int num_partitions, long total_ctr, Histogram<Integer> h) {
        assert (num_partitions > 0) : "Number of partitions can't be zero";
        assert (total_ctr > 0) : "Total cannot be zero [valueCount=" + h.getValueCount() + ", sampleCount=" + h.getSampleCount() + "]";
        final boolean debug = LOG.isDebugEnabled();
        if (debug)
            LOG.debug("Calcuating skew for histogram [num_partitions=" + num_partitions + ", total_ctr=" + total_ctr + "]");

        // The skew factor is a calculation of how evenly the partitions are
        // accessed
        double skew = 0.0d;

        // The best skew is when all of the partitions have an equal number of
        // elements
        // As such this is always zero
        double best = 0.0d;
        double best_ratio = 1 / (double) num_partitions;

        // If a ratio for a partition is above the best_ratio, then we just can
        // take the log of its difference.
        // If it is below the ratio, then we need to flip it so that it is
        // greater than the best_ratio, but
        // we also need to scale the ratio it so that 0.0 has the same cost as
        // 1.0

        // The worst skew is always zero. This would occur if all of the counted
        // elements
        // occured on a single partition, while all other partitions were zero
        // in the histogram
        // Given that, then the ratio for that single partition is 1.0, and
        // log(1.0) == 0
        // double worst = MathUtil.roundToDecimals(Math.log(1.0 / best_ratio) +
        // Math.abs(Math.log(2.0) * (best_ratio * 2) * (num_partitions - 1)),
        // PERCISION);

        // double worst = MathUtil.roundToDecimals(Math.log(1.0 / best_ratio) *
        // num_partitions, PERCISION);
        double worst = Math.log(1.0 / best_ratio) * num_partitions;

        // Iterate through the partitions and calculate the skew summation
        double log = 0.0d;
        double ratio = 0.0d;
        double orig_ratio = 0.0d;
        ArrayList<Long> counts = new ArrayList<Long>();
        StringBuilder sb = (debug ? new StringBuilder() : null);
        for (int i = 0; i < num_partitions; i++) {
            long current = h.get(i, 0);
            counts.add(current);

            orig_ratio = ratio = current / (double) total_ctr;

            // If the ratio is less than the best_ratio, then we flip it to be a
            // ratio above the
            // best_ratio. We have to normalize it so that if we are X% below
            // the best_ratio, then
            // the new value has to be X% above the best_ratio
            if (ratio < best_ratio)
                ratio = best_ratio + ((1.0 - (ratio / best_ratio)) * (1.0 - best_ratio));
            assert (ratio >= best_ratio) : "Invalid ratio: " + ratio;
            assert (ratio <= 1.0) : "Invalid ratio: " + ratio;

            log = Math.abs(Math.log(ratio / best_ratio));
            // log = Math.abs(Math.log(abs_ratio / best_ratio) * abs_ratio);
            skew += log;
            if (debug)
                sb.append(String.format(DEBUG_F, i, current, orig_ratio, ratio, log, skew));
        } // FOR
          // skew = MathUtil.roundToDecimals(skew, PERCISION);

        if (debug) {
            // LOG.debug("values = " + counts);
            LOG.debug("Skew:   " + skew);
            LOG.debug("Best:      " + best);
            LOG.debug("BestRatio: " + best_ratio);
            LOG.debug("Worst:     " + worst);
            // We use the min and max skew values to normalize the calculated
            // skew to be between [0, 1]
            if (skew > worst) {
                System.err.println("\n" + sb.toString());
                System.err.println(h);
                System.err.println(skew + " <= " + worst);
            }
        }
        // assert(skew <= worst);
        assert (MathUtil.lessThanEquals(skew, worst, 0.0001)) : skew + " <= " + worst;
        assert (skew >= best) : skew + " >= " + best;

        final double final_value = (skew / worst);
        if (debug)
            LOG.debug("Final:     " + final_value);
        return (final_value);
        // return (Math.abs(1.0d - (skew - worst) / (best - worst)));
    }
}