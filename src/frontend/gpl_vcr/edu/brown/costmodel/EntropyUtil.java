package edu.brown.costmodel;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.brown.statistics.Histogram;
import edu.brown.utils.MathUtil;

/**
 * 
 * @author pavlo
 */
public abstract class EntropyUtil {
    private static final Logger LOG = Logger.getLogger(EntropyUtil.class);
    
    private static final int PERCISION = 6;
    
    private static final String DEBUG_F = "[%02d] current=%d, orig_r=%.05f, r=%.05f, log=%.05f, entropy=%.05f\n";

    /**
     * Calculate the entropy value of a histogram. This is an estimation of how uniformly
     * the partitions are accessed
     * @param num_partitions
     * @param total_ctr
     * @param h
     * @return
     */
    public static double calculateEntropy(int num_partitions, long total_ctr, Histogram h) {
        assert(num_partitions > 0) : "Number of partitions can't be zero";
        assert(total_ctr > 0) : "Total cannot be zero [valueCount=" + h.getValueCount() + ", sampleCount=" + h.getSampleCount() + "]";
        final boolean debug = LOG.isDebugEnabled(); 
        if (debug) LOG.debug("Calcuating entropy for histogram [num_partitions=" + num_partitions + ", total_ctr=" + total_ctr + "]");
        
        // The entropy value is a calculation of how evenly the partitions are accessed 
        double entropy = 0.0d;
        
        // The best entropy is when all of the partitions have an equal number of elements
        // As such this is always zero
        double best = 0.0d;
        double best_ratio = 1 / (double)num_partitions;
        
        // If a ratio for a partition is above the best_ratio, then we just can take the log of its difference.
        // If it is below the ratio, then we need to flip it so that it is greater than the best_ratio, but
        // we also need to scale the ratio it so that 0.0 has the same cost as 1.0
        
        // The worst entropy is always zero. This would occur if all of the counted elements
        // occured on a single partition, while all other partitions were zero in the histogram
        // Given that, then the ratio for that single partition is 1.0, and log(1.0) == 0
        //double worst = MathUtil.roundToDecimals(Math.log(1.0 / best_ratio) + Math.abs(Math.log(2.0) * (best_ratio * 2) * (num_partitions - 1)), PERCISION);
        double worst = MathUtil.roundToDecimals(Math.log(1.0 / best_ratio) * num_partitions, PERCISION);
        
        // Iterate through the partitions and calculate the entropy summation
        double log = 0.0d;
        double ratio = 0.0d;
        double orig_ratio = 0.0d;
        ArrayList<Long> counts = new ArrayList<Long>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < num_partitions; i++) {
            Long current = h.get(i);
            if (current == null) current = 0l;
            assert(current != null);
            counts.add(current);
            
            orig_ratio = ratio = current / (double)total_ctr;
            
            // If the ratio is less than the best_ratio, then we flip it to be a ratio above the
            // best_ratio. We have to normalize it so that if we are X% below the best_ratio, then 
            // the new value has to be X% above the best_ratio
            if (ratio < best_ratio) ratio = best_ratio + ((1.0 - (ratio / best_ratio)) * (1.0 - best_ratio));
            assert(ratio >= best_ratio) : "Invalid ratio: " + ratio;
            assert(ratio <= 1.0) : "Invalid ratio: " + ratio;
            
            log = Math.abs(Math.log(ratio / best_ratio));
            // log = Math.abs(Math.log(abs_ratio / best_ratio) * abs_ratio);
            entropy += log;
            sb.append(String.format(DEBUG_F, i, current, orig_ratio, ratio, log, entropy));
        } // FOR
        entropy = MathUtil.roundToDecimals(entropy, PERCISION);
        
        if (debug) {
            // LOG.debug("values = " + counts);
            LOG.debug("Entropy:   " + entropy);
            LOG.debug("Best:      " + best);
            LOG.debug("BestRatio: " + best_ratio);
            LOG.debug("Worst:     " + worst);
        }

        // We use the min and max entropy values to normalize the calculated entropy to be between [0, 1]
        if (entropy > worst) {
            System.err.println("\n" + sb.toString());
            System.err.println(h);
            System.err.println(entropy + " <= " + worst);
        }
        assert(entropy <= worst) : entropy + " <= " + worst;
        assert(entropy >= best) : entropy + " >= " + best;
        
        final double final_value = (entropy / worst);
        if (debug) LOG.debug("Final:     " + final_value);
        return (final_value);
        // return (Math.abs(1.0d - (entropy - worst) / (best - worst)));
    }
}