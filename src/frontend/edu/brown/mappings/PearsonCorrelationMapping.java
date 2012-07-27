package edu.brown.mappings;

import java.util.Vector;


/**
 * 
 * @author svelagap
 * 
 */
public class PearsonCorrelationMapping extends AbstractMapping {
    
    private final Vector<Number> values0 = new Vector<Number>();
    private final Vector<Number> values1 = new Vector<Number>();
    
    /**
     * Constructor
     */
    public PearsonCorrelationMapping() {
        super();
    }

    /**
     * Remove all entries in this PearsonCorrelation instance
     */
    public void clear() {
        this.values0.clear();
        this.values1.clear();
        this.num_entries = 0;
        this.last_calculation = null;
    }
    
    /**
     * Add two values to be correlated with each other
     * 
     * @param x
     * @param y
     */
    @Override
    public synchronized <K extends Number, V extends Number> void addOccurrence(K x, V y) {
        this.values0.add(x);
        this.values1.add(y);
        this.num_entries++;
        this.last_calculation = null;
    }

    /**
     * Algorithm for calculating PearsonCorrelation's r shamelessly stolen from Wikipedia.
     * http://en.wikipedia.org/wiki/PearsonCorrelation_correlation
     * 
     * @return The PearsonCorrelation's r of the two variables associated with this object
     */
    @Override
    public synchronized Double calculate() {
        assert(this.num_entries == this.values0.size());
        assert(this.num_entries == this.values1.size());
        if (this.last_calculation != null || this.num_entries == 0) return (this.last_calculation);
        
        double sum_sq_x = 0;
        double sum_sq_y = 0;
        double sum_coproduct = 0;

        double mean_x = this.values0.get(0).doubleValue();
        double mean_y = this.values1.get(0).doubleValue();
        
        for (int i = 2; i <= this.num_entries; i++) {
            double x = this.values0.get(i - 1).doubleValue();
            double y = this.values1.get(i - 1).doubleValue();

            double sweep = (i - 1.0) / i;
            double delta_x = x - mean_x;
            double delta_y = y - mean_y;
            // System.out.println("first " + next + "\n second " + this.get(next));
            sum_sq_x += delta_x * delta_x * sweep;
            sum_sq_y += delta_y * delta_y * sweep;
            sum_coproduct += delta_x * delta_y * sweep;
            mean_x += delta_x / i;
            mean_y += delta_y / i;    
            // System.out.println("mean_x " + mean_x + "\n mean_y " + mean_y);
        } // FOR
        
        double pop_sd_x = Math.sqrt(sum_sq_x / this.num_entries);
        double pop_sd_y = Math.sqrt(sum_sq_y / this.num_entries);
        double cov_x_y = sum_coproduct / this.num_entries;
        
        // Special Case: Single-point (always the same values)
        if (pop_sd_x == 0 && pop_sd_y == 0) {
            this.last_calculation = 1.0d;
        // Special Case: Horizontal Line
        } else if (pop_sd_y == 0) {
            
        // Special Case: Vertical Line
        } else if (pop_sd_y == 0) {
            
            
        } else {
            this.last_calculation = cov_x_y / (pop_sd_x * pop_sd_y); // Correlation;    
        }
        
        return (this.last_calculation);
    }
    
    @Override
    public String toString() {
        String ret = this.getClass().getSimpleName() + 
                     "[# of Entries=" + this.values0.size() + ", Last Calculation=" + this.last_calculation + "]";
        return (ret);
    }
}
