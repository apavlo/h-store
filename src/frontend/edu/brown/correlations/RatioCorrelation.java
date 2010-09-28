package edu.brown.correlations;

public class RatioCorrelation extends AbstractCorrelation {
    private int num_equal = 0;
    

    /**
     * Constructor
     */
    public RatioCorrelation() {
        // Nothing for now...
    }

    /**
     * Remove all entries in this RatioCorrelation instance
     */
    public void clear() {
        super.clear();
        this.num_equal = 0;
    }
    
    /**
     * Add two values to be correlated with each other
     * 
     * @param x
     * @param y
     */
    public synchronized <K extends Number, V extends Number> void addOccurrence(K x, V y) {
        if (x == null) return;
        if (x.equals(y)) this.num_equal++;
        this.num_entries++;
        this.last_calculation = null;
    }

    /**
     * 
     */
    public synchronized Double calculate() {
        if (this.last_calculation != null || this.num_entries == 0) return (this.last_calculation);
        this.last_calculation = this.num_equal / (double)this.num_entries;
        return (this.last_calculation);
    }
}
