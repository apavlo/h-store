package edu.brown.mappings;

public abstract class AbstractMapping {
    protected int num_entries = 0;
    protected Double last_calculation = null;

    /**
     * Returns the number of elements occurences add to this instance
     * @return
     */
    public int size() {
        return (this.num_entries);
    }

    /**
     * Remove all entry data
     */
    public void clear() {
        this.last_calculation = null;
        this.num_entries = 0;
    }
    
    /**
     * Add two values to be correlated with each other
     * @param x
     * @param y
     */
    public abstract <K extends Number, V extends Number> void addOccurrence(K x, V y);
    
    /**
     * Calculate the correlation factor for the given data entries
     * @return
     */
    public abstract Double calculate();
    
    
    @Override
    public String toString() {
        String ret = this.getClass().getSimpleName() + 
                     "[# of Entries=" + this.num_entries + ", Last Calculation=" + this.last_calculation + "]";
        return (ret);
    }
}
