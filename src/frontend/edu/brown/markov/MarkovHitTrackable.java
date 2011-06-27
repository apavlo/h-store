package edu.brown.markov;

public interface MarkovHitTrackable {
    
    /**
     * Set the totalhits for this Vertex to be the current value of instancehits
     * and then reset instancehits
     */
    public void applyInstanceHitsToTotalHits();
    /**
     * 
     * @param delta
     */
    public void incrementTotalHits();

    /**
     * 
     * @return
     */
    public long getTotalHits();
    
    /**
     * Set the number of instance hits, useful for testing
     */
    public void setInstanceHits(int instancehits);

    /**
     * 
     * @return
     */
    public int getInstanceHits();

    /**
     * @return TODO
     * 
     */
    public int incrementInstanceHits();
       
}
