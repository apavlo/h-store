package edu.brown.markov;

public interface Estimation {

    // ----------------------------------------------------------------------------
    // SINGLE-SITED PROBABILITY
    // ----------------------------------------------------------------------------
    public void addSingleSitedProbability(float probability);
    public void setSingleSitedProbability(float probability);
    public float getSingleSitedProbability();
    public boolean isSingleSitedProbabilitySet();
    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    public void addReadOnlyProbability(int partition, float probability);
    public void setReadOnlyProbability(int partition, float probability);
    public float getReadOnlyProbability(int partition);
    public boolean isReadOnlyProbabilitySet(int partition);
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    public void addWriteProbability(int partition, float probability);
    public void setWriteProbability(int partition, float probability);
    public float getWriteProbability(int partition);
    public boolean isWriteProbabilitySet(int partition);
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------
    public void addDoneProbability(int partition, float probability);
    public void setDoneProbability(int partition, float probability);
    public float getDoneProbability(int partition);
    public boolean isDoneProbabilitySet(int partition);

    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    public void addAbortProbability(float probability);
    public void setAbortProbability(float probability);
    public float getAbortProbability();
    public boolean isAbortProbabilitySet();
}
