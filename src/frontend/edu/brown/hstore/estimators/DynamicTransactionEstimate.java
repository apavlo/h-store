package edu.brown.hstore.estimators;


/**
 * Special estimation type that can be dynamically calculated as the
 * transaction runs
 * @author pavlo
 */
public interface DynamicTransactionEstimate extends Estimate {

    // ----------------------------------------------------------------------------
    // SINGLE-PARTITION PROBABILITY
    // ----------------------------------------------------------------------------
    public float getSinglePartitionProbability();
    public void addSinglePartitionProbability(float probability);
    public void setSinglePartitionProbability(float probability);
    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    public float getReadOnlyProbability(int partition);
    public void addReadOnlyProbability(int partition, float probability);
    public void setReadOnlyProbability(int partition, float probability);
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    public float getWriteProbability(int partition);
    public void addWriteProbability(int partition, float probability);
    public void setWriteProbability(int partition, float probability);
    
    // ----------------------------------------------------------------------------
    // FINISH PROBABILITY
    // ----------------------------------------------------------------------------
    public float getFinishProbability(int partition);
    public void addFinishProbability(int partition, float probability);
    public void setFinishProbability(int partition, float probability);
    
    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    public float getAbortProbability();
    public void addAbortProbability(float probability);
    public void setAbortProbability(float probability);
}
