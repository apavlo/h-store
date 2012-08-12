package edu.brown.hstore.estimators;

import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.PartitionSet;

public interface Estimation {

    public boolean isValid();
    
    /**
     * Get the partitions that this transaction will need to read/write data on 
     * @param t
     */
    public PartitionSet getTouchedPartitions(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // SINGLE-PARTITION PROBABILITY
    // ----------------------------------------------------------------------------
    public void addSingleSitedProbability(float probability);
    public void setSingleSitedProbability(float probability);
    public float getSingleSitedProbability();
    public boolean isSingleSitedProbabilitySet();
    public boolean isSinglePartition(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    public void addReadOnlyProbability(int partition, float probability);
    public void setReadOnlyProbability(int partition, float probability);
    public float getReadOnlyProbability(int partition);
    public boolean isReadOnlyProbabilitySet(int partition);
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition);
    public boolean isReadOnlyAllPartitions(EstimationThresholds t);
    
    /**
     * Get the partitions that this transaction will only read from
     * @param t
     */
    public PartitionSet getReadOnlyPartitions(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    public void addWriteProbability(int partition, float probability);
    public void setWriteProbability(int partition, float probability);
    public float getWriteProbability(int partition);
    public boolean isWriteProbabilitySet(int partition);
    public boolean isWritePartition(EstimationThresholds t, int partition);
    /**
     * Get the partitions that this transaction will write to
     * @param t
     */
    public PartitionSet getWritePartitions(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------
    public void addDoneProbability(int partition, float probability);
    public void setDoneProbability(int partition, float probability);
    public float getDoneProbability(int partition);
    public boolean isDoneProbabilitySet(int partition);
    public boolean isFinishedPartition(EstimationThresholds t, int partition);
    
    /**
     * Get the partitions that this transaction is finished with at this point in the transaction
     * @param t
     */
    public PartitionSet getFinishedPartitions(EstimationThresholds t);

    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    public void addAbortProbability(float probability);
    public void setAbortProbability(float probability);
    public float getAbortProbability();
    public boolean isAbortProbabilitySet();
    public boolean isAbortable(EstimationThresholds t);
}
