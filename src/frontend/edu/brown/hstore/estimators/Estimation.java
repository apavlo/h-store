package edu.brown.hstore.estimators;

import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.PartitionSet;

public interface Estimation {

    public boolean isValid();
    
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
    public boolean isReadOnlyAllPartitions(EstimationThresholds t);
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition);
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    public void addWriteProbability(int partition, float probability);
    public void setWriteProbability(int partition, float probability);
    public float getWriteProbability(int partition);
    public boolean isWriteProbabilitySet(int partition);
    public PartitionSet getWritePartitions(EstimationThresholds t);
    public boolean isWritePartition(EstimationThresholds t, int partition);
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------
    public void addDoneProbability(int partition, float probability);
    public void setDoneProbability(int partition, float probability);
    public float getDoneProbability(int partition);
    public boolean isDoneProbabilitySet(int partition);
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
