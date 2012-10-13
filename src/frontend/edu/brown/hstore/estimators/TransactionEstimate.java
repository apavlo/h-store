package edu.brown.hstore.estimators;

import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.PartitionSet;

public interface TransactionEstimate {

    public boolean isValid();
    
    /**
     * Get the partitions that this transaction will need to read/write data on 
     * @param t
     */
    public PartitionSet getTouchedPartitions(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // QUERIES
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if this estimate contains a list of queries
     * that the transaction will execute
     * @return
     */
    public boolean hasQueryList();
    
//    public Statement getEstimatedQueries();
    
    // ----------------------------------------------------------------------------
    // SINGLE-PARTITION PROBABILITY
    // ----------------------------------------------------------------------------
    public boolean isSinglePartitionProbabilitySet();
    public boolean isSinglePartitioned(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
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
    public boolean isWriteProbabilitySet(int partition);
    public boolean isWritePartition(EstimationThresholds t, int partition);
    /**
     * Get the partitions that this transaction will write to
     * @param t
     */
    public PartitionSet getWritePartitions(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // FINISH PROBABILITY
    // ----------------------------------------------------------------------------
    public boolean isFinishProbabilitySet(int partition);
    public boolean isFinishPartition(EstimationThresholds t, int partition);
    
    /**
     * Get the partitions that this transaction is finished with at this point in the transaction
     * @param t
     */
    public PartitionSet getFinishPartitions(EstimationThresholds t);

    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    public boolean isAbortProbabilitySet();
    public boolean isAbortable(EstimationThresholds t);
}
