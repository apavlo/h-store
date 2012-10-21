package edu.brown.hstore.estimators;


import java.util.List;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.markov.EstimationThresholds;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;

public interface Estimate extends Poolable {

    /**
     * Returns true if this TransactionEstimate is considered valid
     * It is up to the implementing classes to descide what it means
     * for it to be valid. 
     * @return
     */
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
     * Returns true if this estimate has a list of Statements that the transaction 
     * will execute on the given partition
     * @param partition TODO
     * @return
     */
    public boolean hasQueryEstimate(int partition);
    
    /**
     * Return a list of CountedStatement handles that the transaction is likely 
     * to execute on the given partition.
     * If there are no queries that need to execute on the given partition, then
     * the returned list will be empty.
     * @return
     */
    public List<CountedStatement> getQueryEstimate(int partition);
    
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
