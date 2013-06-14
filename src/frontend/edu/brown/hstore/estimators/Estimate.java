package edu.brown.hstore.estimators;

import java.util.List;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.markov.EstimationThresholds;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;

public interface Estimate extends Poolable {

    /**
     * Returns true if this Estimate is for the initial estimate
     * @return
     */
    public boolean isInitialEstimate();
    
    /**
     * Returns the batch id for this Estimate
     * @return
     */
    public int getBatchId();
    
    /**
     * Returns true if this Estimate is considered valid and can be used by 
     * the runtime system to modify its operations according to its contents.
     * It is up to the implementing classes to decide what it means
     * for it to be valid. 
     * @return
     */
    public boolean isValid();
    
    /**
     * Get the partitions that this transaction will need to execute a query
     * on in the future. 
     * @param t
     */
    public PartitionSet getTouchedPartitions(EstimationThresholds t);
    
    /**
     * Return the amount of time (in ms) that this txn will take before
     * it commits or aborts. If the estimate can't provide this information,
     * then the returned value will be Long.MAX_VALUE.
     * @return
     */
    public long getRemainingExecutionTime();
    
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
//    public boolean isSinglePartitionProbabilitySet();
    
    /**
     * Returns true if the number of partitions that this txn is expected
     * to touch from this estimate forward is one.
     * @param t
     * @return
     */
    public boolean isSinglePartitioned(EstimationThresholds t);
    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
//    public boolean isReadOnlyProbabilitySet(int partition);
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition);
    public boolean isReadOnlyAllPartitions(EstimationThresholds t);
    
//    /**
//     * Get the partitions that this transaction will only read from
//     * @param t
//     */
//    public PartitionSet getReadOnlyPartitions(EstimationThresholds t);
    
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
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------
    public boolean isDoneProbabilitySet(int partition);
    public boolean isDonePartition(EstimationThresholds t, int partition);
    
    /**
     * Get the partitions that this transaction is finished with at this point in the transaction
     * @param t
     */
    public PartitionSet getDonePartitions(EstimationThresholds t);

    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    public boolean isAbortProbabilitySet();
    public boolean isAbortable(EstimationThresholds t);
}
