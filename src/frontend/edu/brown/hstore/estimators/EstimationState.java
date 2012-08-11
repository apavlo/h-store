package edu.brown.hstore.estimators;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.catalog.Statement;
import org.voltdb.utils.EstTime;

import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovPathEstimator;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;

public abstract class EstimationState implements Poolable {

    protected final int num_partitions;
    
    protected Long txn_id = null;
    protected int base_partition;
    protected long start_time;
    
    protected final PartitionSet touched_partitions = new PartitionSet();
    protected final Map<Statement, Integer> query_instance_cnts = new HashMap<Statement, Integer>();
    
    /**
     * Constructor
     * @param markov - the graph that this txn is using
     * @param estimated_path - the initial path estimation from MarkovPathEstimator
     */
    protected EstimationState(int num_partitions) {
        this.num_partitions = num_partitions;
    }
    
    public void init(Long txn_id, int base_partition, long start_time) {
        this.txn_id = txn_id;
        this.base_partition = base_partition;
        this.start_time = start_time;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.txn_id != null);
    }
    
    @Override
    public void finish() {
        this.touched_partitions.clear();
        this.query_instance_cnts.clear();
        this.txn_id = null;

    }
    
    public long getTransactionId() {
        return (this.txn_id);
    }
    public int getBasePartition() {
        return (this.base_partition);
    }
    public long getStartTime() {
        return (this.start_time);
    }
    public PartitionSet getTouchedPartitions() {
        return (this.touched_partitions);
    }
    
    public abstract Estimation getLastEstimate();
    
    /**
     * Get the number of milli-seconds that have passed since the txn started
     * @return
     */
    public long getExecutionTimeOffset() {
        return (EstTime.currentTimeMillis() - this.start_time);
    }
    
    public long getExecutionTimeOffset(long stop) {
        return (stop - this.start_time);
    }
    
    public int updateQueryInstanceCount(Statement catalog_stmt) {
        Integer cnt = this.query_instance_cnts.get(catalog_stmt);
        if (cnt == null) cnt = 0;
        this.query_instance_cnts.put(catalog_stmt, cnt.intValue() + 1);
        return (cnt.intValue());
    }

}
