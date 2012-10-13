package edu.brown.hstore.estimators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.EstTime;

import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

public abstract class EstimatorState implements Poolable {

    public class CountedStatement {
        public Statement statement;
        public int counter;
        
        public CountedStatement(Statement statement, int counter) {
            this.statement = statement;
            this.counter = counter;
        }
    }
    
    protected final CatalogContext catalogContext;
    protected final PartitionSet touched_partitions = new PartitionSet();
    protected final Map<Statement, Integer> query_instance_cnts = new HashMap<Statement, Integer>();
    protected final List<CountedStatement> prefetchable_stmts = new ArrayList<CountedStatement>();

    protected Long txn_id = null;
    protected int base_partition;
    protected long start_time;
    
    protected final List<TransactionEstimate> estimates = new ArrayList<TransactionEstimate>();
    protected int num_estimates = 0;
    
    /**
     * Constructor
     * @param markov - the graph that this txn is using
     * @param estimated_path - the initial path estimation from MarkovPathEstimator
     */
    protected EstimatorState(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
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
        this.num_estimates = 0;
        this.touched_partitions.clear();
        this.query_instance_cnts.clear();
        this.prefetchable_stmts.clear();
        this.txn_id = null;
    }
    
    public Long getTransactionId() {
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
    public List<CountedStatement> getPrefetchableStatements() {
        return (this.prefetchable_stmts);
    }
    
    /**
     * Get the number of TransactionEstimates generated for this transaction
     * This count includes the initial estimate
     * @return
     */
    public int getEstimateCount() {
        return (this.num_estimates);
    }
    /**
     * Return the full list of estimates generated for this transaction
     * <B>NOTE:</B> This should only be used for testing 
     * @return
     */
    public List<TransactionEstimate> getEstimates() {
        return (Collections.unmodifiableList(this.estimates.subList(1, this.num_estimates)));
    }
    
    // ----------------------------------------------------------------------------
    // API METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the initial TransactionEstimate made for this transaction 
     * before it began execution
     * @return
     */
    @SuppressWarnings("unchecked")
    public final <T extends TransactionEstimate> T getInitialEstimate() {
        return ((T)this.estimates.get(0));
    }

    /**
     * Return the last TransactionEstimate made for this transaction
     * If no new estimate has been made, then it should return the
     * initial estimate
     * @return
     */
    @SuppressWarnings("unchecked")
    public final <T extends TransactionEstimate> T getLastEstimate() {
        return (T)this.estimates.get(this.num_estimates-1);
    }

    protected void addEstimate(TransactionEstimate est) {
        this.num_estimates++;
        this.estimates.add(est);
    }
    
    public int getNumEstimates() {
        return (this.num_estimates);
    }
    
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
    
    @Override
    public String toString() {
        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
        m0.put("TransactionId", this.txn_id);
        m0.put("Base Partition", this.base_partition);
        m0.put("Touched Partitions", this.touched_partitions);
        m0.put("Start Time", this.start_time);
        m0.put("Prefetchable Statements", this.prefetchable_stmts);
        return StringUtil.formatMaps(m0);
    }

    public void addPrefetchableStatement(Statement statement, int counter) {
        this.prefetchable_stmts.add(new CountedStatement(statement, counter));
    }
}
