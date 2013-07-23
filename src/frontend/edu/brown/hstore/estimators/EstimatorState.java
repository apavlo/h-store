package edu.brown.hstore.estimators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.EstTime;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.pools.Poolable;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

public abstract class EstimatorState implements Poolable {

    protected final CatalogContext catalogContext;
    protected final PartitionSet touched_partitions = new PartitionSet();
    protected final Map<Statement, Integer> query_instance_cnts = new HashMap<Statement, Integer>();
    protected final List<CountedStatement> prefetchable_stmts = new ArrayList<CountedStatement>();

    protected Long txn_id = null;
    protected int base_partition;
    protected long start_time;
    
    private Estimate initialEstimate;
    private final List<Estimate> estimates = new ArrayList<Estimate>();
    private boolean shouldAllowUpdates = false;
    private boolean allowUpdates = true;
    
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
        this.initialEstimate = null;
        this.shouldAllowUpdates = false;
        this.allowUpdates = true;
        for (Estimate estimate : this.estimates) {
            if (estimate != null) estimate.finish();
        } // FOR
        this.estimates.clear();
        
        this.touched_partitions.clear();
        this.query_instance_cnts.clear();
        this.prefetchable_stmts.clear();
        this.txn_id = null;
    }
    
    public final Long getTransactionId() {
        return (this.txn_id);
    }
    public final int getBasePartition() {
        return (this.base_partition);
    }
    public final long getStartTime() {
        return (this.start_time);
    }
    public PartitionSet getTouchedPartitions() {
        return (this.touched_partitions);
    }
    public List<CountedStatement> getPrefetchableStatements() {
        return (this.prefetchable_stmts);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION ESTIMATES
    // ----------------------------------------------------------------------------
    
    /**
     * Returns true if the TransactionEstimator thinks that the PartitionExecutor
     * should provide it with updates about txns.
     * @return
     */
    public final boolean shouldAllowUpdates() {
        return (this.shouldAllowUpdates);
    }
    protected void shouldAllowUpdates(boolean enable) {
        this.shouldAllowUpdates = enable;
    }
    
    public final void disableUpdates() {
        this.allowUpdates = false;
    }
    
    public final void enableUpdates() {
        this.allowUpdates = true;
    }
    
    public final boolean isUpdatesEnabled() {
        return (this.allowUpdates);
    }
    
    protected void addInitialEstimate(Estimate estimate) {
        assert(this.initialEstimate == null);
        this.initialEstimate = estimate;
    }

    protected Estimate addEstimate(Estimate est) {
//        assert(this.initialEstimate != null) : 
//            "Trying to add a new estimate before the initial estimate";
        assert(est.isInitialized());
        this.estimates.add(est);
        return (est);
    }
    
    /**
     * Get the number of TransactionEstimates generated for this transaction
     * This count does <B>not</B> include the initial estimate
     * @return
     */
    public int getEstimateCount() {
        return (this.estimates.size());
    }
    
    /**
     * Return the full list of estimates generated for this transaction
     * <B>NOTE:</B> This should only be used for testing 
     * @return
     */
    public List<Estimate> getEstimates() {
        return (this.estimates);
    }
    
    /**
     * Return the initial TransactionEstimate made for this transaction 
     * before it began execution
     * @return
     */
    @SuppressWarnings("unchecked")
    public final <T extends Estimate> T getInitialEstimate() {
        return ((T)this.initialEstimate);
    }

    /**
     * Return the last TransactionEstimate made for this transaction
     * If no new estimate has been made, then it should return the
     * initial estimate
     * @return
     */
    @SuppressWarnings("unchecked")
    public final <T extends Estimate> T getLastEstimate() {
        if (this.estimates.isEmpty()) {
            return (T)this.initialEstimate;
        }
        return ((T)CollectionUtil.last(this.estimates));
    }
    
    // ----------------------------------------------------------------------------
    // API METHODS
    // ----------------------------------------------------------------------------
    
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

    public void addPrefetchableStatement(CountedStatement cntStmt) {
        this.prefetchable_stmts.add(cntStmt);
    }
}
