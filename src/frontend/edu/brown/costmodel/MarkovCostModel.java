package edu.brown.costmodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.Vertex;
import edu.brown.markov.TransactionEstimator.Estimate;
import edu.brown.markov.TransactionEstimator.State;
import edu.brown.markov.Vertex.Type;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.Workload.Filter;

public class MarkovCostModel extends AbstractCostModel {
    private static final Logger LOG = Logger.getLogger(MarkovCostModel.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Cost Model Penalties
     */
    public enum Penalty {
        /**
         * The transaction is single-partitioned and it aborts when we predicted that it wouldn't.
         * Evan says that this is the worst!
         */
        MISSED_ABORT_SINGLE             (0.0d),
        /**
         * The transaction is multi-partitioned and it aborts when we predicted that it wouldn't. 
         */
        MISSED_ABORT_MULTI              (0.0d),
        /**
         * The transaction did not declare it would read at a partition 
         */
        MISSING_READ_PARTITION          (0.0d),
        /**
         * The transaction did not declare it would write at a partition 
         */
        MISSING_WRITE_PARTITION         (0.0d),
        /**
         * The transaction goes back to read at a partition after it declared it was done with it
         */
        RETURN_READ_PARTITION           (0.0d),
        /**
         * The transaction goes back to write at a partition after it declared it was done with it
         */
        RETURN_WRITE_PARTITION          (0.0d),
        /**
         * The transaction said it was going to read at a partition but it never did
         * And it would have executed as single-partitioned if we didn't say it was going to!
         */
        UNUSED_READ_PARTITION_SINGLE    (0.0d),
        /**
         * The transaction said it was going to write at a partition but it never did
         * And it would have executed as single-partitioned if we didn't say it was going to!
         */
        UNUSED_WRITE_PARTITION_SINGLE   (0.0d),
        /**
         * The transaction said it was going to read at a partition but it never did
         */
        UNUSED_READ_PARTITION_MULTI     (0.0d),
        /**
         * The transaction said it was going to write at a partition but it never did
         */
        UNUSED_WRITE_PARTITION_MULTI    (0.0d),
        /**
         * The transaction is done with a partition but we don't identify it
         * until later in the execution path
         */
        LATE_DONE_PARTITION             (0.0d),
        ;
        
        private final double cost;
        private Penalty(double cost) {
            this.cost = cost;
        }
        public double getCost() {
            return this.cost;
        }
    }
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    private final EstimationThresholds thresholds;
    
    /**
     * ClusterId -> TransactionEstimator
     */
    private final Map<Integer, TransactionEstimator> t_estimators = new ConcurrentHashMap<Integer, TransactionEstimator>();
    /**
     * If this is set to true, then we will use the txnid_cluster_xref to figure out
     * what cluster each TransactionTrace belongs to
     */
    private boolean txnid_cluster = false;
    /**
     * Hackish cross-reference table to go from the TransactionId to Cluster#
     */
    private final Map<Long, Integer> txnid_cluster_xref = new HashMap<Long, Integer>();
    
    
    // ----------------------------------------------------------------------------
    // INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The list of penalties accrued for this transaction
     */
    private transient final List<Penalty> penalties = new ArrayList<Penalty>();
    
    private transient final Set<Integer> done_partitions = new HashSet<Integer>();
    
    private transient final Set<Integer> e_all_partitions = new HashSet<Integer>();
    private transient final Set<Integer> e_read_partitions = new HashSet<Integer>();
    private transient final Set<Integer> e_write_partitions = new HashSet<Integer>();
    
    private transient final Set<Integer> a_all_partitions = new HashSet<Integer>();
    private transient final Set<Integer> a_read_partitions = new HashSet<Integer>();
    private transient final Set<Integer> a_write_partitions = new HashSet<Integer>();
    
    /**
     * Constructor
     * @param catalog_db
     * @param p_estimator
     */
    public MarkovCostModel(Database catalog_db, PartitionEstimator p_estimator, EstimationThresholds thresholds) {
        super(MarkovCostModel.class, catalog_db, p_estimator);
        this.thresholds = thresholds;
    }

    /**
     * Enable the TransactionId -> ClusterId lookup
     * @param flag
     */
    public void setTransactionClusterMapping(boolean flag) {
        this.txnid_cluster = flag;
    }
    
    /**
     * Add a TransactionEstimator that is mapped to the given cluster_id
     * @param cluster_id
     * @param t_estimator
     */
    public void addTransactionEstimator(int cluster_id, TransactionEstimator t_estimator) {
        this.t_estimators.put(cluster_id, t_estimator);
    }
    
    public TransactionEstimator getTransactionEstimator(int cluster_id) {
        return (this.t_estimators.get(cluster_id));
    }
    
    /**
     * Map a TransactionId to a ClusterId
     * @param txn_id
     * @param cluster_id
     */
    public void addTransactionClusterXref(long txn_id, int cluster_id) {
        this.txnid_cluster_xref.put(txn_id, cluster_id);
    }
    

    @Override
    public AbstractCostModel clone(Database catalogDb) throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized double estimateTransactionCost(Database catalog_db, Workload workload, Filter filter, TransactionTrace txn_trace) throws Exception {
        long txn_id = txn_trace.getTransactionId();
        Procedure catalog_proc = txn_trace.getCatalogItem(catalog_db);
        
        // This will allow us to select the right TransactionEstimator
        Integer cluster = null;
        if (this.txnid_cluster) {
            // Look-up what cluster our TransactionTrace belongs to
            cluster = this.txnid_cluster_xref.get(txn_id);
        } else {
            // Otherwise just use the base partition
            cluster = this.p_estimator.getBasePartition(txn_trace);
        }
        assert(cluster != null) : "Unexpected Txn# " + txn_id;

        TransactionEstimator t_estimator = this.t_estimators.get(cluster);
        if (t_estimator == null) {
            throw new RuntimeException("Unexpected Cluster# " + cluster);
            //if (trace.get()) LOG.warn("Unexpected Cluster# " + cluster);
            //return (1.0);
        }
        assert(t_estimator != null) : "Unexpected Cluster# " + cluster;
        
        // Throw the txn at the estimator and let it come up with the initial path estimation.
        // Now execute the queries and see what path the txn actually takes
        // I don't think it matters whether we do this in batches, but it probably doesn't hurt
        // to do it right in case we need it later
        // At this point we know what the transaction actually would do using the TransactionEstimator's
        // internal Markov models.
        State s = t_estimator.processTransactionTrace(txn_trace);
        assert(s != null);
        
        if (trace.get()) {
            LOG.trace("Estimated: " + s.getEstimatedPath());
            LOG.trace("Actual:    " + s.getActualPath());
        }
        
        double cost = 0.0d;
        
        // Try fast version
        if (!this.comparePathsFast(s.getEstimatedPath(), s.getActualPath())) {
            // Otherwise we have to do the full path comparison to figure out just how wrong we are
            cost = this.comparePathsFull(s, s.getEstimatedPath(), s.getActualPath());
        }
        
        TransactionEstimator.getStatePool().returnObject(s);
        
        return (cost);
    }
       
    /**
     * Quickly compare the two paths and return true if they are similar enough
     * We don't care if they execute different queries just as long as the read/write paritions
     * match and that the estimated path either commits/aborts as same as actual path 
     * @param estimated
     * @param actual
     * @return
     */
    protected boolean comparePathsFast(final List<Vertex> estimated, final List<Vertex> actual) {
        if (trace.get()) LOG.trace(String.format("Fast Path Compare: Estimated [size=%d] vs. Actual Cost [size=%d]", estimated.size(), actual.size()));
        
        // (1) Check that the estimate's last state matches the actual path (commit vs abort) 
        Vertex e_last = CollectionUtil.getLast(estimated);
        assert(e_last != null);
        Vertex a_last = CollectionUtil.getLast(actual);
        assert(a_last != null);
        assert(a_last.isEndingVertex());
        if (e_last.getType() != a_last.getType()) {
            return (false);
        }
        
        // (2) Check that the partitions that we predicted that the txn would read/write are the same
        this.e_read_partitions.clear();
        this.e_write_partitions.clear();
        this.a_write_partitions.clear();
        this.a_write_partitions.clear();

        MarkovUtil.getReadWritePartitions(estimated, this.e_read_partitions, this.e_write_partitions);
        MarkovUtil.getReadWritePartitions(estimated, this.a_read_partitions, this.a_write_partitions);
        if (this.e_read_partitions.equals(this.a_read_partitions) == false ||
            this.e_write_partitions.equals(this.a_write_partitions) == false) {
            return (false);
        }
        
        // All clear!
        return (true);
    }
    
    /**
     * Calculate relative cost difference the estimated and actual execution paths 
     * @param estimated
     * @param actual
     * @return
     */
    protected double comparePathsFull(State s, List<Vertex> estimated, List<Vertex> actual) {
        double cost = 0.0d;
        this.penalties.clear();

        this.e_all_partitions.clear();
        this.e_all_partitions.addAll(this.e_read_partitions);
        this.e_all_partitions.addAll(this.e_write_partitions);
        Vertex e_last = CollectionUtil.getLast(actual);
        assert(e_last != null);
        
        this.a_all_partitions.clear();
        this.a_all_partitions.addAll(this.a_read_partitions);
        this.a_all_partitions.addAll(this.a_write_partitions);
        Vertex a_last = CollectionUtil.getLast(actual);
        assert(a_last != null);
        assert(a_last.isEndingVertex());
        
        Estimate initial_est = s.getInitialEstimate();
        assert(initial_est != null);
        Estimate last_est = s.getLastEstimate();
        assert(last_est != null);

        boolean e_singlepartitioned = initial_est.isSinglePartition(this.thresholds); 
        boolean a_singlepartitioned = (this.a_all_partitions.size() == 1);

        
        // PENALTY #1
        // If the transaction was predicted to be single-partitioned and we don't predict that it's going to
        // abort when it actually did, then that's bad! Really bad!
        if (initial_est.isUserAbort(this.thresholds) == false && a_last.isAbortVertex()) {
            this.penalties.add(a_singlepartitioned ? Penalty.MISSED_ABORT_SINGLE : Penalty.MISSED_ABORT_MULTI);
        }
        
        // PENALTY #2
        // Get the transaction actually reads/writes at more partitions than it originally predicted
        // This is expensive because it means that we have to abort+restart the txn
        for (Integer p : this.a_read_partitions) {
            if (this.e_read_partitions.contains(p) == false)
                this.penalties.add(Penalty.MISSING_READ_PARTITION);
        } // FOR
        for (Integer p : this.a_write_partitions) {
            if (this.e_write_partitions.contains(p) == false)
                this.penalties.add(Penalty.MISSING_WRITE_PARTITION);
        } // FOR
        
        // PENALTY #3
        // Check whether the transaction has declared that they would read/write at a partition
        // but then they never actually did so
        // The penalty is higher if it was predicted as multi-partitioned but it was actually single-partitioned
        boolean could_be_singlepartitioned = (e_singlepartitioned == false && a_singlepartitioned == true);
        for (Integer p : this.e_read_partitions) {
            if (this.a_read_partitions.contains(p) == false)
                this.penalties.add(could_be_singlepartitioned ? Penalty.UNUSED_READ_PARTITION_SINGLE :
                                                                Penalty.UNUSED_READ_PARTITION_MULTI);
        } // FOR
        for (Integer p : this.e_write_partitions) {
            if (this.a_write_partitions.contains(p) == false)
                this.penalties.add(could_be_singlepartitioned ? Penalty.UNUSED_WRITE_PARTITION_SINGLE :
                                                                Penalty.UNUSED_WRITE_PARTITION_MULTI);
        } // FOR
        
        // PENALTY #4
        // We declared that we were done at a partition but then later we actually needed it
        this.done_partitions.clear();
        int num_estimates = s.getEstimateCount();
        List<Estimate> estimates = s.getEstimates();
        for (int i = 0; i < num_estimates; i++) {
            Estimate est = estimates.get(i);
            this.done_partitions.addAll(est.getFinishedPartitions(this.thresholds));
            
            // Check if we read/write at any partition that was previously declared as done
            
            
        }
        
        
        int e_cnt = estimated.size();
        int a_cnt = actual.size();
        for (int e_i = 1, a_i = 1; a_i < a_cnt-1; a_i++) {
            Vertex a = actual.get(a_i);
            Vertex e = null;
            try {
                e = estimated.get(e_i);
            } catch (IndexOutOfBoundsException ex) {
                // IGNORE
            }
        
            if (trace.get()) LOG.trace(String.format("Estimated[%02d]%s <==> Actual[%02d]%s", e_i, e, a_i, a));
            
            if (e != null) {
                Statement e_stmt = e.getCatalogItem();
                Set<Integer> e_partitions = e.getPartitions();
                e_all_partitions.addAll(e_partitions);
                
                Statement a_stmt = a.getCatalogItem();
                Set<Integer> a_partitions = a.getPartitions();
                a_all_partitions.addAll(a_partitions);
                
                // Check whether they're executing the same queries
                if (a_stmt.equals(e_stmt) == false) {
                    if (trace.get()) LOG.trace("STMT MISMATCH: " + e_stmt + " != " + a_stmt);
                    cost += 1;
                // Great, we're getting there. Check whether these queries are
                // going to touch the same partitions
                } else if ((a_partitions.size() != e_partitions.size()) || 
                           (a_partitions.equals(e_partitions) == false)) {
                    if (trace.get()) LOG.trace("PARTITION MISMATCH: " + e_partitions + " != " + a_partitions);
                    
                    // Do we want to penalize them for every partition they get wrong?
                    for (Integer p : a_partitions) {
                        if (e_partitions.contains(p) == false) {
                            cost += 1.0d;
                        }
                    }
                    
                // Ok they have the same query and they're going to the same partitions,
                // So now check whether that this is the same number of times that they've
                // executed this query before 
                } else if (a.getQueryInstanceIndex() != e.getQueryInstanceIndex()) {
                    int a_idx = a.getQueryInstanceIndex();
                    int e_idx = e.getQueryInstanceIndex();
                    if (trace.get()) LOG.trace("QUERY INDEX MISMATCH: " + e_idx + " != " + a_idx);
                    // Do we actually to penalize them for this??
                }
                
                e_i++;
                if (trace.get()) LOG.trace("");
                
            // If our estimated path is too short, then yeah that's going to cost ya'!
            } else {
                cost += 1.0d;
            }
        } // FOR
        
        // Penalize them for invalid partitions
        // One point for every missing one and one point for every one too many
        int p_missing = 0;
        int p_incorrect = 0;
        for (Integer p : a_all_partitions) {
            if (e_all_partitions.contains(p) == false) p_missing++;
        }
        for (Integer p : e_all_partitions) {
            if (a_all_partitions.contains(p) == false) p_incorrect++;
        }
        cost += p_missing + p_incorrect;
        
       
        return (cost);
    }
    
    public double calculateTotalPenalty() {
        double total = 0.0d;
        for (Penalty p : this.penalties) {
            total += p.getCost();
        }
        return (total);
    }
    
    @Override
    public void clear(boolean force) {
        super.clear(force);
        this.t_estimators.clear();
        this.txnid_cluster_xref.clear();
        this.txnid_cluster = false;
        this.penalties.clear();
    }
    
    @Override
    public void invalidateCache(String catalogKey) {
        // Nothing...
    }

    @Override
    public void prepareImpl(Database catalog_db) {
        // This is the start of a new run through the workload, so we need to re-init
        // our PartitionEstimator so that we are getting the proper catalog objects back
        this.p_estimator.initCatalog(catalog_db);
        this.txnid_cluster_xref.clear();
        this.t_estimators.clear();
    }

}