package edu.brown.costmodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraphsContainer;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.Vertex;
import edu.brown.markov.TransactionEstimator.State;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
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

    public enum PenaltyGroup {
        MISSED_ABORT,
        MISSING_PARTITION,
        RETURN_PARTITION,
        UNUSED_PARTITION,
        LATE_DONE;
    }
    
    /**
     * Cost Model Penalties
     */
    public enum Penalty {
        // ----------------------------------------------------------------------------
        // PENALTY #1
        // ----------------------------------------------------------------------------
        /**
         * The transaction is single-partitioned and it aborts when we predicted that it wouldn't.
         * Evan says that this is the worst!
         */
        MISSED_ABORT_SINGLE             (PenaltyGroup.MISSED_ABORT, 1.0d),
        /**
         * The transaction is multi-partitioned and it aborts when we predicted that it wouldn't. 
         */
        MISSED_ABORT_MULTI              (PenaltyGroup.MISSED_ABORT, 0.8d),
        
        // ----------------------------------------------------------------------------
        // PENALTY #2
        // ----------------------------------------------------------------------------
        /**
         * The transaction did not declare it would read at a partition 
         */
        MISSING_READ_PARTITION          (PenaltyGroup.MISSING_PARTITION, 0.5d),
        /**
         * The transaction did not declare it would write at a partition 
         */
        MISSING_WRITE_PARTITION         (PenaltyGroup.MISSING_PARTITION, 0.5d),
        
        // ----------------------------------------------------------------------------
        // PENALTY #3
        // ----------------------------------------------------------------------------
        /**
         * The transaction goes back to read at a partition after it declared it was done with it
         */
        RETURN_READ_PARTITION           (PenaltyGroup.RETURN_PARTITION, 0.25d),
        /**
         * The transaction goes back to write at a partition after it declared it was done with it
         */
        RETURN_WRITE_PARTITION          (PenaltyGroup.RETURN_PARTITION, 0.25d),
        
        // ----------------------------------------------------------------------------
        // PENALTY #4
        // ----------------------------------------------------------------------------
        /**
         * The transaction said it was going to read at a partition but it never did
         * And it would have executed as single-partitioned if we didn't say it was going to!
         */
        UNUSED_READ_PARTITION_SINGLE    (PenaltyGroup.UNUSED_PARTITION, 0.5d),
        /**
         * The transaction said it was going to write at a partition but it never did
         * And it would have executed as single-partitioned if we didn't say it was going to!
         */
        UNUSED_WRITE_PARTITION_SINGLE   (PenaltyGroup.UNUSED_PARTITION, 0.5d),
        /**
         * The transaction said it was going to read at a partition but it never did
         */
        UNUSED_READ_PARTITION_MULTI     (PenaltyGroup.UNUSED_PARTITION, 0.1d),
        /**
         * The transaction said it was going to write at a partition but it never did
         */
        UNUSED_WRITE_PARTITION_MULTI    (PenaltyGroup.UNUSED_PARTITION, 0.1d),
        
        // ----------------------------------------------------------------------------
        // PENALTY #5
        // ----------------------------------------------------------------------------
        /**
         * The transaction is done with a partition but we don't identify it
         * until later in the execution path
         */
        LATE_DONE_PARTITION             (PenaltyGroup.LATE_DONE, 0.05d),
        ;
        
        private final double cost;
        private final PenaltyGroup group;
        private Penalty(PenaltyGroup group, double cost) {
            this.group = group;
            this.cost = cost;
        }
        public double getCost() {
            return this.cost;
        }
        public PenaltyGroup getGroup() {
            return this.group;
        }
    }
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    private final EstimationThresholds thresholds;
    private final TransactionEstimator t_estimator;
    private final List<Integer> all_partitions;
    
    // ----------------------------------------------------------------------------
    // INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The list of penalties accrued for this transaction
     */
    private transient final List<Penalty> penalties = new ArrayList<Penalty>();
    
    private transient final Set<Integer> done_partitions = new HashSet<Integer>();
    private transient final Map<Integer, Integer> idle_partition_ctrs = new HashMap<Integer, Integer>();
    
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
    public MarkovCostModel(Database catalog_db, PartitionEstimator p_estimator, TransactionEstimator t_estimator, EstimationThresholds thresholds) {
        super(MarkovCostModel.class, catalog_db, p_estimator);
        this.thresholds = thresholds;
        this.t_estimator = t_estimator;
        this.all_partitions = CatalogUtil.getAllPartitionIds(catalog_db);
        
        assert(this.t_estimator != null) : "Missing TransactionEstimator";
    }

    /**
     * Get the penalties for the last TransactionTrace processed
     * Not thread-safe
     * @return
     */
    protected List<Penalty> getLastPenalties() {
        return this.penalties;
    }
    protected Set<Integer> getLastEstimatedAllPartitions() {
        return (this.e_all_partitions);
    }
    protected Set<Integer> getLastEstimatedReadPartitions() {
        return (this.e_read_partitions);
    }
    protected Set<Integer> getLastEstimatedWritePartitions() {
        return (this.e_write_partitions);
    }
    protected Set<Integer> getLastActualAllPartitions() {
        return (this.a_all_partitions);
    }
    protected Set<Integer> getLastActualReadPartitions() {
        return (this.a_read_partitions);
    }
    protected Set<Integer> getLastActualWritePartitions() {
        return (this.a_write_partitions);
    }


    @Override
    public synchronized double estimateTransactionCost(Database catalog_db, Workload workload, Filter filter, TransactionTrace txn_trace) throws Exception {
        // Throw the txn at the estimator and let it come up with the initial path estimation.
        // Now execute the queries and see what path the txn actually takes
        // I don't think it matters whether we do this in batches, but it probably doesn't hurt
        // to do it right in case we need it later
        // At this point we know what the transaction actually would do using the TransactionEstimator's
        // internal Markov models.
        State s = this.t_estimator.processTransactionTrace(txn_trace);
        assert(s != null);
        
        if (trace.get()) {
            LOG.trace("Estimated: " + s.getEstimatedPath());
            LOG.trace("Actual:    " + s.getActualPath());
        }
        
        double cost = 0.0d;
        
        // Try fast version
        if (!this.comparePathsFast(s.getEstimatedPath(), s.getActualPath())) {
            if (debug.get()) LOG.info("Fast Comparsion Failed!");
            // Otherwise we have to do the full path comparison to figure out just how wrong we are
            cost = this.comparePathsFull(s);
        }
        
//        if (cost > 0) {
////            System.err.println(txn_trace.debug(catalog_db));
//            System.err.println("COST = " + cost);
//            System.err.println("PENALTIES = " + this.penalties);
//            System.err.println("ESTIMATED PARTITIONS: " + this.e_all_partitions);
//            System.err.println("ACTUAL PARTITIONS: " + this.a_all_partitions);
//            System.err.println();
//            
//            String e_path = "ESTIMATED PATH:\n" + StringUtil.join("\n", s.getEstimatedPath());
//            String a_path = "ACTUAL PATH:\n" + StringUtil.join("\n", s.getActualPath());
//            System.err.println(StringUtil.columns(e_path, a_path));
//            
////            for (MarkovEstimate est : s.getEstimates()) {
////                System.err.println(est + "\n" + est.getVertex().debug() + "\n" + StringUtil.SINGLE_LINE);
////            }
//            
//            throw new RuntimeException("We're fucked");
//        }

        
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
        final boolean t = trace.get();
        if (t) LOG.trace(String.format("Fast Path Compare: Estimated [size=%d] vs. Actual [size=%d]", estimated.size(), actual.size()));
        
        // (1) Check that the MarkovEstimate's last state matches the actual path (commit vs abort) 
        Vertex e_last = CollectionUtil.getLast(estimated);
        assert(e_last != null);
        Vertex a_last = CollectionUtil.getLast(actual);
        assert(a_last != null);
        assert(a_last.isEndingVertex());
        if (t) {
            LOG.trace("Estimated Last Vertex: " + e_last);
            LOG.trace("Actual Last Vertex: " + a_last);
        }
        if (e_last.getType() != a_last.getType()) {
            return (false);
        }
        
        // (2) Check that the partitions that we predicted that the txn would read/write are the same
        this.e_read_partitions.clear();
        this.e_write_partitions.clear();
        this.a_read_partitions.clear();
        this.a_write_partitions.clear();

        MarkovUtil.getReadWritePartitions(estimated, this.e_read_partitions, this.e_write_partitions);
        MarkovUtil.getReadWritePartitions(actual, this.a_read_partitions, this.a_write_partitions);
        
        if (t) {
            LOG.trace("Estimated Read Partitions:  " + this.e_read_partitions);
            LOG.trace("Estimated Write Partitions: " + this.e_write_partitions);
            LOG.trace("Actual Read Partitions:     " + this.a_read_partitions);
            LOG.trace("Actual Write Partitions:    " + this.a_write_partitions);
        }
        
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
    protected double comparePathsFull(State s) {
        final boolean t = trace.get();
        final boolean d = debug.get();
        
        if (d) LOG.debug("Performing full comparison of Transaction #" + s.getTransactionId());
        
        this.penalties.clear();

        List<Vertex> estimated = s.getEstimatedPath();
        this.e_all_partitions.clear();
        this.e_all_partitions.addAll(this.e_read_partitions);
        this.e_all_partitions.addAll(this.e_write_partitions);
        Vertex e_last = CollectionUtil.getLast(estimated);
        assert(e_last != null);
        
        List<Vertex> actual = s.getActualPath();
        this.a_all_partitions.clear();
        this.a_all_partitions.addAll(this.a_read_partitions);
        this.a_all_partitions.addAll(this.a_write_partitions);
        Vertex a_last = CollectionUtil.getLast(actual);
        assert(a_last != null);
        assert(a_last.isEndingVertex());
        
        MarkovEstimate initial_est = s.getInitialEstimate();
        assert(initial_est != null);
        MarkovEstimate last_est = s.getLastEstimate();
        assert(last_est != null);

        boolean e_singlepartitioned = initial_est.isSinglePartition(this.thresholds); 
        boolean a_singlepartitioned = (this.a_all_partitions.size() == 1);
        
        boolean first_penalty = true;

        // ----------------------------------------------------------------------------
        // PENALTY #1
        // If the transaction was predicted to be single-partitioned and we don't predict that it's going to
        // abort when it actually did, then that's bad! Really bad!
        // ----------------------------------------------------------------------------
        first_penalty = true;
        if (initial_est.isUserAbort(this.thresholds) == false && a_last.isAbortVertex()) {
            if (t) {
                if (first_penalty) {
                    LOG.trace("PENALTY #1: " + PenaltyGroup.MISSED_ABORT);
                    first_penalty = false;
                }
                LOG.trace(String.format("Txn #%d aborts but we predicted that it would never!", s.getTransactionId()));
            }
            this.penalties.add(a_singlepartitioned ? Penalty.MISSED_ABORT_SINGLE : Penalty.MISSED_ABORT_MULTI);
        }
        
        // ----------------------------------------------------------------------------
        // PENALTY #2
        // The transaction actually reads/writes at more partitions than it originally predicted
        // This is expensive because it means that we have to abort+restart the txn
        // ----------------------------------------------------------------------------
        first_penalty = true;
        for (Integer p : this.a_read_partitions) {
            if (this.e_read_partitions.contains(p) == false) {
                if (t) {
                    if (first_penalty) {
                        LOG.trace("PENALTY #2: " + PenaltyGroup.MISSING_PARTITION);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d failed to predict that it was READING at partition %d", s.getTransactionId(), p));
                }
                this.penalties.add(Penalty.MISSING_READ_PARTITION);
            }
        } // FOR
        for (Integer p : this.a_write_partitions) {
            if (this.e_write_partitions.contains(p) == false) {
                if (t) {
                    if (first_penalty) {
                        LOG.trace("PENALTY #2: " + PenaltyGroup.MISSING_PARTITION);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d failed to predict that it was WRITING at partition %d", s.getTransactionId(), p));
                }
                this.penalties.add(Penalty.MISSING_WRITE_PARTITION);
            }
        } // FOR
        
        // ----------------------------------------------------------------------------
        // PENALTY #3
        // We declared that we were done at a partition but then later we actually needed it
        // This can happen if there is a path that a has very low probability of us taking it, but then
        // ended up taking it anyway
        //
        // PENALTY #5
        // We keep track of the last batch round that we finished with a partition. We then
        // count how long it takes before we realize that we are finished. We declare that the MarkovEstimate
        // was late if we don't mark it as finished immediately in the next batch
        // ----------------------------------------------------------------------------
        first_penalty = true;
        boolean first_penalty5 = true;
        
        this.done_partitions.clear();
        int num_estimates = s.getEstimateCount();
        List<MarkovEstimate> estimates = s.getEstimates();
        int last_est_idx = 0;
        Set<Integer> touched_partitions = new HashSet<Integer>();
        Set<Integer> new_touched_partitions = new HashSet<Integer>();

        // Reset the idle counters
        for (Integer p : this.all_partitions) {
            this.idle_partition_ctrs.put(p, 0);
        }
        
        for (int i = 0; i < num_estimates; i++) {
            MarkovEstimate est = estimates.get(i);
            Vertex est_v = est.getVertex();
            
            // Get the path of vertices
            int start = last_est_idx;
            int stop = actual.indexOf(est_v);
            assert(stop != -1);
            
            new_touched_partitions.clear();
            for ( ; start <= stop; start++) {
                Vertex v = actual.get(start);
                assert(v != null);
                
                Statement catalog_stmt = v.getCatalogItem();
                QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
                Penalty ptype = (qtype == QueryType.SELECT ? Penalty.RETURN_READ_PARTITION : Penalty.RETURN_WRITE_PARTITION);
                for (Integer p : v.getPartitions()) {
                    // Check if we read/write at any partition that was previously declared as done
                    if (this.done_partitions.contains(p)) {
                        if (t) {
                            if (first_penalty) {
                                LOG.trace("PENALTY #3: " + PenaltyGroup.RETURN_PARTITION);
                                first_penalty = false;
                            }
                            LOG.trace(String.format("Txn #%d said that it was done at partition %d but it executed a %s", s.getTransactionId(), p, qtype.name()));
                        }
                        this.penalties.add(ptype);
                        this.done_partitions.remove(p);
                    }
                } // FOR
                new_touched_partitions.addAll(v.getPartitions());
                
                // For each partition that we don't touch here, we want to increase their idle counter
                for (Integer p : this.all_partitions) {
                    if (new_touched_partitions.contains(p) == false) {
                        this.idle_partition_ctrs.put(p, this.idle_partition_ctrs.get(p)+1);
                    } else {
                        this.idle_partition_ctrs.put(p, 0);
                    }
                } // FOR
            } // FOR
            last_est_idx = stop;
            touched_partitions.addAll(new_touched_partitions);
            
            // This is the key part: We will only add a partition to our set of "done" partitions
            // if we touched it in the past. Otherwise, we will always mark every partition as done
            // if there is a conditional clause that causes the partition to get touched. This is because
            // our initial estimation of what partitions we are done at will be based on the total
            // path estimation and not directly on the finished probabilities
            for (Integer finished_p : est.getFinishedPartitions(this.thresholds)) {
                if (touched_partitions.contains(finished_p)) {
                    // We are late with identifying that a partition is finished if it was
                    // idle for more than one batch round
                    if (this.idle_partition_ctrs.get(finished_p) > 0) {
                        if (t) {
                            if (first_penalty5) {
                                LOG.trace("PENALTY #5: " + PenaltyGroup.LATE_DONE);
                                first_penalty5 = false;
                            }
                            LOG.trace(String.format("Txn #%d kept partition %d idle for %d batch rounds before declaring it was done", s.getTransactionId(), finished_p, this.idle_partition_ctrs.get(finished_p)));
                        }
                        this.penalties.add(Penalty.LATE_DONE_PARTITION);
                        // Set it to basically negative infinity so that we are nevery penalized more than once for this partition
                        this.idle_partition_ctrs.put(finished_p, Integer.MIN_VALUE);
                    }
                    if (this.done_partitions.contains(finished_p) == false) {
                        if (t) LOG.trace(String.format("Marking touched partition %d as finished for the first time in MarkovEstimate #%d", finished_p.intValue(), i));
                        this.done_partitions.add(finished_p);
                    }
                }
            } // FOR
        } // FOR
        
        // ----------------------------------------------------------------------------
        // PENALTY #4
        // Check whether the transaction has declared that they would read/write at a partition
        // but then they never actually did so
        // The penalty is higher if it was predicted as multi-partitioned but it was actually single-partitioned
        // ----------------------------------------------------------------------------
        first_penalty = true;
        boolean could_be_singlepartitioned = (e_singlepartitioned == false && a_singlepartitioned == true);
        for (Integer p : this.e_read_partitions) {
            if (this.a_read_partitions.contains(p) == false) {
                if (t) {
                    if (first_penalty) {
                        LOG.trace("PENALTY #4: " + PenaltyGroup.UNUSED_PARTITION);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d predicted it would READ at partition %d but it never did", s.getTransactionId(), p));
                }
                this.penalties.add(could_be_singlepartitioned ? Penalty.UNUSED_READ_PARTITION_SINGLE :
                                                                Penalty.UNUSED_READ_PARTITION_MULTI);
            }
        } // FOR
        for (Integer p : this.e_write_partitions) {
            if (this.a_write_partitions.contains(p) == false) {
                if (t) {
                    if (first_penalty) {
                        LOG.trace("PENALTY #4: " + PenaltyGroup.UNUSED_PARTITION);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d predicted it would WRITE at partition %d but it never did", s.getTransactionId(), p));
                }
                this.penalties.add(could_be_singlepartitioned ? Penalty.UNUSED_WRITE_PARTITION_SINGLE :
                                                                Penalty.UNUSED_WRITE_PARTITION_MULTI);
            }
        } // FOR
        
        if (t) LOG.trace(String.format("Number of Penalties %d: %s", this.penalties.size(), this.penalties));
        double cost = 0.0d;
        for (Penalty p : this.penalties) cost += p.getCost();
        return (cost);
    }
    
    @Override
    public void clear(boolean force) {
        super.clear(force);
        this.penalties.clear();
    }
    
    @Override
    public void invalidateCache(String catalogKey) {
        // Nothing...
    }

    @Override
    public AbstractCostModel clone(Database catalogDb) throws CloneNotSupportedException {
        return null;
    }
    
    @Override
    public void prepareImpl(Database catalog_db) {
        // This is the start of a new run through the workload, so we need to re-init
        // our PartitionEstimator so that we are getting the proper catalog objects back
        this.p_estimator.initCatalog(catalog_db);
    }
    
    /**
     * @param args
     */
    public static void main(String vargs[]) throws Exception {
        final ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG, 
            ArgumentsParser.PARAM_MARKOV,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_CORRELATIONS
        );

//        // Make a fake neworder
//        Object params[] = {
//            new Long(1),        // (0) W_ID
//            new Long(2),        // (1) D_ID
//            new Long(3),        // (2) C_ID
//            new TimestampType(),// (3) TIMESTAMP
//            null,               // (4) ITEM_ID
//            null,               // (5) SUPPY_WAREHOUSE
//            null,               // (6) QUANTITY
//        };
//        int num_items = 20;
//        for (int i = 4; i <= 6; i++) {
//            Long arr[] = new Long[num_items];
//            for (int ii = 0; ii < arr.length; ii++) {
//                arr[ii] = new Long(1 + (ii == num_items-1 ? 1 : 0)); 
//            } // FOR
//            params[i] = arr;
//        } // FOR
//        for (int i = 0; i < params.length; i++) {
//            String val = (i >= 4 ? Arrays.toString((Long[])params[i]) : params[i].toString());
//            System.err.println(String.format("params[%d] = %s", i, val));
//        }
//        Procedure catalog_proc = args.catalog_db.getProcedures().get("neworder");
//        TransactionTrace tt = new TransactionTrace(Long.MAX_VALUE, catalog_proc, params);
//        tt.stop();
//        args.workload.addTransaction(catalog_proc, tt);
        
        // Only load the MarkovGraphs that we actually need
        Set<Procedure> procedures = args.workload.getProcedures(args.catalog_db);
        
        String input_path = args.getParam(ArgumentsParser.PARAM_MARKOV);
        Map<Integer, MarkovGraphsContainer> m = MarkovUtil.loadProcedures(args.catalog_db, input_path, procedures);
        assert(m != null);
        Boolean global = m.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
        
        MarkovGraphsContainer markovs = null;
        if (global != null && global == true) {
            markovs = m.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
            
        // HACK: Combine the partitioned-based graphs into a single Container
        } else {
            markovs = new MarkovGraphsContainer();
            for (MarkovGraphsContainer orig : m.values()) {
                markovs.copy(orig);
            } // FOR
        }
 
        final PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db);
        final EstimationThresholds thresholds = new EstimationThresholds();
        final TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, args.param_correlations, markovs);
        
        
        final Histogram total_h = new Histogram();
        final Histogram missed_h = new Histogram();
        final Histogram accurate_h = new Histogram();
        final Histogram penalty_h = new Histogram();
        
        LOG.info(String.format("Estimating the accuracy of the MarkovGraphs using %d transactions", args.workload.getTransactionCount()));
        int num_threads = ThreadUtil.getMaxGlobalThreads();
        final List<TransactionTrace> queue = new ArrayList<TransactionTrace>();
        for (TransactionTrace txn_trace : args.workload.getTransactions()) {
            queue.add(txn_trace);
            total_h.put(txn_trace.getCatalogItemName());
        } // FOR
        Collections.shuffle(queue);

        final AtomicInteger total = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < num_threads; i++) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    TransactionTrace txn_trace = null;
                    final MarkovCostModel costmodel = new MarkovCostModel(args.catalog_db, p_estimator, t_estimator, thresholds);
                    final Set<PenaltyGroup> penalty_groups = new HashSet<PenaltyGroup>();
                    
                    while (true) {
                        synchronized (queue) {
                            if (queue.isEmpty()) break;
                            txn_trace = queue.remove(0);
                        }
                        
                        double cost = 0.0d;
                        Throwable error = null;
                        try {
                            cost = costmodel.estimateTransactionCost(args.catalog_db, txn_trace);
                        } catch (AssertionError ex) {
                            error = ex;
                        } catch (Exception ex) {
                            error = ex;
                        }
                        if (error != null) {
                            LOG.warn("Failed to MarkovEstimate cost for " + txn_trace, error);
                            failures.getAndIncrement();
                            continue;
                        }
                        
                        String proc_name = txn_trace.getCatalogItemName();
                        if (cost > 0) {
                            penalty_groups.clear();
                            for (Penalty p : costmodel.getLastPenalties()) {
                                penalty_groups.add(p.getGroup());
                            } // FOR
                            for (PenaltyGroup pg : penalty_groups) {
                                penalty_h.put(pg);
                            } // FOR
                            missed_h.put(proc_name);
                        } else {
                            accurate_h.put(proc_name);
                        }
                        int cnt = total.incrementAndGet(); 
                        if (cnt % 100 == 0) LOG.info(String.format("Processed %d transactions [failures=%d]", cnt, failures.get()));
                    } // WHILE
                } 
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);
        
        int accurate_cnt = total.get() - (int)missed_h.getSampleCount();
        assert(accurate_cnt == accurate_h.getSampleCount());
        
        Map<String, Object> m0 = new ListOrderedMap<String, Object>();
        m0.put("RESULT", String.format("%05d / %05d [%.03f]", accurate_cnt, total.get(), (accurate_cnt / (double)total.get())));
        m0.put("FAILURES", String.format("%05d / %05d [%.03f]", failures.get(), total_h.getSampleCount(), (failures.get() / (double)total_h.getSampleCount())));
        
        Map<String, Object> m1 = new ListOrderedMap<String, Object>();
        for (PenaltyGroup pg : PenaltyGroup.values()) {
            long cnt = penalty_h.get(pg, 0);
            m1.put(pg.toString(), String.format("%05d [%.03f]", cnt, cnt / (double)total.get()));
        }

        System.err.println("TRANSACTION COUNTS:");
        System.err.println(total_h);
        System.err.println(StringUtil.DOUBLE_LINE);
        System.err.println("TRANSACTION ACCURACY:");
        System.err.println(accurate_h);
        System.err.println(StringUtil.DOUBLE_LINE);
        System.err.println(StringUtil.formatMaps(m0, m1));
    }
}