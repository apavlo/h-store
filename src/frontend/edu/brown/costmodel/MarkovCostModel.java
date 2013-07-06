package edu.brown.costmodel;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.hstore.txns.TransactionUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovProbabilityCalculator;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;

public class MarkovCostModel extends AbstractCostModel {
    private static final Logger LOG = Logger.getLogger(MarkovCostModel.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public enum MarkovOptimization {
        OP1_BASEPARTITION, OP2_PARTITIONS, OP3_ABORTS, OP4_FINISHED;

        private final Set<Penalty> penalties = new TreeSet<Penalty>();
    }

    /**
     * Cost Model Penalties
     */
    public enum Penalty {
        // ----------------------------------------------------------------------------
        // OP1_BASEPARTITION
        // ----------------------------------------------------------------------------
        /**
         * The most access partition is not the same as the PartitionEstimator's
         * base partition
         */
        WRONG_BASE_PARTITION(MarkovOptimization.OP1_BASEPARTITION, 0.5d),

        // ----------------------------------------------------------------------------
        // OP2_PARTITIONS
        // ----------------------------------------------------------------------------
        /**
         * The transaction did not declare it would read at a partition
         */
        MISSED_READ_PARTITION(MarkovOptimization.OP2_PARTITIONS, 0.5d),
        /**
         * The transaction did not declare it would write at a partition
         */
        MISSED_WRITE_PARTITION(MarkovOptimization.OP2_PARTITIONS, 0.5d),
        /**
         * The transaction said it was going to read at a partition but it never
         * did And it would have executed as single-partitioned if we didn't say
         * it was going to!
         */
        UNUSED_READ_PARTITION_SINGLE(MarkovOptimization.OP2_PARTITIONS, 0.5d),
        /**
         * The transaction said it was going to write at a partition but it
         * never did And it would have executed as single-partitioned if we
         * didn't say it was going to!
         */
        UNUSED_WRITE_PARTITION_SINGLE(MarkovOptimization.OP2_PARTITIONS, 0.5d),
        /**
         * The transaction said it was going to read at a partition but it never
         * did
         */
        UNUSED_READ_PARTITION_MULTI(MarkovOptimization.OP2_PARTITIONS, 0.1d),
        /**
         * The transaction said it was going to write at a partition but it
         * never did
         */
        UNUSED_WRITE_PARTITION_MULTI(MarkovOptimization.OP2_PARTITIONS, 0.1d),

        // ----------------------------------------------------------------------------
        // OP3_ABORTS
        // ----------------------------------------------------------------------------
        /**
         * The transaction is single-partitioned and it aborts when we predicted
         * that it wouldn't. Evan says that this is the worst!
         */
        MISSED_ABORT_SINGLE(MarkovOptimization.OP3_ABORTS, 1.0d),
        /**
         * The transaction is multi-partitioned and it aborts when we predicted
         * that it wouldn't.
         */
        MISSED_ABORT_MULTI(MarkovOptimization.OP3_ABORTS, 0.8d),
        /**
         * A transaction will never abort after a certain point in its
         * execution, but we failed to identify that we could have disabled undo
         * buffers
         */
        MISSED_NO_UNDO_BUFFER(MarkovOptimization.OP3_ABORTS, 0.25d),

        // ----------------------------------------------------------------------------
        // OP4_FINISHED
        // ----------------------------------------------------------------------------
        /**
         * The transaction goes back to read at a partition after it declared it
         * was done with it
         */
        RETURN_READ_PARTITION(MarkovOptimization.OP4_FINISHED, 0.25d),
        /**
         * The transaction goes back to write at a partition after it declared
         * it was done with it
         */
        RETURN_WRITE_PARTITION(MarkovOptimization.OP4_FINISHED, 0.25d),
        /**
         * The transaction is done with a partition but we don't identify it
         * until later in the execution path
         */
        LATE_DONE_PARTITION(MarkovOptimization.OP4_FINISHED, 0.05d), ;

        private final double cost;
        private final MarkovOptimization group;

        private Penalty(MarkovOptimization group, double cost) {
            this.group = group;
            this.group.penalties.add(this);
            this.cost = cost;
        }

        public double getCost() {
            return this.cost;
        }

        public MarkovOptimization getGroup() {
            return this.group;
        }
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    private final CatalogContext catalogContext;
    private final EstimationThresholds thresholds;
    private final MarkovEstimator t_estimator;
    private boolean force_full_comparison = false;
    private boolean force_regenerate_markovestimates = false;

    private ObjectHistogram<Procedure> fast_path_counter = new ObjectHistogram<Procedure>();
    private ObjectHistogram<Procedure> full_path_counter = new ObjectHistogram<Procedure>();

    // ----------------------------------------------------------------------------
    // INVOCATION DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * The list of penalties accrued for this transaction
     */
    private transient final List<Penalty> penalties = new ArrayList<Penalty>();

    private transient final PartitionSet done_partitions = new PartitionSet();
    private transient final ObjectHistogram<Integer> idle_partition_ctrs = new ObjectHistogram<Integer>();

    private transient final PartitionSet e_all_partitions = new PartitionSet();
    private transient final PartitionSet e_read_partitions = new PartitionSet();
    private transient final PartitionSet e_write_partitions = new PartitionSet();

    private transient final PartitionSet a_all_partitions = new PartitionSet();
    private transient final PartitionSet a_read_partitions = new PartitionSet();
    private transient final PartitionSet a_write_partitions = new PartitionSet();

    /**
     * Constructor
     * 
     * @param catalogContext
     * @param p_estimator
     */
    public MarkovCostModel(CatalogContext catalogContext, PartitionEstimator p_estimator, MarkovEstimator t_estimator, EstimationThresholds thresholds) {
        super(MarkovCostModel.class, catalogContext, p_estimator);
        this.catalogContext = catalogContext;
        this.thresholds = thresholds;
        this.t_estimator = t_estimator;

        assert (this.t_estimator != null) : "Missing TransactionEstimator";
    }

    /**
     * Get the penalties for the last TransactionTrace processed Not thread-safe
     * 
     * @return
     */
    protected List<Penalty> getLastPenalties() {
        return this.penalties;
    }

    protected PartitionSet getLastEstimatedAllPartitions() {
        return (this.e_all_partitions);
    }

    protected PartitionSet getLastEstimatedReadPartitions() {
        return (this.e_read_partitions);
    }

    protected PartitionSet getLastEstimatedWritePartitions() {
        return (this.e_write_partitions);
    }

    protected PartitionSet getLastActualAllPartitions() {
        return (this.a_all_partitions);
    }

    protected PartitionSet getLastActualReadPartitions() {
        return (this.a_read_partitions);
    }

    protected PartitionSet getLastActualWritePartitions() {
        return (this.a_write_partitions);
    }

    protected MarkovCostModel forceFullPathComparison() {
        this.force_full_comparison = true;
        return (this);
    }

    protected MarkovCostModel forceRegenerateMarkovEstimates() {
        this.force_regenerate_markovestimates = true;
        return (this);
    }

    @Override
    public synchronized double estimateTransactionCost(CatalogContext catalogContext, Workload workload, Filter filter, TransactionTrace txn_trace) throws Exception {
        // Throw the txn at the estimator and let it come up with the initial
        // path estimation.
        // Now execute the queries and see what path the txn actually takes
        // I don't think it matters whether we do this in batches, but it
        // probably doesn't hurt
        // to do it right in case we need it later
        // At this point we know what the transaction actually would do using
        // the TransactionEstimator's
        // internal Markov models.
        MarkovEstimatorState s = this.t_estimator.processTransactionTrace(txn_trace);
        assert (s != null);
        Procedure catalog_proc = txn_trace.getCatalogItem(catalogContext.database);

        if (debug.val) {
            LOG.debug("Measuring MarkovEstimate Accuracy: " + txn_trace);
            if (trace.val) {
                LOG.trace("Estimated: " + ((MarkovEstimate)s.getInitialEstimate()).getMarkovPath());
                LOG.trace("Actual:    " + s.getActualPath());
            }
        }

        double cost = 0.0d;

        this.e_read_partitions.clear();
        this.e_write_partitions.clear();
        MarkovEstimate initialEst = s.getInitialEstimate();
        assert(initialEst != null);
        assert(initialEst.isInitialized());
        for (Integer partition : initialEst.getTouchedPartitions(this.thresholds)) {
            if (initialEst.isDonePartition(this.thresholds, partition.intValue()) == false) {
                for (MarkovVertex v : initialEst.getMarkovPath()) {
                    if (v.getPartitions().contains(partition) == false)
                        continue;
                    if (((Statement) v.getCatalogItem()).getReadonly()) {
                        this.e_read_partitions.add(partition);
                    } else {
                        this.e_write_partitions.add(partition);
                    }
                } // FOR
            }
        } // FOR

        List<MarkovVertex> initialPath = initialEst.getMarkovPath();
        List<MarkovVertex> actualPath = s.getActualPath();
        this.a_read_partitions.clear();
        this.a_write_partitions.clear();
        MarkovUtil.getReadWritePartitions(actualPath, this.a_read_partitions, this.a_write_partitions);

        // Try fast version
        try {
            if (this.force_full_comparison || !this.comparePathsFast(CollectionUtil.last(initialPath), actualPath)) {
                // Otherwise we have to do the full path comparison to figure
                // out just how wrong we are
                cost = this.comparePathsFull(s);
                this.full_path_counter.put(catalog_proc);
            } else {
                this.fast_path_counter.put(catalog_proc);
            }
        } catch (Throwable ex) {
            System.err.println(txn_trace.debug(catalogContext.database));
            System.err.println("COST = " + cost);
            System.err.println("BASE PARTITION = " + s.getBasePartition());
            System.err.println("PENALTIES = " + this.penalties);
            System.err.println("ESTIMATED PARTITIONS: " + this.e_all_partitions);
            System.err.println("ACTUAL PARTITIONS: " + this.a_all_partitions);
            System.err.println("MARKOV GRAPH: " + MarkovUtil.exportGraphviz(s.getMarkovGraph(), true, null).writeToTempFile(catalog_proc));
            System.err.println();

            String e_path = "ESTIMATED PATH:\n" + StringUtil.join("\n", initialEst.getMarkovPath());
            String a_path = "ACTUAL PATH:\n" + StringUtil.join("\n", s.getActualPath());
            System.err.println(StringUtil.columns(e_path, a_path));
            System.err.println("MARKOV ESTIMATE:\n" + s.getInitialEstimate());

            throw new RuntimeException(ex);
        }

        this.t_estimator.destroyEstimatorState(s);
        return (cost);
    }

    /**
     * Quickly compare the two paths and return true if they are similar enough
     * We don't care if they execute different queries just as long as the
     * read/write partitions match and that the estimated path either
     * commits/aborts as same as actual path
     * 
     * @param estimated
     * @param actual
     * @return
     */
    protected boolean comparePathsFast(List<MarkovVertex> estimated, List<MarkovVertex> actual) {
        if (trace.val)
            LOG.trace(String.format("Fast Path Compare: Estimated [size=%d] vs. Actual [size=%d]", estimated.size(), actual.size()));
        this.e_read_partitions.clear();
        this.e_write_partitions.clear();
        MarkovUtil.getReadWritePartitions(estimated, this.e_read_partitions, this.e_write_partitions);
        this.a_read_partitions.clear();
        this.a_write_partitions.clear();
        MarkovUtil.getReadWritePartitions(actual, this.a_read_partitions, this.a_write_partitions);
        return (this.comparePathsFast(CollectionUtil.last(estimated), actual));
    }

    /**
     * @param e_last
     * @param actual
     * @return
     */
    private boolean comparePathsFast(MarkovVertex e_last, List<MarkovVertex> actual) {
        // (1) Check that the MarkovEstimate's last state matches the actual
        // path (commit vs abort)
        assert (e_last != null);
        MarkovVertex a_last = CollectionUtil.last(actual);
        assert (a_last != null);
        assert (a_last.isEndingVertex());
        if (trace.val) {
            LOG.trace("Estimated Last Vertex: " + e_last);
            LOG.trace("Actual Last Vertex: " + a_last);
        }
        if (e_last.getType() != a_last.getType()) {
            return (false);
        }

        // (2) Check that the partitions that we predicted that the txn would
        // read/write are the same
        if (trace.val) {
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
     * Calculate relative cost difference the estimated and actual execution
     * paths
     * 
     * @param estimated
     * @param actual
     * @return
     */
    protected double comparePathsFull(MarkovEstimatorState s) {
        if (debug.val)
            LOG.debug("Performing full comparison of Transaction #" + s.getTransactionId());

        this.penalties.clear();

        MarkovEstimate initialEst = s.getInitialEstimate();
        List<MarkovVertex> estimated = initialEst.getMarkovPath();
        this.e_all_partitions.clear();
        this.e_all_partitions.addAll(this.e_read_partitions);
        this.e_all_partitions.addAll(this.e_write_partitions);
        MarkovVertex e_last = CollectionUtil.last(estimated);
        assert (e_last != null);

        List<MarkovVertex> actual = s.getActualPath();
        this.a_all_partitions.clear();
        this.a_all_partitions.addAll(this.a_read_partitions);
        this.a_all_partitions.addAll(this.a_write_partitions);
        MarkovVertex a_last = CollectionUtil.last(actual);
        assert (a_last != null);
        assert (a_last.isEndingVertex());

        MarkovEstimate initial_est = s.getInitialEstimate();
        assert (initial_est != null);
        MarkovEstimate last_est = s.getLastEstimate();
        assert (last_est != null);
        MarkovGraph markov = s.getMarkovGraph();
        assert (markov != null);

        final int base_partition = s.getBasePartition();
        final int num_estimates = s.getEstimateCount();
        List<Estimate> estimates = null;

        // This is strictly for the paper so that we can show how slow it would
        // be to have calculate probabilities through a traversal for each batch
        if (this.force_regenerate_markovestimates) {
            if (debug.val) {
                String name = TransactionUtil.formatTxnName(markov.getProcedure(), s.getTransactionId());
                LOG.debug("Using " + MarkovProbabilityCalculator.class.getSimpleName() + " to calculate MarkoEstimates for " + name);
            }
            estimates = new ArrayList<Estimate>();
            for (Estimate e : s.getEstimates()) {
                MarkovEstimate est = (MarkovEstimate)e; 
                MarkovVertex v = est.getVertex();
                MarkovEstimate new_est = MarkovProbabilityCalculator.generate(this.catalogContext, markov, v);
                assert (new_est != null);
                estimates.add(est);
            } // FOR
        } else {
            estimates = s.getEstimates();
        }

        boolean e_singlepartitioned = initial_est.isSinglePartitioned(this.thresholds);
        boolean a_singlepartitioned = (this.a_all_partitions.size() == 1);

        boolean first_penalty = true;

        if (trace.val) {
            LOG.trace("Estimated Read Partitions:  " + this.e_read_partitions);
            LOG.trace("Estimated Write Partitions: " + this.e_write_partitions);
            LOG.trace("Actual Read Partitions:     " + this.a_read_partitions);
            LOG.trace("Actual Write Partitions:    " + this.a_write_partitions);
        }

        // ----------------------------------------------------------------------------
        // BASE PARTITION
        // ----------------------------------------------------------------------------
        PartitionSet most_touched = initial_est.getMostTouchedPartitions(this.thresholds);
        Integer e_base_partition = null;
        if (most_touched.size() > 1) {
            e_base_partition = CollectionUtil.random(most_touched);
        } else {
            e_base_partition = CollectionUtil.first(most_touched);
        }
        if (e_base_partition == null || e_base_partition != base_partition) {
            if (trace.val) {
                LOG.trace(String.format("Estimated base partition for txn #%d was %d but PartitionEstimator says it should be %d", s.getTransactionId(), e_base_partition, base_partition));
            }
            this.penalties.add(Penalty.WRONG_BASE_PARTITION);
            // assert(false) : e_base_partition + " != " + base_partition + "  "
            // + most_touched;
        }

        // ----------------------------------------------------------------------------
        // ABORTS
        // If the transaction was predicted to be single-partitioned and we
        // don't predict that it's going to
        // abort when it actually did, then that's bad! Really bad!
        // ----------------------------------------------------------------------------
        first_penalty = true;
        if (initial_est.isAbortable(this.thresholds) == false && a_last.isAbortVertex()) {
            if (trace.val) {
                if (first_penalty) {
                    LOG.trace("PENALTY: " + MarkovOptimization.OP3_ABORTS);
                    first_penalty = false;
                }
                LOG.trace(String.format("Txn #%d aborts but we predicted that it would never!", s.getTransactionId()));
            }
            this.penalties.add(a_singlepartitioned ? Penalty.MISSED_ABORT_SINGLE : Penalty.MISSED_ABORT_MULTI);
        }

        // For each MarkovEstimate, check whether there is a path in the graph
        // for the current vertex
        // to the abort state. If there isn't, then we need to check whether
        // This should match ExecutionSite.executeLocalPlan()
        MarkovVertex abort_v = markov.getAbortVertex();
        boolean last_hadAbortPath = true;
        first_penalty = true;
        for (Estimate e : estimates) {
            MarkovEstimate est = (MarkovEstimate)e;
            assert(est.isInitialized()) : "Uninitialized MarkovEstimate from " + s;
            MarkovVertex v = est.getVertex();
            assert (v != null) : "No vertex?\n" + est;
            boolean isAbortable = est.isAbortable(this.thresholds);
            boolean isReadOnly = est.isReadOnlyPartition(this.thresholds, base_partition);
            boolean hasAbortPath;
            synchronized (markov) {
                hasAbortPath = (markov.getPath(v, abort_v).isEmpty() == false);
            } // SYNCH

            // Make sure that we didn't have a path for the last MarkovEstimate
            // but
            // we somehow have one now
            if (hasAbortPath && last_hadAbortPath == false) {
                LOG.info("MARKOV: " + MarkovUtil.exportGraphviz(markov, false, markov.getPath(v, abort_v)).writeToTempFile());
                assert (last_hadAbortPath);
            }

            // If the path is not empty, then this txn could still abort
            if (hasAbortPath)
                continue;

            // Otherwise check whether our estimate still says to go with undo
            // buffers when
            // we're going to be read-only for the rest of the transaction
            // This is would be considered wasted work
            if (isAbortable && isReadOnly) {
                if (trace.val) {
                    if (first_penalty) {
                        LOG.trace("PENALTY: " + MarkovOptimization.OP3_ABORTS);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d will never abort but we failed to disable undo buffers!", s.getTransactionId()));
                }
                this.penalties.add(Penalty.MISSED_NO_UNDO_BUFFER);
            }
            last_hadAbortPath = false;
        } // FOR

        // ----------------------------------------------------------------------------
        // MISSED PARTITIONS
        // The transaction actually reads/writes at more partitions than it originally predicted
        // This is expensive because it means that we have to abort+restart the txn
        // ----------------------------------------------------------------------------
        first_penalty = true;
        for (Integer p : this.a_read_partitions) {
            if (this.e_read_partitions.contains(p) == false) {
                if (trace.val) {
                    if (first_penalty) {
                        LOG.trace("PENALTY: " + MarkovOptimization.OP2_PARTITIONS);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d failed to predict that it was READING at partition %d", s.getTransactionId(), p));
                }
                this.penalties.add(Penalty.MISSED_READ_PARTITION);
            }
        } // FOR
        for (Integer p : this.a_write_partitions) {
            if (this.e_write_partitions.contains(p) == false) {
                if (trace.val) {
                    if (first_penalty) {
                        LOG.trace("PENALTY: " + MarkovOptimization.OP2_PARTITIONS);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d failed to predict that it was WRITING at partition %d", s.getTransactionId(), p));
                }
                this.penalties.add(Penalty.MISSED_WRITE_PARTITION);
            }
        } // FOR
        // if (this.penalties.size() > 0) {
        // LOG.info("Estimated Read Partitions:  " + this.e_read_partitions);
        // LOG.info("Estimated Write Partitions: " + this.e_write_partitions);
        // LOG.info("Actual Read Partitions:     " + this.a_read_partitions);
        // LOG.info("Actual Write Partitions:    " + this.a_write_partitions);
        //
        // LOG.info("IS ABORTABLE:    " +
        // initial_est.isAbortable(this.thresholds));
        // LOG.info("ABORT THRESHOLD: " + this.thresholds.getAbortThreshold());
        // LOG.info("Current State\n" + actual.get(1).debug());
        // LOG.info("MarkovEstimate\n" + initial_est.toString());
        // // LOG.info("GRAPH: " + MarkovUtil.exportGraphviz(s.getMarkovGraph(),
        // false, true, false, null).writeToTempFile());
        // System.exit(1);
        // }

        // ----------------------------------------------------------------------------
        // RETURN TO PARTITIONS
        // We declared that we were done at a partition but then later we
        // actually needed it. This can happen if there is a path that a has
        // very low probability of us taking it, but then ended up taking it anyway
        //
        // LATE FINISHED PARTITIONS
        // We keep track of the last batch round that we finished with a partition.
        // We then count how long it takes before we realize that we are finished.
        // We declare that the MarkovEstimate was late if we don't mark it as finished 
        // immediately in the next batch
        // ----------------------------------------------------------------------------
        first_penalty = true;
        boolean first_penalty5 = true;

        this.done_partitions.clear();
        int last_est_idx = 0;
        PartitionSet touched_partitions = new PartitionSet();
        PartitionSet new_touched_partitions = new PartitionSet();

        // Reset the idle counters
        this.idle_partition_ctrs.clear();

        for (int i = 0; i < num_estimates; i++) {
            MarkovEstimate est = (MarkovEstimate)estimates.get(i);
            MarkovVertex est_v = est.getVertex();

            // Get the path of vertices
            int start = last_est_idx;
            int stop = actual.indexOf(est_v);
            
            // So this is just a hack so that our test cases still work
            if (stop == -1) {
                LOG.warn("Failed to find MarkovVertex " + est_v + " in path!");
                continue;
            }
            assert(stop != -1);

            new_touched_partitions.clear();
            for (; start <= stop; start++) {
                MarkovVertex v = actual.get(start);
                assert (v != null);

                Statement catalog_stmt = v.getCatalogItem();
                QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
                Penalty ptype = (qtype == QueryType.SELECT ? Penalty.RETURN_READ_PARTITION : Penalty.RETURN_WRITE_PARTITION);
                for (Integer p : v.getPartitions()) {
                    // Check if we read/write at any partition that was
                    // previously declared as done
                    if (this.done_partitions.contains(p)) {
                        if (trace.val) {
                            if (first_penalty) {
                                LOG.trace("PENALTY: " + MarkovOptimization.OP4_FINISHED);
                                first_penalty = false;
                            }
                            LOG.trace(String.format("Txn #%d said that it was done at partition %d but it executed a %s", s.getTransactionId(), p, qtype.name()));
                        }
                        this.penalties.add(ptype);
                        this.done_partitions.remove(p);
                    }
                } // FOR
                new_touched_partitions.addAll(v.getPartitions());

                // For each partition that we don't touch here, we want to
                // increase their idle counter
                this.idle_partition_ctrs.put(this.catalogContext.getAllPartitionIds());
            } // FOR
            last_est_idx = stop;
            touched_partitions.addAll(new_touched_partitions);

            // This is the key part: We will only add a partition to our set of
            // "done" partitions if we touched it in the past. Otherwise, we will 
            // always mark every partition as done if there is a conditional clause
            // that causes the partition to get touched. This is because our initial 
            // estimation of what partitions we are done at will be based on the total
            // path estimation and not directly on the finished probabilities
            for (Integer finished_p : est.getDonePartitions(this.thresholds)) {
                if (touched_partitions.contains(finished_p)) {
                    // We are late with identifying that a partition is finished
                    // if it was idle for more than one batch round
                    if (this.idle_partition_ctrs.get(finished_p, 0) > 0) {
                        if (trace.val) {
                            if (first_penalty5) {
                                LOG.trace("PENALTY: " + MarkovOptimization.OP4_FINISHED);
                                first_penalty5 = false;
                            }
                            LOG.trace(String.format("Txn #%d kept partition %d idle for %d batch rounds before declaring it was done", s.getTransactionId(), finished_p,
                                    this.idle_partition_ctrs.get(finished_p)));
                        }
                        this.penalties.add(Penalty.LATE_DONE_PARTITION);
                        // Set it to basically negative infinity so that we are
                        // never penalized more than once for this partition
                        // FIXME this.idle_partition_ctrs.put(finished_p, Integer.MIN_VALUE);
                    }
                    if (this.done_partitions.contains(finished_p) == false) {
                        if (trace.val)
                            LOG.trace(String.format("Marking touched partition %d as finished for the first time in MarkovEstimate #%d", finished_p.intValue(), i));
                        this.done_partitions.add(finished_p);
                    }
                }
            } // FOR
        } // FOR

        // ----------------------------------------------------------------------------
        // UNUSED PARTITIONS
        // Check whether the transaction has declared that they would read/write
        // at a partition
        // but then they never actually did so
        // The penalty is higher if it was predicted as multi-partitioned but it
        // was actually single-partitioned
        // ----------------------------------------------------------------------------
        first_penalty = true;
        boolean could_be_singlepartitioned = (e_singlepartitioned == false && a_singlepartitioned == true);
        for (Integer p : this.e_read_partitions) {
            if (this.a_read_partitions.contains(p) == false) {
                if (trace.val) {
                    if (first_penalty) {
                        LOG.trace("PENALTY: " + MarkovOptimization.OP2_PARTITIONS);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d predicted it would READ at partition %d but it never did", s.getTransactionId(), p));
                }
                this.penalties.add(could_be_singlepartitioned ? Penalty.UNUSED_READ_PARTITION_SINGLE : Penalty.UNUSED_READ_PARTITION_MULTI);
            }
        } // FOR
        for (Integer p : this.e_write_partitions) {
            if (this.a_write_partitions.contains(p) == false) {
                if (trace.val) {
                    if (first_penalty) {
                        LOG.trace("PENALTY: " + MarkovOptimization.OP2_PARTITIONS);
                        first_penalty = false;
                    }
                    LOG.trace(String.format("Txn #%d predicted it would WRITE at partition %d but it never did", s.getTransactionId(), p));
                }
                this.penalties.add(could_be_singlepartitioned ? Penalty.UNUSED_WRITE_PARTITION_SINGLE : Penalty.UNUSED_WRITE_PARTITION_MULTI);
            }
        } // FOR

        if (trace.val)
            LOG.info(String.format("Number of Penalties %d: %s", this.penalties.size(), this.penalties));
        double cost = 0.0d;
        for (Penalty p : this.penalties)
            cost += p.getCost();

        // if (this.penalties.isEmpty() == false) throw new
        // RuntimeException("Missed optimizations for " + s.getFormattedName());
        return (cost);
    }

    @Override
    public void clear(boolean force) {
        super.clear(force);
        this.fast_path_counter.clear();
        this.full_path_counter.clear();
        this.penalties.clear();
    }

    @Override
    public void invalidateCache(String catalogKey) {
        // Nothing...
    }

    @Override
    public void prepareImpl(CatalogContext catalogContext) {
        // This is the start of a new run through the workload, so we need to re-init 
        // our PartitionEstimator so that we are getting the proper catalog objects back
        this.p_estimator.initCatalog(catalogContext);
    }

    /**
     * @param args
     */
    @SuppressWarnings("unchecked")
    public static void main(String vargs[]) throws Exception {
        final ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG,
                     ArgumentsParser.PARAM_MARKOV,
                     ArgumentsParser.PARAM_WORKLOAD,
                     ArgumentsParser.PARAM_MAPPINGS,
                     ArgumentsParser.PARAM_MARKOV_THRESHOLDS);
        HStoreConf.initArgumentsParser(args);
        final int num_partitions = args.catalogContext.numberOfPartitions;
        final int base_partition = (args.workload_base_partitions.size() == 1 ? CollectionUtil.first(args.workload_base_partitions) : HStoreConstants.NULL_PARTITION_ID);
        final int num_threads = ThreadUtil.getMaxGlobalThreads();
        final boolean stop_on_error = true;
        final boolean force_fullpath = true;
        final boolean force_regenerate = true;
        final boolean skip_processing = false;

        final ObjectHistogram<Procedure> total_h = new ObjectHistogram<Procedure>();
        final ObjectHistogram<Procedure> missed_h = new ObjectHistogram<Procedure>();
        final ObjectHistogram<Procedure> accurate_h = new ObjectHistogram<Procedure>();
        final ObjectHistogram<MarkovOptimization> optimizations_h = new ObjectHistogram<MarkovOptimization>();
        final ObjectHistogram<Penalty> penalties_h = new ObjectHistogram<Penalty>();
        final Map<Procedure, ObjectHistogram<MarkovOptimization>> proc_penalties_h = new ConcurrentHashMap<Procedure, ObjectHistogram<MarkovOptimization>>();

        final AtomicInteger total = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        final List<Thread> processing_threads = new ArrayList<Thread>();
        final AtomicInteger thread_finished = new AtomicInteger(0);

        // Only load the MarkovGraphs that we actually need
        final int num_transactions = args.workload.getTransactionCount();
        assert (num_transactions > 0) : "No TransactionTraces";
        final int marker = Math.max(1, (int) (num_transactions * 0.10));
        final Set<Procedure> procedures = args.workload.getProcedures(args.catalog_db);
        PartitionSet partitions = null;
        if (base_partition != HStoreConstants.NULL_PARTITION_ID) {
            partitions = new PartitionSet(base_partition);
        } else {
            partitions = args.catalogContext.getAllPartitionIds();
        }

        final File input_path = args.getFileParam(ArgumentsParser.PARAM_MARKOV);
        final Map<Integer, MarkovGraphsContainer> m = MarkovGraphsContainerUtil.load(args.catalogContext,
                                                                                     input_path, procedures, partitions);
        assert (m != null);
        final boolean global = m.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
        final Map<Integer, MarkovGraphsContainer> thread_markovs[] = (Map<Integer, MarkovGraphsContainer>[]) new Map<?, ?>[num_threads];

        // If this is a GLOBAL model, load up a copy for each thread so that
        // there is no thread contention
        if (global && num_threads > 2) {
            LOG.info("Loading multiple copies of GLOBAL MarkovGraphsContainer");
            for (int i = 0; i < num_threads; i++) {
                final int thread_id = i;
                runnables.add(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            thread_markovs[thread_id] = MarkovGraphsContainerUtil.load(args.catalogContext,
                                                                                       input_path, procedures, null);
                        } catch (Throwable ex) {
                            throw new RuntimeException(ex);
                        }
                        assert (thread_markovs[thread_id].containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID));
                        LOG.info(String.format("Loading Thread Finished %d / %d", thread_finished.incrementAndGet(), num_threads));
                    }
                });
            } // FOR
            ThreadUtil.runNewPool(runnables, procedures.size());
            thread_finished.set(0);
            runnables.clear();
        } else {
            for (int i = 0; i < num_threads; i++) {
                thread_markovs[i] = m;
            } // FOR
        }

        final PartitionEstimator p_estimator = new PartitionEstimator(args.catalogContext);
        final MarkovCostModel thread_costmodels[][] = new MarkovCostModel[num_threads][num_partitions];
        final ProfileMeasurement profilers[] = new ProfileMeasurement[num_threads];
        final LinkedBlockingDeque<Pair<Integer, TransactionTrace>> queues[] = (LinkedBlockingDeque<Pair<Integer, TransactionTrace>>[]) new LinkedBlockingDeque<?>[num_threads];
        for (int i = 0; i < num_threads; i++) {
            profilers[i] = new ProfileMeasurement("ESTIMATION");
            queues[i] = new LinkedBlockingDeque<Pair<Integer, TransactionTrace>>();
        } // FOR
        LOG.info(String.format("Estimating the accuracy of the MarkovGraphs using %d transactions [threads=%d]", args.workload.getTransactionCount(), num_threads));
        LOG.info("THRESHOLDS: " + args.thresholds);

        // QUEUING THREAD
        final AtomicBoolean queued_all = new AtomicBoolean(false);
        runnables.add(new Runnable() {
            @Override
            public void run() {
                List<TransactionTrace> all_txns = new ArrayList<TransactionTrace>(args.workload.getTransactions());
                Collections.shuffle(all_txns);
                int ctr = 0;
                for (TransactionTrace txn_trace : all_txns) {
                    // Make sure it goes to the right base partition
                    int partition = HStoreConstants.NULL_PARTITION_ID;
                    try {
                        partition = p_estimator.getBasePartition(txn_trace);
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    assert(partition != HStoreConstants.NULL_PARTITION_ID) : "Failed to get base partition for " + txn_trace + "\n" + txn_trace.debug(args.catalog_db);
                    if (base_partition != HStoreConstants.NULL_PARTITION_ID && base_partition != partition)
                        continue;

                    int queue_idx = (global ? ctr : partition) % num_threads;
                    queues[queue_idx].add(Pair.of(partition, txn_trace));
                    if (++ctr % marker == 0)
                        LOG.info(String.format("Queued %d/%d transactions", ctr, num_transactions));
                } // FOR
                queued_all.set(true);

                // Poke all our threads just in case they finished
                for (Thread t : processing_threads)
                    t.interrupt();
            }
        });

        // PROCESSING THREADS
        for (int i = 0; i < num_threads; i++) {
            final int thread_id = i;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    Thread self = Thread.currentThread();
                    processing_threads.add(self);

                    Pair<Integer, TransactionTrace> pair = null;
                    final Set<MarkovOptimization> penalty_groups = new HashSet<MarkovOptimization>();
                    final Set<Penalty> penalties = new HashSet<Penalty>();
                    ObjectHistogram<MarkovOptimization> proc_h = null;

                    ProfileMeasurement profiler = profilers[thread_id];
                    assert (profiler != null);

                    MarkovCostModel costmodels[] = thread_costmodels[thread_id];
                    for (int p = 0; p < num_partitions; p++) {
                        MarkovGraphsContainer markovs = (global ? thread_markovs[thread_id].get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID) : thread_markovs[thread_id].get(p));
                        MarkovEstimator t_estimator = new MarkovEstimator(args.catalogContext, p_estimator, markovs);
                        costmodels[p] = new MarkovCostModel(args.catalogContext, p_estimator, t_estimator, args.thresholds);
                        if (force_fullpath)
                            thread_costmodels[thread_id][p].forceFullPathComparison();
                        if (force_regenerate)
                            thread_costmodels[thread_id][p].forceRegenerateMarkovEstimates();
                    } // FOR

                    int thread_ctr = 0;
                    while (true) {
                        try {
                            if (queued_all.get()) {
                                pair = queues[thread_id].poll();
                            } else {
                                pair = queues[thread_id].take();

                                // Steal work
                                if (pair == null) {
                                    for (int i = 0; i < num_threads; i++) {
                                        if (i == thread_id)
                                            continue;
                                        pair = queues[i].take();
                                        if (pair != null)
                                            break;
                                    } // FOR
                                }
                            }
                        } catch (InterruptedException ex) {
                            continue;
                        }
                        if (pair == null)
                            break;

                        int partition = pair.getFirst();
                        TransactionTrace txn_trace = pair.getSecond();
                        Procedure catalog_proc = txn_trace.getCatalogItem(args.catalog_db);
                        total_h.put(catalog_proc);
                        if (debug.val)
                            LOG.debug(String.format("Processing %s [%d / %d]", txn_trace, thread_ctr, thread_ctr + queues[thread_id].size()));

                        proc_h = proc_penalties_h.get(catalog_proc);
                        if (proc_h == null) {
                            synchronized (proc_penalties_h) {
                                proc_h = proc_penalties_h.get(catalog_proc);
                                if (proc_h == null) {
                                    proc_h = new ObjectHistogram<MarkovOptimization>();
                                    proc_penalties_h.put(catalog_proc, proc_h);
                                }
                            } // SYNCH
                        }

                        double cost = 0.0d;
                        Throwable error = null;
                        try {
                            profiler.start();
                            if (skip_processing == false) {
                                cost = costmodels[partition].estimateTransactionCost(args.catalogContext, txn_trace);
                            }
                        } catch (Throwable ex) {
                            error = ex;
                        } finally {
                            profiler.stop();
                        }
                        if (error != null) {
                            failures.getAndIncrement();
                            String msg = "Failed to estimate transaction cost for " + txn_trace;
                            if (stop_on_error)
                                throw new RuntimeException(msg, error);
                            LOG.warn(msg, error);
                            continue;
                        }

                        if (cost > 0) {
                            penalty_groups.clear();
                            penalties.clear();
                            for (Penalty p : costmodels[partition].getLastPenalties()) {
                                penalty_groups.add(p.getGroup());
                                penalties.add(p);
                            } // FOR
                            proc_h.put(penalty_groups);
                            optimizations_h.put(penalty_groups);
                            penalties_h.put(penalties);
                            missed_h.put(catalog_proc);
                        } else {
                            accurate_h.put(catalog_proc);
                        }
                        int global_ctr = total.incrementAndGet();
                        if (global_ctr % marker == 0)
                            LOG.info(String.format("Processed %d/%d transactions %s", global_ctr, num_transactions, (failures.get() > 0 ? String.format("[failures=%d]", failures.get()) : "")));
                        thread_ctr++;
                    } // WHILE
                    LOG.info(String.format("Processing Thread Finished %d / %d", thread_finished.incrementAndGet(), num_threads));
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);

        Map<Object, String> debugLabels = CatalogUtil.getHistogramLabels(args.catalog_db.getProcedures());

        ObjectHistogram<Procedure> fastpath_h = new ObjectHistogram<Procedure>();
        fastpath_h.setDebugLabels(debugLabels);
        ObjectHistogram<Procedure> fullpath_h = new ObjectHistogram<Procedure>();
        fullpath_h.setDebugLabels(debugLabels);
        ProfileMeasurement total_time = new ProfileMeasurement("ESTIMATION");

        for (int i = 0; i < num_threads; i++) {
            for (int p = 0; p < num_partitions; p++) {
                MarkovCostModel mc = thread_costmodels[i][p];
                fastpath_h.put(mc.fast_path_counter);
                fullpath_h.put(mc.full_path_counter);
                total_time.appendTime(profilers[i]);
            }
        } // FOR

        int accurate_cnt = total.get() - (int) missed_h.getSampleCount();
        assert (accurate_cnt == accurate_h.getSampleCount());

        // ---------------------------------------------------------------------------------------------

        Map<String, Object> m0 = new ListOrderedMap<String, Object>();
        m0.put("PARTITIONS", num_partitions);
        m0.put("FORCE FULLPATH", force_fullpath);
        m0.put("FORCE REGENERATE", force_regenerate);
        m0.put("COMPUTATION TIME", String.format("%.2f ms total / %.2f ms avg", total_time.getTotalThinkTimeMS(), total_time.getAverageThinkTimeMS()));
        m0.put("TRANSACTION COUNTS", total_h.setDebugLabels(debugLabels));

        Map<String, Object> m1 = new ListOrderedMap<String, Object>();
        m1.put("ACCURATE TRANSACTIONS", accurate_h.setDebugLabels(debugLabels));
        m1.put("MISSED TRANSACTIONS", missed_h.setDebugLabels(debugLabels));

        Map<String, Object> m2 = new ListOrderedMap<String, Object>();
        m2.put("FAST PATH", fastpath_h);
        m2.put("FULL PATH", fullpath_h);

        Map<String, Object> m3 = new ListOrderedMap<String, Object>();
        m3.put("ESTIMATE ACCURACY", String.format("%5d / %05d [%.03f]", accurate_cnt, total.get(), (accurate_cnt / (double) total.get())));
        if (failures.get() > 0) {
            m3.put("ERRORS", String.format("%5d / %05d [%.03f]", failures.get(), total_h.getSampleCount(), (failures.get() / (double) total_h.getSampleCount())));
        }

        ListOrderedMap<String, String> m4 = new ListOrderedMap<String, String>();
        final String f = "%5d [%.03f]";
        for (MarkovOptimization pg : MarkovOptimization.values()) {
            long cnt = optimizations_h.get(pg, 0);
            m4.put(pg.toString(), String.format(f, cnt, cnt / (double) total.get()));
            for (Penalty p : pg.penalties) {
                cnt = penalties_h.get(p, 0);
                m4.put(String.format("  + %s", p), String.format(f, cnt, cnt / (double) total.get()));
            } // FOR
            m4.put(m4.lastKey(), m4.get(m4.lastKey()) + "\n"); // .concat("\n");
        } // FOR

        ListOrderedMap<String, Object> m5 = new ListOrderedMap<String, Object>();
        for (Entry<Procedure, ObjectHistogram<MarkovOptimization>> e : proc_penalties_h.entrySet()) {
            if (e.getValue().isEmpty() == false)
                m5.put(e.getKey().getName(), e.getValue());
        }

        System.err.println(StringUtil.formatMaps(m0, m1, m2) + StringUtil.DOUBLE_LINE + StringUtil.formatMaps(m3, m4) + StringUtil.DOUBLE_LINE + StringUtil.formatMaps(m5));
    }
}