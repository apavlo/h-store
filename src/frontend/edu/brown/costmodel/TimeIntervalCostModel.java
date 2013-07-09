/**
 * 
 */
package edu.brown.costmodel;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.FixCatalog;
import edu.brown.costmodel.SingleSitedCostModel.QueryCacheEntry;
import edu.brown.costmodel.SingleSitedCostModel.TransactionCacheEntry;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.Consumer;
import edu.brown.utils.MathUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.Producer;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;

/**
 * @author pavlo
 */
public class TimeIntervalCostModel<T extends AbstractCostModel> extends AbstractCostModel {
    private static final Logger LOG = Logger.getLogger(TimeIntervalCostModel.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Internal cost models (one per interval)
     */
    private final int num_intervals;
    private final T cost_models[];

    private PartitionSet all_partitions;

    /**
     * For testing
     */
    protected double last_execution_cost;
    protected double last_skew_cost;
    protected Double last_final_cost;

    protected final Map<String, ObjectHistogram<?>> debug_histograms = new LinkedHashMap<String, ObjectHistogram<?>>();

    final ObjectHistogram<Integer> target_histogram = new ObjectHistogram<Integer>();

    /** The number of single-partition txns per interval */
    final int singlepartition_ctrs[];

    /**
     * The number of single-partition txns with actual partitions that we
     * calculated This differs from incomplete txns where we have to mark them
     * as single-partition because we don't know what they're actually going to
     * do
     */
    final int singlepartition_with_partitions_ctrs[];

    /**
     * 
     */
    final int multipartition_ctrs[];
    final int incomplete_txn_ctrs[];

    /**
     * When the Java doesn't execute on the same machine as where the queries go
     **/
    final int exec_mismatch_ctrs[];

    /** The number of partitions touched per interval **/
    final int partitions_touched[];
    final double interval_weights[];
    final double total_interval_txns[];
    final double total_interval_queries[];

    final double txn_skews[];
    final double exec_skews[];
    final double total_skews[];

    /**
     * This histogram is to keep track of those partitions that we need to add
     * to the access histogram in the entropy calculation. If a txn is
     * incomplete (i.e., they have queries that we did not calculate an estimate
     * for), then we need to mark it as going to all partitions. So we have to
     * make sure we don't count the partitions that we *do* know the incomplete
     * is going to more than once
     */
    final ObjectHistogram<Integer> incomplete_txn_histogram[];
    final ObjectHistogram<Integer> exec_histogram[];
    final ObjectHistogram<Integer> missing_txn_histogram[];

    /** Temporary Data Structures */
    final List<Integer> tmp_touched = new ArrayList<Integer>();
    final List<Long> tmp_total = new ArrayList<Long>();
    final List<Double> tmp_penalties = new ArrayList<Double>();
    final List<Long> tmp_potential = new ArrayList<Long>();

    /** Temporary mapping from intervals to Consumers */
    final Map<Integer, Consumer<Pair<TransactionTrace, Integer>>> tmp_consumers = new HashMap<Integer, Consumer<Pair<TransactionTrace, Integer>>>();

    /**
     * Constructor
     */
    @SuppressWarnings("unchecked")
    public TimeIntervalCostModel(CatalogContext catalogContext, Class<? extends T> inner_class, int num_intervals) {
        super(TimeIntervalCostModel.class, catalogContext, new PartitionEstimator(catalogContext));
        this.num_intervals = num_intervals;
        this.cost_models = (T[]) (new AbstractCostModel[num_intervals]);

        try {
            Constructor<?> constructor = ClassUtil.getConstructor(inner_class, Database.class, PartitionEstimator.class);
            for (int i = 0; i < this.cost_models.length; i++) {
                this.cost_models[i] = (T) constructor.newInstance(catalogContext, this.p_estimator);
            } // FOR
        } catch (Exception ex) {
            LOG.fatal("Failed to create the inner cost models", ex);
            System.exit(1);
        }
        assert (this.num_intervals > 0);
        if (trace.val)
            LOG.trace("TimeIntervalCostModel: " + this.num_intervals + " intervals");

        singlepartition_ctrs = new int[num_intervals];
        singlepartition_with_partitions_ctrs = new int[num_intervals];
        multipartition_ctrs = new int[num_intervals];
        incomplete_txn_ctrs = new int[num_intervals];
        exec_mismatch_ctrs = new int[num_intervals];
        partitions_touched = new int[num_intervals];
        interval_weights = new double[num_intervals];
        total_interval_txns = new double[num_intervals];
        total_interval_queries = new double[num_intervals];
        incomplete_txn_histogram = new ObjectHistogram[num_intervals];
        exec_histogram = new ObjectHistogram[num_intervals];
        missing_txn_histogram = new ObjectHistogram[num_intervals];

        txn_skews = new double[num_intervals];
        exec_skews = new double[num_intervals];
        total_skews = new double[num_intervals];

        for (int i = 0; i < num_intervals; i++) {
            incomplete_txn_histogram[i] = new ObjectHistogram<Integer>();
            exec_histogram[i] = new ObjectHistogram<Integer>();
            missing_txn_histogram[i] = new ObjectHistogram<Integer>();
        } // FOR
    }

    // @Override
    // public AbstractCostModel clone(CatalogContext catalogContext) throws
    // CloneNotSupportedException {
    // TimeIntervalCostModel<T> clone = new TimeIntervalCostModel<T>(catalog_db,
    // this.inner_class, this.cost_models.length);
    // return (clone);
    // }

    @Override
    public void applyDesignerHints(DesignerHints hints) {
        super.applyDesignerHints(hints);
        for (T cm : this.cost_models) {
            cm.applyDesignerHints(hints);
        } // FOR
    }

    public double getLastSkewCost() {
        return last_skew_cost;
    }

    public double getLastExecutionCost() {
        return last_execution_cost;
    }

    public Double getLastFinalCost() {
        return last_final_cost;
    }

    @Override
    public void clear(boolean force) {
        super.clear(force);
        if (force || this.isCachingEnabled() == false) {
            if (debug.val)
                LOG.debug("Clearing out all interval cost models");
            for (int i = 0; i < this.num_intervals; i++) {
                this.cost_models[i].clear(force);
            } // FOR
        }
        tmp_penalties.clear();
        tmp_potential.clear();
        tmp_total.clear();
        tmp_touched.clear();
    }

    @Override
    public void setCachingEnabled(boolean useCaching) {
        super.setCachingEnabled(useCaching);
        for (int i = 0; i < this.num_intervals; i++) {
            this.cost_models[i].setCachingEnabled(useCaching);
        } // FOR
        assert (this.use_caching == useCaching);
    }

    /**
     * Return the inner cost model for the given time interval
     * 
     * @param interval
     * @return
     */
    public T getCostModel(int interval) {
        return (this.cost_models[interval]);
    }

    /**
     * Return the number of number of intervals
     * 
     * @return
     */
    public int getIntevalCount() {
        return (this.cost_models.length);
    }

    @Override
    public void prepareImpl(final CatalogContext catalogContext) {
        this.all_partitions = catalogContext.getAllPartitionIds();
        assert (this.all_partitions.isEmpty() == false) : "No partitions???";

        for (int i = 0; i < num_intervals; i++) {
            this.cost_models[i].prepare(catalogContext);
            if (!this.use_caching) {
                this.cost_models[i].clear(true);
                assert (this.cost_models[i].getTxnPartitionAccessHistogram().isEmpty());
                assert (this.cost_models[i].getQueryPartitionAccessHistogram().isEmpty());
            }
        } // FOR

        // Note that we want to clear our counters but not our internal cost
        // model data structures
        this.clear();
    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.costmodel.AbstractCostModel#estimateCost(org.voltdb.catalog
     * .Database, edu.brown.workload.TransactionTrace,
     * edu.brown.workload.AbstractWorkload.Filter)
     */
    @Override
    public double estimateTransactionCost(CatalogContext catalogContext, Workload workload, Filter filter, TransactionTrace xact) throws Exception {
        assert (workload != null) : "The workload handle is null";
        // First figure out the time interval of this
        int interval = workload.getTimeInterval(xact, this.cost_models.length);
        return (this.cost_models[interval].estimateTransactionCost(catalogContext, workload, filter, xact));
    }

    /**
     * 
     */
    @Override
    protected double estimateWorkloadCostImpl(final CatalogContext catalogContext, final Workload workload, final Filter filter, final Double upper_bound) throws Exception {

        if (debug.val)
            LOG.debug("Calculating workload execution cost across " + num_intervals + " intervals for " + num_partitions + " partitions");

        // (1) Grab the costs at the different time intervals
        //     Also create the ratios that we will use to weight the interval costs
        final AtomicLong total_txns = new AtomicLong(0);

        // final HashSet<Long> trace_ids[] = new HashSet[num_intervals];
        for (int i = 0; i < num_intervals; i++) {
            total_interval_txns[i] = 0;
            total_interval_queries[i] = 0;
            singlepartition_ctrs[i] = 0;
            singlepartition_with_partitions_ctrs[i] = 0;
            multipartition_ctrs[i] = 0;
            partitions_touched[i] = 0;
            incomplete_txn_ctrs[i] = 0;
            exec_mismatch_ctrs[i] = 0;
            incomplete_txn_histogram[i].clear();
            missing_txn_histogram[i].clear();
            exec_histogram[i].clear();
        } // FOR

        // (2) Now go through the workload and estimate the partitions that each txn 
        //     will touch for the given catalog setups
        if (trace.val) {
            LOG.trace("Total # of Txns in Workload: " + workload.getTransactionCount());
            if (filter != null)
                LOG.trace("Workload Filter Chain:       " + StringUtil.join("   ", "\n", filter.getFilters()));
        }

        // QUEUING THREAD
        tmp_consumers.clear();
        Producer<TransactionTrace, Pair<TransactionTrace, Integer>> producer = new Producer<TransactionTrace, Pair<TransactionTrace, Integer>>(CollectionUtil.iterable(workload.iterator(filter))) {
            @Override
            public Pair<Consumer<Pair<TransactionTrace, Integer>>, Pair<TransactionTrace, Integer>> transform(TransactionTrace txn_trace) {
                int i = workload.getTimeInterval(txn_trace, num_intervals);
                assert (i >= 0) : "Invalid time interval '" + i + "'\n" + txn_trace.debug(catalogContext.database);
                assert (i < num_intervals) : "Invalid interval: " + i + "\n" + txn_trace.debug(catalogContext.database);
                total_txns.incrementAndGet();
                Pair<TransactionTrace, Integer> p = Pair.of(txn_trace, i);
                return (Pair.of(tmp_consumers.get(i), p));
            }
        };

        // PROCESSING THREADS
        final int num_threads = ThreadUtil.getMaxGlobalThreads();
        int interval_ctr = 0;
        for (int thread = 0; thread < num_threads; thread++) {
            // First create a new IntervalProcessor/Consumer
            IntervalProcessor ip = new IntervalProcessor(catalogContext, workload, filter);

            // Then assign it to some number of intervals
            for (int i = 0, cnt = (int) Math.ceil(num_intervals / (double) num_threads); i < cnt; i++) {
                if (interval_ctr > num_intervals)
                    break;
                tmp_consumers.put(interval_ctr++, ip);
                if (trace.val)
                    LOG.trace(String.format("Interval #%02d => IntervalProcessor #%02d", interval_ctr - 1, thread));
            } // FOR

            // And make sure that we queue it up too
            producer.addConsumer(ip);
        } // FOR (threads)

        ThreadUtil.runGlobalPool(producer.getRunnablesList()); // BLOCKING
        if (debug.val) {
            int processed = 0;
            for (Consumer<?> c : producer.getConsumers()) {
                processed += c.getProcessedCounter();
            } // FOR
            assert (total_txns.get() == processed) : String.format("Expected[%d] != Processed[%d]", total_txns.get(), processed);
        }

        // We have to convert all of the costs into the range of [0.0, 1.0]
        // For each interval, divide the number of partitions touched by the total number 
        // of partitions that the interval could have touched (worst case scenario)
        final double execution_costs[] = new double[num_intervals];
        StringBuilder sb = (this.isDebugEnabled() || debug.val ? new StringBuilder() : null);
        Map<String, Object> debug_m = null;
        if (sb != null) {
            debug_m = new LinkedHashMap<String, Object>();
        }

        if (debug.val)
            LOG.debug("Calculating execution cost for " + this.num_intervals + " intervals...");
        long total_multipartition_txns = 0;
        for (int i = 0; i < this.num_intervals; i++) {
            interval_weights[i] = total_interval_txns[i] / (double) total_txns.get();
            long total_txns_in_interval = (long) total_interval_txns[i];
            long total_queries_in_interval = (long) total_interval_queries[i];
            long num_txns = this.cost_models[i].txn_ctr.get();
            long potential_txn_touches = (total_txns_in_interval * num_partitions); // TXNS
            double penalty = 0.0d;
            total_multipartition_txns += multipartition_ctrs[i];

            // Divide the total number of partitions touched by...
            // This is the total number of partitions that we could have touched
            // in this interval
            // And this is the total number of partitions that we did actually touch
            if (multipartition_ctrs[i] > 0) {
                assert (partitions_touched[i] > 0) : "No touched partitions for interval " + i;
                double cost = (partitions_touched[i] / (double) potential_txn_touches);

                if (this.use_multitpartition_penalty) {
                    penalty = this.multipartition_penalty * (1.0d + (multipartition_ctrs[i] / (double) total_txns_in_interval));
                    assert (penalty >= 1.0) : "The multipartition penalty is less than one: " + penalty;
                    cost *= penalty;
                }
                execution_costs[i] = Math.min(cost, (double) potential_txn_touches);
            }

            // For each txn that wasn't even evaluated, add all of the
            // partitions to the incomplete histogram
            if (num_txns < total_txns_in_interval) {
                if (trace.val)
                    LOG.trace("Adding " + (total_txns_in_interval - num_txns) + " entries to the incomplete histogram for interval #" + i);
                for (long ii = num_txns; ii < total_txns_in_interval; ii++) {
                    missing_txn_histogram[i].put(all_partitions);
                } // WHILE
            }

            if (sb != null) {
                tmp_penalties.add(penalty);
                tmp_total.add(total_txns_in_interval);
                tmp_touched.add(partitions_touched[i]);
                tmp_potential.add(potential_txn_touches);

                Map<String, Object> inner = new LinkedHashMap<String, Object>();
                inner.put("Partitions Touched", partitions_touched[i]);
                inner.put("Potential Touched", potential_txn_touches);
                inner.put("Multi-Partition Txns", multipartition_ctrs[i]);
                inner.put("Total Txns", total_txns_in_interval);
                inner.put("Total Queries", total_queries_in_interval);
                inner.put("Missing Txns", (total_txns_in_interval - num_txns));
                inner.put("Cost", String.format("%.05f", execution_costs[i]));
                inner.put("Exec Txns", exec_histogram[i].getSampleCount());
                debug_m.put("Interval #" + i, inner);
            }
        } // FOR

        if (sb != null) {
            Map<String, Object> m0 = new LinkedHashMap<String, Object>();
            m0.put("SinglePartition Txns", (total_txns.get() - total_multipartition_txns));
            m0.put("MultiPartition Txns", total_multipartition_txns);
            m0.put("Total Txns", String.format("%d [%.06f]", total_txns.get(), (1.0d - (total_multipartition_txns / (double) total_txns.get()))));

            Map<String, Object> m1 = new LinkedHashMap<String, Object>();
            m1.put("Touched Partitions", tmp_touched);
            m1.put("Potential Partitions", tmp_potential);
            m1.put("Total Partitions", tmp_total);
            m1.put("Penalties", tmp_penalties);

            sb.append(StringUtil.formatMaps(debug_m, m0, m1));
            if (debug.val)
                LOG.debug("**** Execution Cost ****\n" + sb);
            this.appendDebugMessage(sb);
        }

        // LOG.debug("Execution By Intervals:\n" + sb.toString());

        // (3) We then need to go through and grab the histograms of partitions were accessed
        if (sb != null) {
            if (debug.val)
                LOG.debug("Calculating skew factor for " + this.num_intervals + " intervals...");
            debug_histograms.clear();
            sb = new StringBuilder();
        }
        for (int i = 0; i < this.num_intervals; i++) {
            Histogram<Integer> histogram_txn = this.cost_models[i].getTxnPartitionAccessHistogram();
            Histogram<Integer> histogram_query = this.cost_models[i].getQueryPartitionAccessHistogram();
            this.histogram_query_partitions.put(histogram_query);
            long num_queries = this.cost_models[i].query_ctr.get();
            this.query_ctr.addAndGet(num_queries);

            // DEBUG
            SingleSitedCostModel inner_costModel = (SingleSitedCostModel) this.cost_models[i];
            boolean is_valid = (partitions_touched[i] + singlepartition_with_partitions_ctrs[i]) == (this.cost_models[i].getTxnPartitionAccessHistogram().getSampleCount() + exec_mismatch_ctrs[i]);
            if (!is_valid) {
                LOG.error("Transaction Entries: " + inner_costModel.getTransactionCacheEntries().size());
                ObjectHistogram<Integer> check = new ObjectHistogram<Integer>();
                for (TransactionCacheEntry tce : inner_costModel.getTransactionCacheEntries()) {
                    check.put(tce.getTouchedPartitions());
                    // LOG.error(tce.debug() + "\n");
                }
                LOG.error("Check Touched Partitions: sample=" + check.getSampleCount() + ", values=" + check.getValueCount());
                LOG.error("Cache Touched Partitions: sample=" + this.cost_models[i].getTxnPartitionAccessHistogram().getSampleCount() + ", values="
                        + this.cost_models[i].getTxnPartitionAccessHistogram().getValueCount());

                int qtotal = inner_costModel.getAllQueryCacheEntries().size();
                int ctr = 0;
                int multip = 0;
                for (QueryCacheEntry qce : inner_costModel.getAllQueryCacheEntries()) {
                    ctr += (qce.getAllPartitions().isEmpty() ? 0 : 1);
                    multip += (qce.getAllPartitions().size() > 1 ? 1 : 0);
                } // FOR
                LOG.error("# of QueryCacheEntries with Touched Partitions: " + ctr + " / " + qtotal);
                LOG.error("# of MultiP QueryCacheEntries: " + multip);
            }
            assert (is_valid) : String.format("Partitions Touched by Txns Mismatch in Interval #%d\n" + "(partitions_touched[%d] + singlepartition_with_partitions_ctrs[%d]) != "
                    + "(histogram_txn[%d] + exec_mismatch_ctrs[%d])", i, partitions_touched[i], singlepartition_with_partitions_ctrs[i], this.cost_models[i].getTxnPartitionAccessHistogram()
                    .getSampleCount(), exec_mismatch_ctrs[i]);

            this.histogram_java_partitions.put(this.cost_models[i].getJavaExecutionHistogram());
            this.histogram_txn_partitions.put(histogram_txn);
            long num_txns = this.cost_models[i].txn_ctr.get();
            assert (num_txns >= 0) : "The transaction counter at interval #" + i + " is " + num_txns;
            this.txn_ctr.addAndGet(num_txns);

            // Calculate the skew factor at this time interval
            // XXX: Should the number of txns be the total number of unique txns
            //      that were executed or the total number of times a txn touched the partitions?
            // XXX: What do we do when the number of elements that we are examining is zero?
            //      I guess the cost just needs to be zero?
            // XXX: What histogram do we want to use?
            target_histogram.clear();
            target_histogram.put(histogram_txn);

            // For each txn that we haven't gotten an estimate for at this interval, 
            // we're going mark it as being broadcast to all partitions. That way the access
            // histogram will look uniform. Then as more information is added, we will
            // This is an attempt to make sure that the skew cost never decreases but only increases
            long total_txns_in_interval = (long) total_interval_txns[i];
            if (sb != null) {
                debug_histograms.put("Incomplete Txns", incomplete_txn_histogram[i]);
                debug_histograms.put("Missing Txns", missing_txn_histogram[i]);
                debug_histograms.put("Target Partitions (BEFORE)", new ObjectHistogram<Integer>(target_histogram));
                debug_histograms.put("Target Partitions (AFTER)", target_histogram);
            }

            // Merge the values from incomplete histogram into the target
            // histogram
            target_histogram.put(incomplete_txn_histogram[i]);
            target_histogram.put(missing_txn_histogram[i]);
            exec_histogram[i].put(missing_txn_histogram[i]);

            long num_elements = target_histogram.getSampleCount();

            // The number of partition touches should never be greater than our
            // potential touches
            assert(num_elements <= (total_txns_in_interval * num_partitions)) :
                "New Partitions Touched Sample Count [" + num_elements + "] < " + 
                "Maximum Potential Touched Count [" + (total_txns_in_interval * num_partitions) + "]";

            if (sb != null) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                for (String key : debug_histograms.keySet()) {
                    ObjectHistogram<?> h = debug_histograms.get(key);
                    m.put(key, String.format("[Sample=%d, Value=%d]\n%s", h.getSampleCount(), h.getValueCount(), h));
                } // FOR
                sb.append(String.format("INTERVAL #%d [total_txns_in_interval=%d, num_txns=%d, incomplete_txns=%d]\n%s", i, total_txns_in_interval, num_txns, incomplete_txn_ctrs[i],
                        StringUtil.formatMaps(m)));
            }

            // Txn Skew
            if (num_elements == 0) {
                txn_skews[i] = 0.0d;
            } else {
                txn_skews[i] = SkewFactorUtil.calculateSkew(num_partitions, num_elements, target_histogram);
            }

            // Exec Skew
            if (exec_histogram[i].getSampleCount() == 0) {
                exec_skews[i] = 0.0d;
            } else {
                exec_skews[i] = SkewFactorUtil.calculateSkew(num_partitions, exec_histogram[i].getSampleCount(), exec_histogram[i]);
            }
            total_skews[i] = (0.5 * exec_skews[i]) + (0.5 * txn_skews[i]);

            if (sb != null) {
                sb.append("Txn Skew   = " + MathUtil.roundToDecimals(txn_skews[i], 6) + "\n");
                sb.append("Exec Skew  = " + MathUtil.roundToDecimals(exec_skews[i], 6) + "\n");
                sb.append("Total Skew = " + MathUtil.roundToDecimals(total_skews[i], 6) + "\n");
                sb.append(StringUtil.DOUBLE_LINE);
            }
        } // FOR
        if (sb != null && sb.length() > 0) {
            if (debug.val)
                LOG.debug("**** Skew Factor ****\n" + sb);
            this.appendDebugMessage(sb);
        }
        if (trace.val) {
            for (int i = 0; i < num_intervals; i++) {
                LOG.trace("Time Interval #" + i + "\n" + "Total # of Txns: " + this.cost_models[i].txn_ctr.get() + "\n" + "Multi-Partition Txns: " + multipartition_ctrs[i] + "\n" + "Execution Cost: "
                        + execution_costs[i] + "\n" + "ProcHistogram:\n" + this.cost_models[i].getProcedureHistogram().toString() + "\n" +
                        // "TransactionsPerPartitionHistogram:\n" +
                        // this.cost_models[i].getTxnPartitionAccessHistogram()
                        // + "\n" +
                        StringUtil.SINGLE_LINE);
            }
        }

        // (3) We can now calculate the final total estimate cost of this workload as the following 
        //     Just take the simple ratio of mp txns / all txns
        this.last_execution_cost = MathUtil.weightedMean(execution_costs, total_interval_txns); // MathUtil.roundToDecimals(MathUtil.geometricMean(execution_costs,
                                                                                                // MathUtil.GEOMETRIC_MEAN_ZERO),
                                                                                                // 10);

        // The final skew cost needs to be weighted by the percentage of txns running in that interval
        // This will cause the partitions with few txns
        this.last_skew_cost = MathUtil.weightedMean(total_skews, total_interval_txns); // roundToDecimals(MathUtil.geometricMean(entropies,
                                                                                       // MathUtil.GEOMETRIC_MEAN_ZERO),
                                                                                       // 10);
        double new_final_cost = (this.use_execution ? (this.execution_weight * this.last_execution_cost) : 0) + (this.use_skew ? (this.skew_weight * this.last_skew_cost) : 0);

        if (sb != null) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Total Txns", total_txns.get());
            m.put("Interval Txns", Arrays.toString(total_interval_txns));
            m.put("Execution Costs", Arrays.toString(execution_costs));
            m.put("Skew Factors", Arrays.toString(total_skews));
            m.put("Txn Skew", Arrays.toString(txn_skews));
            m.put("Exec Skew", Arrays.toString(exec_skews));
            m.put("Interval Weights", Arrays.toString(interval_weights));
            m.put("Final Cost", String.format("%f = %f + %f", new_final_cost, this.last_execution_cost, this.last_skew_cost));
            if (debug.val)
                LOG.debug(StringUtil.formatMaps(m));
            this.appendDebugMessage(StringUtil.formatMaps(m));
        }

        this.last_final_cost = new_final_cost;
        return (MathUtil.roundToDecimals(this.last_final_cost, 5));
    }

    /**
     * 
     */
    private class IntervalProcessor extends Consumer<Pair<TransactionTrace, Integer>> {

        final Set<Integer> tmp_missingPartitions = new HashSet<Integer>();
        final CatalogContext catalogContext;
        final Workload workload;
        final Filter filter;

        public IntervalProcessor(CatalogContext catalogContext, final Workload workload, final Filter filter) {
            this.catalogContext = catalogContext;
            this.workload = workload;
            this.filter = filter;
        }

        @Override
        public void process(Pair<TransactionTrace, Integer> p) {
            assert (p != null);
            final TransactionTrace txn_trace = p.getFirst();
            final int i = p.getSecond(); // Interval
            final int txn_weight = (use_txn_weights ? txn_trace.getWeight() : 1);
            final String proc_key = CatalogKey.createKey(CatalogUtil.DEFAULT_DATABASE_NAME, txn_trace.getCatalogItemName());

            // Terrible Hack: Assume that we are using the SingleSitedCostModel
            // and that
            // it will return fixed values based on whether the txn is
            // single-partitioned or not
            SingleSitedCostModel singlesited_cost_model = (SingleSitedCostModel) cost_models[i];

            total_interval_txns[i] += txn_weight;
            total_interval_queries[i] += (txn_trace.getQueryCount() * txn_weight);
            histogram_procs.put(proc_key, txn_weight);

            try {
                singlesited_cost_model.estimateTransactionCost(catalogContext, workload, filter, txn_trace);
                TransactionCacheEntry txn_entry = singlesited_cost_model.getTransactionCacheEntry(txn_trace);
                assert (txn_entry != null) : "No txn entry for " + txn_trace;
                Collection<Integer> partitions = txn_entry.getTouchedPartitions();

                // If the txn runs on only one partition, then the cost is
                // nothing
                if (txn_entry.isSinglePartitioned()) {
                    singlepartition_ctrs[i] += txn_weight;
                    if (!partitions.isEmpty()) {
                        assert (txn_entry.getAllTouchedPartitionsHistogram().getValueCount() == 1) : txn_entry + " says it was single-partitioned but the partition count says otherwise:\n"
                                + txn_entry.debug();
                        singlepartition_with_partitions_ctrs[i] += txn_weight;
                    }
                    histogram_sp_procs.put(proc_key, txn_weight);

                    // If the txn runs on multiple partitions, then the cost
                    // is...
                    // XXX 2010-06-28: The number of partitions that the txn
                    // touches divided by the total number of partitions
                    // XXX 2010-07-02: The histogram for the total number of
                    // partitions touched by all of the queries
                    // in the transaction. This ensures that txns with just one
                    // multi-partition query
                    // isn't weighted the same as a txn with many
                    // multi-partition queries
                } else {
                    assert (!partitions.isEmpty()) : "No touched partitions for " + txn_trace;
                    if (partitions.size() == 1 && txn_entry.getExecutionPartition() != HStoreConstants.NULL_PARTITION_ID) {
                        assert (CollectionUtil.first(partitions) != txn_entry.getExecutionPartition()) : txn_entry.debug();
                        exec_mismatch_ctrs[i] += txn_weight;
                        partitions_touched[i] += txn_weight;
                    } else {
                        assert (partitions.size() > 1) : String.format("%s is not marked as single-partition but it only touches one partition\n%s", txn_trace, txn_entry.debug());
                    }
                    partitions_touched[i] += (partitions.size() * txn_weight); // Txns
                    multipartition_ctrs[i] += txn_weight;
                    histogram_mp_procs.put(proc_key, txn_weight);
                }
                Integer base_partition = txn_entry.getExecutionPartition();
                if (base_partition != null) {
                    exec_histogram[i].put(base_partition, txn_weight);
                } else {
                    exec_histogram[i].put(all_partitions, txn_weight);
                }
                if (debug.val) { // &&
                                   // txn_trace.getCatalogItemName().equalsIgnoreCase("DeleteCallForwarding"))
                                   // {
                    Procedure catalog_proc = txn_trace.getCatalogItem(catalogContext.database);
                    Map<String, Object> inner = new LinkedHashMap<String, Object>();
                    for (Statement catalog_stmt : catalog_proc.getStatements()) {
                        inner.put(catalog_stmt.fullName(), CatalogUtil.getReferencedTables(catalog_stmt));
                    }

                    Map<String, Object> m = new LinkedHashMap<String, Object>();
                    m.put(txn_trace.toString(), null);
                    m.put("Interval", i);
                    m.put("Single-Partition", txn_entry.isSinglePartitioned());
                    m.put("Base Partition", base_partition);
                    m.put("Touched Partitions", partitions);
                    m.put(catalog_proc.fullName(), inner);
                    LOG.debug(StringUtil.formatMaps(m));
                }

                // We need to keep a count of the number txns that didn't have
                // all of its queries estimated
                // completely so that we can update the access histograms down
                // below for entropy calculations
                // Note that this is at the txn level, not the query level.
                if (!txn_entry.isComplete()) {
                    incomplete_txn_ctrs[i] += txn_weight;
                    tmp_missingPartitions.clear();
                    tmp_missingPartitions.addAll(all_partitions);
                    tmp_missingPartitions.removeAll(txn_entry.getTouchedPartitions());
                    // Update the histogram for this interval to keep track of
                    // how many times we need to
                    // increase the partition access histogram
                    incomplete_txn_histogram[i].put(tmp_missingPartitions, txn_weight);
                    if (trace.val) {
                        Map<String, Object> m = new LinkedHashMap<String, Object>();
                        m.put(String.format("Marking %s as incomplete in interval #%d", txn_trace, i), null);
                        m.put("Examined Queries", txn_entry.getExaminedQueryCount());
                        m.put("Total Queries", txn_entry.getTotalQueryCount());
                        m.put("Touched Partitions", txn_entry.getTouchedPartitions());
                        m.put("Missing Partitions", tmp_missingPartitions);
                        LOG.trace(StringUtil.formatMaps(m));
                    }
                }
            } catch (Exception ex) {
                CatalogUtil.saveCatalog(catalogContext.catalog, CatalogUtil.CATALOG_FILENAME);
                throw new RuntimeException("Failed to estimate cost for " + txn_trace.getCatalogItemName() + " at interval " + i, ex);
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.costmodel.AbstractCostModel#invalidateCache(java.lang.String)
     */
    @Override
    public void invalidateCache(String catalog_key) {
        for (T cm : this.cost_models) {
            cm.invalidateCache(catalog_key);
        } // FOR
    }

    /**
     * MAIN!
     * 
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD, ArgumentsParser.PARAM_PARTITION_PLAN, ArgumentsParser.PARAM_DESIGNER_INTERVALS
        // ArgumentsParser.PARAM_DESIGNER_HINTS
        );
        assert (args.workload.getTransactionCount() > 0) : "No transactions were loaded from " + args.workload;

        if (args.hasParam(ArgumentsParser.PARAM_CATALOG_HOSTS)) {
            ClusterConfiguration cc = new ClusterConfiguration(args.getParam(ArgumentsParser.PARAM_CATALOG_HOSTS));
            args.updateCatalog(FixCatalog.cloneCatalog(args.catalog, cc), null);
        }

        // If given a PartitionPlan, then update the catalog
        File pplan_path = new File(args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN));
        if (pplan_path.exists()) {
            PartitionPlan pplan = new PartitionPlan();
            pplan.load(pplan_path, args.catalog_db);
            if (args.getBooleanParam(ArgumentsParser.PARAM_PARTITION_PLAN_REMOVE_PROCS, false)) {
                for (Procedure catalog_proc : pplan.proc_entries.keySet()) {
                    pplan.setNullProcParameter(catalog_proc);
                } // FOR
            }
            if (args.getBooleanParam(ArgumentsParser.PARAM_PARTITION_PLAN_RANDOM_PROCS, false)) {
                for (Procedure catalog_proc : pplan.proc_entries.keySet()) {
                    pplan.setRandomProcParameter(catalog_proc);
                } // FOR
            }
            pplan.apply(args.catalog_db);
            System.out.println("Applied PartitionPlan '" + pplan_path + "' to catalog\n" + pplan);
            System.out.print(StringUtil.DOUBLE_LINE);

            if (args.hasParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT)) {
                String output = args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT);
                if (output.equals("-"))
                    output = pplan_path.getAbsolutePath();
                pplan.save(new File(output));
                System.out.println("Saved PartitionPlan to '" + output + "'");
            }
        } else {
            System.err.println("PartitionPlan file '" + pplan_path + "' does not exist. Ignoring...");
        }
        System.out.flush();

        int num_intervals = args.num_intervals; // getIntParam(ArgumentsParser.PARAM_DESIGNER_INTERVALS);
        TimeIntervalCostModel<SingleSitedCostModel> costmodel = new TimeIntervalCostModel<SingleSitedCostModel>(args.catalogContext, SingleSitedCostModel.class, num_intervals);
        if (args.hasParam(ArgumentsParser.PARAM_DESIGNER_HINTS))
            costmodel.applyDesignerHints(args.designer_hints);
        double cost = costmodel.estimateWorkloadCost(args.catalogContext, args.workload);

        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("PARTITIONS", args.catalogContext.numberOfPartitions);
        m.put("INTERVALS", args.num_intervals);
        m.put("EXEC COST", costmodel.last_execution_cost);
        m.put("SKEW COST", costmodel.last_skew_cost);
        m.put("TOTAL COST", cost);
        m.put("PARTITIONS TOUCHED", costmodel.getTxnPartitionAccessHistogram().getSampleCount());
        System.out.println(StringUtil.formatMaps(m));

        // long total = 0;
        m.clear();
        // for (int i = 0; i < num_intervals; i++) {
        // SingleSitedCostModel cm = costmodel.getCostModel(i);
        // Histogram<Integer> h = cm.getTxnPartitionAccessHistogram();
        // m.put(String.format("Interval %02d", i),
        // cm.getTxnPartitionAccessHistogram());
        // total += h.getSampleCount();
        // h.setKeepZeroEntries(true);
        // for (Integer partition :
        // CatalogUtil.getAllPartitionIds(args.catalog_db)) {
        // if (h.contains(partition) == false) h.put(partition, 0);
        // }
        // System.out.println(StringUtil.box("Interval #" + i, "+", 100) + "\n"
        // + h);
        // System.out.println();
        // } // FOR
        // System.out.println(StringUtil.formatMaps(m));
        // System.err.println("TOTAL: " + total);

    }
}