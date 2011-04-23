/**
 * 
 */
package edu.brown.costmodel;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.costmodel.SingleSitedCostModel.QueryCacheEntry;
import edu.brown.costmodel.SingleSitedCostModel.TransactionCacheEntry;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.partitioners.PartitionPlan;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.Workload;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload.Filter;

/**
 * @author pavlo
 */
public class TimeIntervalCostModel<T extends AbstractCostModel> extends AbstractCostModel {
    private static final Logger LOG = Logger.getLogger(TimeIntervalCostModel.class);

    /**
     * Internal cost models (one per interval)
     */
    private final int num_intervals;
    private final T cost_models[];
    private final Class<? extends T> inner_class;

    /**
     * For testing
     */
    protected double last_execution_cost;
    protected double last_entropy_cost;
    protected Double last_final_cost;
    
    protected String last_debug; 

    
    /**
     * Constructor 
     */
    @SuppressWarnings("unchecked")
    public TimeIntervalCostModel(Database catalog_db, Class<? extends T> inner_class, int num_intervals) {
        super(TimeIntervalCostModel.class, catalog_db, new PartitionEstimator(catalog_db));
        this.num_intervals = num_intervals;
        this.cost_models = (T[])(new AbstractCostModel[num_intervals]);
        this.inner_class = inner_class;
        
        try {
            Constructor constructor = ClassUtil.getConstructor(inner_class, Database.class, PartitionEstimator.class);
            for (int i = 0; i < this.cost_models.length; i++) {
                this.cost_models[i] = (T)constructor.newInstance(catalog_db, this.p_estimator);
            } // FOR
        } catch (Exception ex) {
            LOG.fatal("Failed to create the inner cost models", ex);
            System.exit(1);
        }
        assert(this.num_intervals > 0);
        LOG.debug("TimeIntervalCostModel: " + this.num_intervals + " intervals");
    }
    
    @Override
    public AbstractCostModel clone(Database catalog_db) throws CloneNotSupportedException {
        TimeIntervalCostModel<T> clone = new TimeIntervalCostModel<T>(catalog_db, this.inner_class, this.cost_models.length);
        return (clone);
    }
    
    @Override
    public void applyDesignerHints(DesignerHints hints) {
        super.applyDesignerHints(hints);
        for (T cm : this.cost_models) {
            cm.applyDesignerHints(hints);
        } // FOR
    }
    
    public String getLastDebugMessages() {
        return (this.last_debug);
    }
    
    public double getLastEntropyCost() {
        return last_entropy_cost;
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
        if (force || !this.isCachingEnabled()) {
            if (LOG.isDebugEnabled()) LOG.debug("Clearing out all interval cost models");
            for (int i = 0; i < this.num_intervals; i++) {
                this.cost_models[i].clear(force);
            } // FOR
        }
    }
    
    @Override
    public void setCachingEnabled(boolean useCaching) {
        super.setCachingEnabled(useCaching);
        for (int i = 0; i < this.num_intervals; i++) {
            this.cost_models[i].setCachingEnabled(useCaching);
        } // FOR
        assert(this.use_caching == useCaching);
    }
    
    /**
     * Return the inner cost model for the given time interval
     * @param interval
     * @return
     */
    public T getCostModel(int interval) {
        return (this.cost_models[interval]);
    }
    
    /**
     * Return the number of number of intervals
     * @return
     */
    public int getIntevalCount() {
        return (this.cost_models.length);
    }
    
    @Override
    public void prepareImpl(final Database catalog_db) {
        for (int i = 0; i < num_intervals; i++) {
            this.cost_models[i].prepare(catalog_db);
            if (!this.use_caching) {
                assert(this.cost_models[i].getTxnPartitionAccessHistogram().isEmpty());
                assert(this.cost_models[i].getQueryPartitionAccessHistogram().isEmpty());
            }
        } // FOR
    }
    
    /* (non-Javadoc)
     * @see edu.brown.costmodel.AbstractCostModel#estimateCost(org.voltdb.catalog.Database, edu.brown.workload.TransactionTrace, edu.brown.workload.AbstractWorkload.Filter)
     */
    @Override
    public double estimateTransactionCost(Database catalog_db, Workload workload, Filter filter, TransactionTrace xact) throws Exception {
        assert(workload != null) : "The workload handle is null";
        // First figure out the time interval of this 
        int interval = workload.getTimeInterval(xact, this.cost_models.length);
        return (this.cost_models[interval].estimateTransactionCost(catalog_db, workload, filter, xact));
    }
    
    /**
     * 
     */
    @Override
    public double estimateCost(Database catalog_db, Workload workload, Workload.Filter filter) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        this.last_debug = "";
        
        this.prepare(catalog_db);
        int num_partitions = CatalogUtil.getNumberOfPartitions(catalog_db);
        List<Integer> all_partitions = CatalogUtil.getAllPartitionIds(catalog_db);
        
        if (debug) LOG.debug("Calculating workload execution cost across " + num_intervals + " intervals for " + num_partitions + " partitions");

        // Note that we want to clear our counters but not our internal cost model data structures
        this.clear();
        
        // (1) Grab the costs at the different time intervals
        //     Also create the ratios that we will use to weight the interval costs
        Histogram histogram_proc_intervals = workload.getTimeIntervalProcedureHistogram(this.num_intervals);
        Histogram histogram_query_intervals = workload.getTimeIntervalQueryHistogram(this.num_intervals);
        final long total_txns = workload.getTransactionCount();
        final int singlepartition_ctrs[] = new int[num_intervals];
        final int singlepartition_with_partitions_ctrs[] = new int[num_intervals];
        final int multipartition_ctrs[] = new int[num_intervals];
        final int incomplete_txn_ctrs[] = new int[num_intervals];
        final int exec_mismatch_ctrs[] = new int[num_intervals]; // When the Java doesn't execute on the same machine as where the queries go
        final int partitions_touched[] = new int[num_intervals];
        final double interval_weights[] = new double[num_intervals];
        final double total_interval_txns[] = new double[num_intervals];
        final double total_interval_queries[] = new double[num_intervals];
        final Histogram incomplete_txn_histogram[] = new Histogram[num_intervals];
        final Histogram exec_histogram[] = new Histogram[num_intervals];
        final Histogram missing_txn_histogram[] = new Histogram[num_intervals];
//        final HashSet<Long> trace_ids[] = new HashSet[num_intervals]; 
        for (int i = 0; i < num_intervals; i++) {
//            trace_ids[i] = new HashSet<Long>();
            
            singlepartition_ctrs[i] = 0;
            singlepartition_with_partitions_ctrs[i] = 0;
            multipartition_ctrs[i] = 0;
            partitions_touched[i] = 0;
            incomplete_txn_ctrs[i] = 0;
            exec_mismatch_ctrs[i] = 0;
            total_interval_txns[i] = histogram_proc_intervals.get(i, 0);
            total_interval_queries[i] = histogram_query_intervals.get(i, 0);
            interval_weights[i] = total_interval_txns[i] / (double)total_txns;
            
            // This histogram is to keep track of those partitions that we need to add to the access histogram
            // in the entropy calculation. If a txn is incomplete (i.e., they have queries that we did not
            // calculate an estimate for), then we need to mark it as going to all partitions. So we have to make
            // sure we don't count the partitions that we *do* know the incomplete is going to more than once
            incomplete_txn_histogram[i] = new Histogram();
            
            missing_txn_histogram[i] = new Histogram();
            exec_histogram[i] = new Histogram();
            
        } // FOR
        
        // (2) Now go through the workload and estimate the partitions that each txn will touch
        //     for the given catalog setups
        if (trace) {
            LOG.trace("Total # of Txns in Workload: " + workload.getTransactionCount());
            LOG.trace("Workload Filter Chain:       " + filter);
        }
        Iterator<AbstractTraceElement<?>> it = workload.iterator(filter);
        while (it.hasNext()) {
            AbstractTraceElement<?> element = it.next();
            if (element instanceof TransactionTrace) {
                TransactionTrace txn_trace = (TransactionTrace)element;
                int i = workload.getTimeInterval(txn_trace, num_intervals);
                assert(i >= 0);
                assert(i < num_intervals) : "Invalid interval: " + i;
                try {
                    /*
                    if (trace_ids[i].contains(txn_trace.getId())) {
                        System.err.println(StringUtil.join("\n", trace_ids[i]));
                        throw new RuntimeException("Duplicate " + txn_trace + " for interval #" + i);
                    }
                    assert(!trace_ids[i].contains(txn_trace.getId())) : "Duplicate " + txn_trace + " for interval #" + i;
                    trace_ids[i].add(txn_trace.getId());
                    */
                    
                    // Terrible Hack: Assume that we are using the SingleSitedCostModel and that
                    // it will return fixed values based on whether the txn is single-partitioned or not
                    this.cost_models[i].estimateTransactionCost(catalog_db, workload, filter, txn_trace);
                    SingleSitedCostModel singlesited_cost_model = (SingleSitedCostModel)this.cost_models[i]; 
                    TransactionCacheEntry txn_entry = singlesited_cost_model.getTransactionCacheEntry(txn_trace);
                    assert(txn_entry != null) : "No txn entry for " + txn_trace;
                    Set<Integer> partitions = txn_entry.getTouchedPartitions();
                    
                    // If the txn runs on only one partition, then the cost is nothing
                    if (txn_entry.isSingleSited()) {
                        singlepartition_ctrs[i]++;
                        if (!partitions.isEmpty()) {
                            assert(txn_entry.getAllTouchedPartitionsHistogram().getValueCount() == 1) :
                                txn_entry + " says it was single-sited but the partition count says otherwise:\n" + txn_entry.debug();
                            singlepartition_with_partitions_ctrs[i]++;
                        }
                        this.histogram_sp_procs.put(CatalogKey.createKey(CatalogUtil.DEFAULT_DATABASE_NAME, txn_trace.getCatalogItemName()));
                        
                    // If the txn runs on multiple partitions, then the cost is...
                    // XXX 2010-06-28: The number of partitions that the txn touches divided by the total number of partitions
                    // XXX 2010-07-02: The histogram for the total number of partitions touched by all of the queries 
                    //                 in the transaction. This ensures that txns with just one multi-partition query
                    //                 isn't weighted the same as a txn with many multi-partition queries
                    } else {
                        assert(!partitions.isEmpty()) : "No touched partitions for " + txn_trace;
                        if (partitions.size() == 1 && txn_entry.getExecutionPartition() != null) {
                            assert(CollectionUtil.getFirst(partitions) != txn_entry.getExecutionPartition()) : txn_entry.debug();
                            exec_mismatch_ctrs[i]++;
                            partitions_touched[i]++;
                        } else {
                            assert(partitions.size() > 1) : txn_entry.debug();
                        }
                        partitions_touched[i] += partitions.size(); // Txns
                        multipartition_ctrs[i]++;
                        this.histogram_mp_procs.put(CatalogKey.createKey(CatalogUtil.DEFAULT_DATABASE_NAME, txn_trace.getCatalogItemName()));
                    }
                    Integer base_partition = txn_entry.getExecutionPartition();
                    if (base_partition != null) {
                        exec_histogram[i].put(base_partition);
                    } else {
                        exec_histogram[i].putAll(all_partitions);
                    }
                    if (trace) LOG.trace(txn_trace + ": " + (txn_entry.isSingleSited() ? "Single" : "Multi") + "-Sited [" +
                                         "singlep_ctrs=" + singlepartition_ctrs[i] + ", " +
                                         "singlep_with_partitions_ctrs=" + singlepartition_with_partitions_ctrs[i] + ", " +
                                         "p_touched=" + partitions_touched[i] + ", " +
                                         "exec_mismatch=" + exec_mismatch_ctrs[i] + "]");

                    // We need to keep a count of the number txns that didn't have all of its queries estimated
                    // completely so that we can update the access histograms down below for entropy calculations
                    // Note that this is at the txn level, not the query level.
                    if (!txn_entry.isComplete()) {
                        if (trace) LOG.trace("Marking " + txn_trace + " as incomplete in interval #" + i);
                        incomplete_txn_ctrs[i]++;
                        Set<Integer> missing_partitions = new HashSet<Integer>(all_partitions);
                        missing_partitions.removeAll(txn_entry.getTouchedPartitions());
                        // Update the histogram for this interval to keep track of how many times we need to
                        // increase the partition access histogram
                        incomplete_txn_histogram[i].putAll(missing_partitions);
                    }
                } catch (Exception ex) {
                    LOG.error("Failed to estimate cost for " + txn_trace.getCatalogItemName() + " at interval " + i);
                    CatalogUtil.saveCatalog(catalog_db.getCatalog(), "catalog.txt");
                    throw ex;
                }
            }
        } // WHILE
        
        // We have to convert all of the costs into the range of [0.0, 1.0]
        // For each interval, divide the number of partitions touched by the total number of partitions
        // that the interval could have touched (worst case scenario)
        final double execution_costs[] = new double[num_intervals];
        StringBuilder sb = new StringBuilder();

        ArrayList<Integer> touched = new ArrayList<Integer>();
        ArrayList<Long> potential = new ArrayList<Long>();
        ArrayList<Long> total = new ArrayList<Long>();
        ArrayList<Double> penalties = new ArrayList<Double>();
        
        if (debug) LOG.debug("Calculating execution cost for " + this.num_intervals + " intervals...");
        long total_multipartition_txns = 0;
        for (int i = 0; i < this.num_intervals; i++) {
            long total_txns_in_interval = histogram_proc_intervals.get(i, 0);
            long total_queries_in_interval = histogram_query_intervals.get(i, 0);
            long num_txns = this.cost_models[i].txn_ctr.get();
            long potential_txn_touches = (total_txns_in_interval * num_partitions); // TXNS
            double penalty = 0.0d;
            total_multipartition_txns += multipartition_ctrs[i];
            
            // Divide the total number of partitions touched by...
            // This is the total number of partitions that we could have touched in this interval
            // And this is the total number of partitions that we did actually touch
            if (multipartition_ctrs[i] > 0) {
                assert(partitions_touched[i] > 0) : "No touched partitions for interval " + i;
                double cost = (partitions_touched[i] / (double)potential_txn_touches);
                
                if (this.use_multitpartition_penalty) {
                    penalty = this.multipartition_penalty * (1.0d + (multipartition_ctrs[i] / (double)total_txns_in_interval));
                    assert(penalty >= 1.0) : "The multipartition penalty is less than one: " + penalty;
                    cost *= penalty;
                }
                execution_costs[i] = Math.min(cost, (double)potential_txn_touches);
            }
            
            // For each txn that wasn't even evaluated, add all of the partitions to the incomplete histogram
            if (num_txns < total_txns_in_interval) {
                if (trace) LOG.trace("Adding " + (total_txns_in_interval - num_txns) + " entries to the incomplete histogram for interval #" + i);
                for (long ii = num_txns; ii < total_txns_in_interval; ii++) {
                    missing_txn_histogram[i].putAll(all_partitions);
                } // WHILE
            }
            
            penalties.add(penalty);
            total.add(total_txns_in_interval);
            touched.add(partitions_touched[i]);
            potential.add(potential_txn_touches);
            
            if (trace) {
                sb.append("Interval #" + i + ": ")
                  .append("PartTouched=" + partitions_touched[i] + ", ")
                  .append("PotentialTouched="  + potential_txn_touches + ", ")
    //              .append("PotentialTouched="  + potential_query_touches + ", ")
                  .append("MultiPartTxns=" + multipartition_ctrs[i] + ", ")
                  .append("TotalTxns=" + total_txns_in_interval + ", ")
                  .append("TotalQueries=" + total_queries_in_interval + ", ")
                  .append("MissingTxns=" + (total_txns_in_interval - num_txns) + ", ")
                  .append("Cost=" + String.format("%.05f", execution_costs[i]) + ", ")
                  .append("ExecTxns: " + exec_histogram[i].getSampleCount())
                  .append("\n");
            }
        } // FOR
        
        if (debug) {
            sb.append("SinglePartition Txns: ").append(total_txns - total_multipartition_txns).append("\n");
            sb.append("MultiPartition Txns:  ").append(total_multipartition_txns).append("\n");
            sb.append("Total Txns:           ").append(total_txns).append(" [").append(1.0d - (total_multipartition_txns / (double)total_txns)).append("]\n");
            
            sb.append("\n");
            sb.append("touched = " + touched.toString() + "\n");
            sb.append("potential = " + potential.toString() + "\n");
            sb.append("total = " + total.toString() + "\n");
            sb.append("multipartition = " + Arrays.toString(multipartition_ctrs) + "\n");
            sb.append("penalties = " + penalties + "\n");
            LOG.debug("**** Execution Cost ****\n" + sb);
            this.last_debug += sb.toString();
        }
        
        // LOG.debug("Execution By Intervals:\n" + sb.toString());
        
        // (3) We then need to go through and grab the histograms of partitions were accessed
        if (debug) LOG.debug("Calculating skew factor for " + this.num_intervals + " intervals...");
        final double txn_skews[] = new double[num_intervals];
        final double exec_skews[] = new double[num_intervals];
        final double total_skews[] = new double[num_intervals];
        if (debug) sb = new StringBuilder();
        for (int i = 0; i < num_intervals; i++) {
            Histogram histogram_query = this.cost_models[i].getQueryPartitionAccessHistogram();
            this.histogram_query_partitions.putHistogram(histogram_query);
            long num_queries = this.cost_models[i].query_ctr.get();
            this.query_ctr.addAndGet(num_queries);
            
            Histogram histogram_txn = this.cost_models[i].getTxnPartitionAccessHistogram();

            // DEBUG
            SingleSitedCostModel singlesited_cost_model = (SingleSitedCostModel)this.cost_models[i]; 
            if (!((partitions_touched[i] + singlepartition_with_partitions_ctrs[i]) == (this.cost_models[i].getTxnPartitionAccessHistogram().getSampleCount() + exec_mismatch_ctrs[i]))) {
                System.err.println("Transaction Entries: " + singlesited_cost_model.getTransactionCacheEntries().size());
                
                Histogram check = new Histogram();
                for (TransactionCacheEntry tce : singlesited_cost_model.getTransactionCacheEntries()) {
                    check.putAll(tce.getTouchedPartitions());
                    System.err.println(tce.debug() + "\n");
                }
                System.err.println("Check Touched Partitions: sample=" + check.getSampleCount() + ", value=" + check.getValueCount());
                System.err.println("Cache Touched Partitions: sample=" + this.cost_models[i].getTxnPartitionAccessHistogram().getSampleCount() + ", value=" + this.cost_models[i].getTxnPartitionAccessHistogram().getValueCount());
                
                int qtotal = singlesited_cost_model.getQueryCacheEntries().size();
                int ctr = 0;
                int multip = 0;
                for (QueryCacheEntry qce : singlesited_cost_model.getQueryCacheEntries()) {
                    ctr += (qce.getAllPartitions().isEmpty() ? 0 : 1);
                    multip += qce.getAllPartitions().size() > 1 ? 1 : 0;
                }
                System.err.println("# of QueryCacheEntries with Touched Partitions: " + ctr + " / " + qtotal);
                System.err.println("# of MultiP QueryCacheEntries: " +  multip);
            }
            assert((partitions_touched[i] + singlepartition_with_partitions_ctrs[i]) == (this.cost_models[i].getTxnPartitionAccessHistogram().getSampleCount() + exec_mismatch_ctrs[i])) :
                "Partitions Touched by Txns Mismatch in Interval #" + i + "\n(" +
                "partitions_touched[" + partitions_touched[i] + "] + " +
                "singlepartition_with_partitions_ctrs[" + singlepartition_with_partitions_ctrs[i] + "]" +
                ") != (" +
                "histogram_txn[" + this.cost_models[i].getTxnPartitionAccessHistogram().getSampleCount() + "] + " +
                "exec_mismatch_ctrs[" + exec_mismatch_ctrs[i] + "]" +
                ")";
            
            
            this.histogram_txn_partitions.putHistogram(histogram_txn);
            long num_txns = this.cost_models[i].txn_ctr.get();
            assert(num_txns >= 0) : "The transaction counter at interval #" + i + " is " + num_txns;
            this.txn_ctr.addAndGet(num_txns);

            Histogram histogram_execute = this.cost_models[i].getJavaExecutionHistogram();
            this.histogram_java_partitions.putHistogram(histogram_execute);
            
            // Calculate the entropy value at this time interval
            // XXX: Should the number of txns be the total number of unique txns that were executed
            //      or the total number of times a txn touched the partitions?
            // XXX: What do we do when the number of elements that we are examining is zero? I guess
            //      the cost just needs to be zero?
            // XXX: What histogram do we want to use?
            Histogram target_histogram = new Histogram(histogram_txn);
            
            // For each txn that we haven't gotten an estimate for at this interval, we're going
            // mark it as being broadcast to all partitions. That way the access histogram will
            // look uniform. Then as more information is added, we will
            // This is an attempt to make sure that the entropy cost never decreases but only increases
            long total_txns_in_interval = histogram_proc_intervals.get(i, 0);
            if (trace) {
                sb.append("INTERVAL #" + i + " [total_txns_in_interval=" + total_txns_in_interval + ", num_txns=" + num_txns + ", incomplete_txns=" + incomplete_txn_ctrs[i] + ", sample_count=" + target_histogram.getSampleCount() + "]\n");
                sb.append("Incomplete Txn Histogram: [sample_count=" + incomplete_txn_histogram[i].getSampleCount() + "]\n");
                sb.append(incomplete_txn_histogram[i].toString() + "\n");
                sb.append("Missing Txn Histogram: [sample_count=" + missing_txn_histogram[i].getSampleCount() + "]\n");
                sb.append(missing_txn_histogram[i].toString() + "\n");
                sb.append("BEFORE: [sample_count=" + target_histogram.getSampleCount() + "]\n" + target_histogram.toString() + "\n");
            }

            // Merge the values from incomplete histogram into the target histogram            
            target_histogram.putHistogram(incomplete_txn_histogram[i]);
            target_histogram.putHistogram(missing_txn_histogram[i]);
            exec_histogram[i].putHistogram(missing_txn_histogram[i]);
            
            long num_elements = target_histogram.getSampleCount();
            
            // The number of partition touches should never be greater than our potential touches
            assert(num_elements <= potential.get(i)) : 
                "New Partitions Touched Sample Count [" + num_elements + "] < " +
                "Maximum Potential Touched Count [" + potential.get(i) + "]";
            if (trace) {
                sb.append("AFTER: [sample_count=" + num_elements + "]\n");
                sb.append(target_histogram + "\n");
            }
            
            // Txn Skew
            if (num_elements == 0) {
                txn_skews[i] = 0.0d;
            } else {
                txn_skews[i] = EntropyUtil.calculateEntropy(num_partitions, num_elements, target_histogram);
            }
            
            // Exec Skew
            if (exec_histogram[i].getSampleCount() == 0) {
                exec_skews[i] = 0.0d;
            } else {
                exec_skews[i] = EntropyUtil.calculateEntropy(num_partitions, exec_histogram[i].getSampleCount(), exec_histogram[i]);
            }
            total_skews[i] = (0.5 * exec_skews[i]) + (0.5 * txn_skews[i]); 
            
            if (trace) {
                sb.append("Txn Skew   = " + MathUtil.roundToDecimals(txn_skews[i], 6) + "\n");
                sb.append("Exec Skew  = " + MathUtil.roundToDecimals(exec_skews[i], 6) + "\n");
                sb.append("Total Skew = " + MathUtil.roundToDecimals(total_skews[i], 6) + "\n");
                sb.append(StringUtil.DOUBLE_LINE);
            }
        } // FOR
        if (debug && sb.length() > 0) {
            LOG.debug("**** Skew Factor ****\n" + sb);
            this.last_debug += sb.toString();
        }
        if (trace) {
            for (int i = 0; i < num_intervals; i++) {
                LOG.trace("Time Interval #" + i + "\n" +
                          "Total # of Txns: " + this.cost_models[i].txn_ctr.get() + "\n" +
                          "Multi-Partition Txns: " + multipartition_ctrs[i] + "\n" + 
                          "Execution Cost: " + execution_costs[i] + "\n" +
                          "ProcHistogram:\n" + this.cost_models[i].getProcedureHistogram().toString() + "\n" +
                          //"TransactionsPerPartitionHistogram:\n" + this.cost_models[i].getTxnPartitionAccessHistogram() + "\n" +
                          StringUtil.SINGLE_LINE);
            }
        }
        
        
        // (3) We can now calculate the final total estimate cost of this workload as the following
        // Just take the simple ratio of mp txns / all txns
        this.last_execution_cost = MathUtil.weightedMean(execution_costs, total_interval_txns); // MathUtil.roundToDecimals(MathUtil.geometricMean(execution_costs, MathUtil.GEOMETRIC_MEAN_ZERO), 10);
        
        // The final entropy cost needs to be weighted by the percentage of txns running in that interval
        // This will cause the partitions with few txns 
        
        
        this.last_entropy_cost = MathUtil.weightedMean(total_skews, total_interval_txns); // roundToDecimals(MathUtil.geometricMean(entropies, MathUtil.GEOMETRIC_MEAN_ZERO), 10);
        double new_final_cost = (this.use_execution ? (this.execution_weight * this.last_execution_cost) : 0) + 
                                (this.use_entropy ? (this.entropy_weight * this.last_entropy_cost) : 0);

        String f = "%.06f";
        if (debug) {
            sb = new StringBuilder();
            sb.append("Total Txns:      ").append(total_txns).append("\n");
            
            sb.append("Interval Txns:   [");
            for (double e : total_interval_txns) sb.append(e + ", ");
            sb.append("]\n");
            
            sb.append("Execution Costs: [");
            for (Double e : execution_costs) sb.append(String.format(f + ", ", e));
            sb.append("]\n");
            
            sb.append("Skew Factors:    [");
            for (Double e : total_skews) sb.append(String.format(f + ", ", e));
            sb.append("]\n");
            
            sb.append("Txn Skew:        [");
            for (Double e : txn_skews) sb.append(String.format(f + ", ", e));
            sb.append("]\n");
            
            sb.append("Exec Skew:       [");
            for (Double e : exec_skews) sb.append(String.format(f + ", ", e));
            sb.append("]\n");
            
            sb.append("Interval Weights:   [");
            for (Double e : interval_weights) sb.append(String.format(f + ", ", e));
            sb.append("]\n");
            
            sb.append(String.format("Final Cost " + f + " = " + f + " + " + f,  new_final_cost, this.last_execution_cost, this.last_entropy_cost));
            LOG.debug("\n" + sb);
            this.last_debug += sb.toString();
        }
        
        this.last_final_cost = new_final_cost;
        return (MathUtil.roundToDecimals(this.last_final_cost, 5));
    }
    
    /* (non-Javadoc)
     * @see edu.brown.costmodel.AbstractCostModel#invalidateCache(java.lang.String)
     */
    @Override
    public void invalidateCache(String catalog_key) {
        for (T cm : this.cost_models) {
            cm.invalidateCache(catalog_key);
        } // FOR
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
                ArgumentsParser.PARAM_CATALOG,
                ArgumentsParser.PARAM_WORKLOAD,
                ArgumentsParser.PARAM_PARTITION_PLAN,
                ArgumentsParser.PARAM_DESIGNER_INTERVALS
//                ArgumentsParser.PARAM_DESIGNER_HINTS
        );
        assert(args.workload.getTransactionCount() > 0) : "No transactions were loaded from " + args.workload;
        
        // If given a PartitionPlan, then update the catalog
        File pplan_path = new File(args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN));
        if (pplan_path.exists()) {
            PartitionPlan pplan = new PartitionPlan();
            pplan.load(pplan_path.getAbsolutePath(), args.catalog_db);
            pplan.apply(args.catalog_db);
            System.out.println("Applied PartitionPlan '" + pplan_path + "' to catalog\n" + pplan);
            System.out.print(StringUtil.DOUBLE_LINE);
        } else {
            System.err.println("PartitionPlan file '" + pplan_path + "' does not exist. Ignoring...");
        }
        System.out.flush();
        
        int num_intervals = args.num_intervals; // getIntParam(ArgumentsParser.PARAM_DESIGNER_INTERVALS);
        TimeIntervalCostModel<SingleSitedCostModel> costmodel = new TimeIntervalCostModel<SingleSitedCostModel>(args.catalog_db, SingleSitedCostModel.class, num_intervals);
        if (args.hasParam(ArgumentsParser.PARAM_DESIGNER_HINTS)) costmodel.applyDesignerHints(args.designer_hints);
        double cost = costmodel.estimateCost(args.catalog_db, args.workload);
        System.out.println("PARTITIONS = " + CatalogUtil.getNumberOfPartitions(args.catalog_db));
        System.out.println("INTERVALS  = " + args.num_intervals);
        System.out.println("COST       = " + cost);
        
//        for (int i = 0; i < num_intervals; i++) {
//            Histogram h = costmodel.getCostModel(i).histogram_txn_partitions;
//            h.setKeepZeroEntries(true);
//            for (Integer partition : CatalogUtil.getAllPartitionIds(args.catalog_db)) {
//                if (h.contains(partition) == false) h.put(partition, 0);
//            }
//            System.out.println(StringUtil.box("Interval #" + i, "+", 100) + "\n" + h);
//            System.out.println();
//        } // FOR
        //System.err.println(h);
        
    }
}