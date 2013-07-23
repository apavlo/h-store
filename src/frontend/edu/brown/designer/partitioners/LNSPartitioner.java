/**
 * 
 */
package edu.brown.designer.partitioners;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.costmodel.AbstractCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.StringBoxUtil;
import edu.brown.utils.StringUtil;

/**
 * Large-Neighborhood Search Partitioner
 * 
 * @author pavlo
 */
public class LNSPartitioner extends AbstractPartitioner implements JSONSerializable {
    protected static final Logger LOG = Logger.getLogger(LNSPartitioner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final String DEBUG_COST_FORMAT = "%.04f";

    // ----------------------------------------------------------------------------
    // CHECKPOINT STATE INFORMATION
    // ----------------------------------------------------------------------------
    public PartitionPlan initial_solution;
    public double initial_cost;
    public double initial_memory;

    public PartitionPlan best_solution;
    public double best_cost;
    public double best_memory;

    public TimestampType start_time = null;
    public ProfileMeasurement total_search_time = new ProfileMeasurement("total");
    public TimestampType last_checkpoint = null;
    public int last_relax_size = 0;
    public int last_elapsed_time = 0;
    public Long last_backtrack_count = null;
    public Double last_backtrack_limit = null;
    public Double last_localtime_limit = null;
    public HaltReason last_halt_reason = HaltReason.NULL;
    public Double last_entropy_weight = null;
    public Integer restart_ctr = null;

    // ----------------------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // ----------------------------------------------------------------------------

    protected final AbstractCostModel costmodel;
    protected final ParameterMappingsSet mappings;
    protected AccessGraph agraph;
    protected transient boolean init_called = false;

    /**
     * Keep track of our sets of relaxed tables When we've processed all
     * combinations for the current relaxation size, we can then automatically
     * jump to the next size without waiting for the timeout
     */
    protected final Set<Collection<Table>> relaxed_sets = new HashSet<Collection<Table>>();
    protected transient BigInteger relaxed_sets_max = null;

    // ----------------------------------------------------------------------------
    // PRE-COMPUTED CATALOG INFORMATION
    // ----------------------------------------------------------------------------

    /** TODO */
    protected final ListOrderedMap<Table, ListOrderedSet<Column>> orig_table_attributes = new ListOrderedMap<Table, ListOrderedSet<Column>>();

    /** Estimate total size of each Table if split by the number of partitions */
    protected final Map<Table, Long> table_nonreplicated_size = new HashMap<Table, Long>();
    /** Estimate total size of each Table if replicated at all partitions */
    protected final Map<Table, Long> table_replicated_size = new HashMap<Table, Long>();

    /**
     * Mapping from Table to the set of Procedures that has a Statement that
     * references that Table
     */
    protected final Map<Table, Collection<Procedure>> table_procedures = new HashMap<Table, Collection<Procedure>>();

    /**
     * Mapping from Column to the set of Procedures that has a Statement that
     * references that Column
     */
    protected final Map<Column, Collection<Procedure>> column_procedures = new HashMap<Column, Collection<Procedure>>();

    protected final Map<Column, Map<Column, Set<Procedure>>> columnswap_procedures = new HashMap<Column, Map<Column, Set<Procedure>>>();

    protected final ListOrderedMap<Procedure, ListOrderedSet<ProcParameter>> orig_proc_attributes = new ListOrderedMap<Procedure, ListOrderedSet<ProcParameter>>();
    protected final Map<Procedure, Collection<Column>> proc_columns = new HashMap<Procedure, Collection<Column>>();
    protected final Map<Procedure, ObjectHistogram<Column>> proc_column_histogram = new HashMap<Procedure, ObjectHistogram<Column>>();
    protected final Map<Procedure, Map<ProcParameter, Set<MultiProcParameter>>> proc_multipoc_map = new HashMap<Procedure, Map<ProcParameter, Set<MultiProcParameter>>>();

    private final Set<Table> ignore_tables = new HashSet<Table>();
    private final Set<Procedure> ignore_procs = new HashSet<Procedure>();

    /**
     * @param designer
     * @param info
     */
    public LNSPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);
        this.costmodel = info.getCostModel();
        assert (this.costmodel != null) : "CostModel is null!";
        this.mappings = new ParameterMappingsSet();
        assert (info.getMappingsFile() != null) : "The parameter mappings file path was not set";
    }

    /**
     * Initialize the LNS partitioner. Creates all of the internal data
     * structures that we need for performing the various relaxations and local
     * searches
     * 
     * @param hints
     * @throws Exception
     */
    protected void init(DesignerHints hints) throws Exception {
        assert (hints != null);
        this.init_called = true;
        AccessGraph first = this.generateAccessGraph();
        this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(info.catalogContext.database, first);

        // Set the limits initially from the hints file
        if (hints.limit_back_tracks != null)
            this.last_backtrack_limit = new Double(hints.limit_back_tracks);
        if (hints.limit_local_time != null)
            this.last_localtime_limit = new Double(hints.limit_local_time);
        if (this.last_entropy_weight == null)
            this.last_entropy_weight = hints.weight_costmodel_skew;

        // HACK: Reload the correlations file so that we can get the proper
        // catalog objects
        this.mappings.load(info.getMappingsFile(), info.catalogContext.database);

        // this.agraph.setVertexVerbose(true);
        // GraphvizExport<DesignerVertex, DesignerEdge> gv = new
        // GraphvizExport<DesignerVertex, DesignerEdge>(this.agraph);
        // gv.setCollapseEdges(true);
        // System.err.println("ORIG GRAPH:" + gv.writeToTempFile());
        //
        // gv = new GraphvizExport<DesignerVertex,
        // DesignerEdge>(this.single_agraph);
        // gv.setCollapseEdges(true);
        // System.err.println("SINGLE GRAPH:" + gv.writeToTempFile());

        // Gather all the information we need about each table
        for (Table catalog_tbl : PartitionerUtil.generateTableOrder(info, this.agraph, hints)) {

            // Ignore this table if it's not used in the AcessGraph
            DesignerVertex v = null;
            try {
                v = this.agraph.getVertex(catalog_tbl);
            } catch (IllegalArgumentException ex) {
                // IGNORE
            }
            if (v == null) {
                LOG.warn(String.format("Ignoring %s - No references in workload AccessGraph", catalog_tbl));
                this.ignore_tables.add(catalog_tbl);
                continue;
            }

            // Potential Partitioning Attributes
            Collection<Column> columns = PartitionerUtil.generateColumnOrder(info, this.agraph, catalog_tbl, hints, false, true);
            assert (!columns.isEmpty()) : "No potential partitioning columns selected for " + catalog_tbl;
            this.orig_table_attributes.put(catalog_tbl, (ListOrderedSet<Column>) CollectionUtil.addAll(new ListOrderedSet<Column>(), columns));

            // Table Size (when the table is and is not replicated)
            TableStatistics ts = info.stats.getTableStatistics(catalog_tbl);
            this.table_nonreplicated_size.put(catalog_tbl, Math.round(ts.tuple_size_total / (double) this.num_partitions));
            this.table_replicated_size.put(catalog_tbl, ts.tuple_size_total);

            if (trace.val)
                LOG.trace(catalog_tbl.getName() + ": " + columns);
        } // FOR

        // We also need to know some things about the Procedures and their
        // ProcParameters
        Histogram<String> workloadHistogram = info.workload.getProcedureHistogram();
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            // Skip if we're explicitly force to ignore this guy
            if (PartitionerUtil.shouldIgnoreProcedure(hints, catalog_proc)) {
                if (debug.val) LOG.warn(String.format("Ignoring %s - Set to be ignored by the DesignerHints.", catalog_proc));
                this.ignore_procs.add(catalog_proc);
                continue;
            }
            // Or if there are not transactions in the sample workload
            else if (workloadHistogram.get(CatalogKey.createKey(catalog_proc), 0) == 0) {
                if (debug.val) LOG.warn(String.format("Ignoring %s - No transaction records in sample workload.", catalog_proc));
                this.ignore_procs.add(catalog_proc);
                continue;
            }

            Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_proc);
            if (columns.isEmpty()) {
                if (debug.val) LOG.warn(String.format("Ignoring %s - Does not reference any columns in its queries.", catalog_proc));
                this.ignore_procs.add(catalog_proc);
                continue;
            }
            this.proc_columns.put(catalog_proc, columns);

            // Aha! Use the Procedure-specific AccessGraph to build our
            // Histogram! Where's your god now??
            AccessGraph proc_agraph = this.designer.getAccessGraph(catalog_proc);
            assert (proc_agraph != null);
            // if (catalog_proc.getName().equals("GetAccessData")) {
            // GraphVisualizationPanel.createFrame(proc_agraph).setVisible(true);
            // }
            this.proc_column_histogram.put(catalog_proc, PartitionerUtil.generateProcedureColumnAccessHistogram(info, hints, proc_agraph, catalog_proc));

            // Gather the list ProcParameters we should even consider for
            // partitioning each procedure
            // TODO: For now we'll just put all the parameters in there plus the
            // null one
            HashMap<ProcParameter, Set<MultiProcParameter>> multiparams = new HashMap<ProcParameter, Set<MultiProcParameter>>();
            if (hints.enable_multi_partitioning) {
                multiparams.putAll(PartitionerUtil.generateMultiProcParameters(info, hints, catalog_proc));
                if (trace.val)
                    LOG.trace(catalog_proc + " MultiProcParameters:\n" + multiparams);
            }
            this.proc_multipoc_map.put(catalog_proc, multiparams);

            ListOrderedSet<ProcParameter> params = new ListOrderedSet<ProcParameter>();
            CollectionUtil.addAll(params, CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index"));
            if (hints.enable_array_procparameter_candidates == false) {
                params.removeAll(CatalogUtil.getArrayProcParameters(catalog_proc));
            }
            params.add(NullProcParameter.singleton(catalog_proc));
            this.orig_proc_attributes.put(catalog_proc, params);

            // Add multi-column partitioning parameters for each table
            if (hints.enable_multi_partitioning) {
                Map<Table, Collection<MultiColumn>> multicolumns = PartitionerUtil.generateMultiColumns(info, hints, catalog_proc);
                for (Entry<Table, Collection<MultiColumn>> e : multicolumns.entrySet()) {
                    if (this.ignore_tables.contains(e.getKey()))
                        continue;
                    if (trace.val)
                        LOG.trace(e.getKey().getName() + " MultiColumns:\n" + multicolumns);
                    ListOrderedSet<Column> cols = this.orig_table_attributes.get(e.getKey());
                    if (cols == null) {
                        cols = new ListOrderedSet<Column>();
                        this.orig_table_attributes.put(e.getKey(), cols);
                    }
                    cols.addAll(e.getValue());
                } // FOR
            }
        } // FOR
          // Go back through the table and create the sets of Procedures that
          // touch Columns
        for (Entry<Table, ListOrderedSet<Column>> e : this.orig_table_attributes.entrySet()) {
            Table catalog_tbl = e.getKey();

            // We need to know which Procedures reference each table so that we
            // can selectively choose
            // a new ProcParameter for each swap. Make sure we only include
            // those Procedures that
            // we can make a decision about partitioning
            this.table_procedures.put(catalog_tbl, CatalogUtil.getReferencingProcedures(catalog_tbl));
            this.table_procedures.get(catalog_tbl).retainAll(this.orig_proc_attributes.keySet());

            // We actually can be a bit more fine-grained in our costmodel cache
            // invalidation if we know
            // which columns are actually referenced in each of the procedures
            for (Column catalog_col : this.orig_table_attributes.get(catalog_tbl)) {
                Collection<Procedure> procedures = CatalogUtil.getReferencingProcedures(catalog_col);
                procedures.retainAll(this.orig_proc_attributes.keySet());
                this.column_procedures.put(catalog_col, procedures);
            } // FOR

            // And also pre-compute the intersection of the procedure sets for
            // each Column pair
            for (Column catalog_col0 : this.orig_table_attributes.get(catalog_tbl)) {
                Map<Column, Set<Procedure>> intersections = new HashMap<Column, Set<Procedure>>();
                Collection<Procedure> procs0 = this.column_procedures.get(catalog_col0);

                for (Column catalog_col1 : this.orig_table_attributes.get(catalog_tbl)) {
                    if (catalog_col0.equals(catalog_col1))
                        continue;
                    Collection<Procedure> procs1 = this.column_procedures.get(catalog_col1);
                    intersections.put(catalog_col1, new HashSet<Procedure>(procs0));
                    intersections.get(catalog_col1).retainAll(procs1);
                } // FOR
                this.columnswap_procedures.put(catalog_col0, intersections);
            } // FOR
        } // FOR

    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.designer.partitioners.AbstractPartitioner#generate(edu.brown
     * .designer.DesignerHints)
     */
    @Override
    public PartitionPlan generate(DesignerHints hints) throws Exception {

        // Reload the checkpoint file so that we can continue this dirty mess!
        if (hints.enable_checkpoints) {
            if (this.checkpoint != null && this.checkpoint.exists()) {
                this.load(this.checkpoint, info.catalogContext.database);
                LOG.info("Loaded checkpoint from '" + this.checkpoint + "'");

                // Important! We need to update the hints based on what's in the
                // checkpoint
                // We really need to link the hints and the checkpoints
                // better...
                hints.weight_costmodel_skew = this.last_entropy_weight;

            } else if (this.checkpoint != null) {
                LOG.info("Not loading non-existent checkpoint file: " + this.checkpoint);
            }
            if (this.start_time == null && this.last_checkpoint == null) {
                this.start_time = hints.startGlobalSearchTimer();
            } else {
                LOG.info("Setting checkpoint offset times");
                assert (hints != null);
                assert (this.start_time != null) : "Start Time is null";
                assert (this.last_checkpoint != null) : "Last CheckPoint is null";
                hints.offsetCheckpointTime(this.start_time, this.last_checkpoint);
            }
            assert (this.start_time != null);
        } else {
            LOG.info("Checkpoints disabled");
        }

        // Initialize a bunch of stuff we need
        this.init(hints);

        hints.startGlobalSearchTimer();
        LOG.info("Starting Large-Neighborhood Search\n" + this.debugHeader(hints));

        // Tell the costmodel about the hints.
        this.costmodel.applyDesignerHints(hints);

        // First we need to hit up the MostPopularPartitioner in order to get an
        // initial solution
        if (this.initial_solution == null)
            this.calculateInitialSolution(hints);
        assert (this.initial_solution != null);

        if (this.best_solution == null) {
            LOG.info("Initializing current best solution to be initial solution");
            this.best_solution = new PartitionPlan(this.initial_solution);
            this.best_memory = this.initial_memory;
            this.best_cost = this.initial_cost;
        } else {
            LOG.info("Checking whether previously calculated best solution has the same cost");

            // Sanity Check!
            this.best_solution.apply(info.catalogContext.database);
            this.costmodel.clear(true);
            double cost = this.costmodel.estimateWorkloadCost(info.catalogContext, info.workload);

            boolean valid = MathUtil.equals(this.best_cost, cost, 0.01);
            LOG.info(String.format("Checkpoint Cost [" + DEBUG_COST_FORMAT + "] <-> Reloaded Cost [" + DEBUG_COST_FORMAT + "] ==> %s", cost, this.best_cost, (valid ? "VALID" : "FAIL")));
            // assert(valid) : cost + " == " + this.best_cost + "\n" +
            // PartitionPlan.createFromCatalog(info.catalogContext.database, hints);
            this.best_cost = cost;
        }
        assert (this.best_solution != null);

        // Relax and begin the new search!
        this.costmodel.clear(true);
        this.costmodel.setDebuggingEnabled(true);
        if (this.restart_ctr == null)
            this.restart_ctr = 0;

        final ListOrderedSet<Table> table_attributes = new ListOrderedSet<Table>();
        final ListOrderedSet<Procedure> proc_attributes = new ListOrderedSet<Procedure>();

        while (true) {
            // Found search target
            if (this.last_halt_reason == HaltReason.FOUND_TARGET) {
                LOG.info("Found target PartitionPlan. Halting");
                break;
            }
            // Time Limit
            else if (hints.limit_total_time != null && this.total_search_time.getTotalThinkTimeSeconds() > hints.limit_total_time) {
                LOG.info("Time limit reached: " + hints.limit_total_time + " seconds");
                break;
            }

            this.total_search_time.start();
            // IMPORTANT: Make sure that we are always start comparing swaps
            // using the solution
            // at the beginning of a restart (or the start of the search). We do
            // *not* want to
            // compare swaps using the global best cost
            if (!this.relaxCurrentSolution(hints, this.restart_ctr++, table_attributes, proc_attributes)) {
                LOG.debug("Halting LNS!");
                break;
            }

            // Local Search!
            this.localSearch(hints, table_attributes.asList(), proc_attributes.asList());

            // Sanity Check!
            if (this.restart_ctr % 3 == 0) {
                LOG.info("Running sanity check on best solution...");
                this.costmodel.clear(true);
                double cost2 = this.costmodel.estimateWorkloadCost(info.catalogContext, info.workload);
                LOG.info(String.format("Before[" + DEBUG_COST_FORMAT + "] <=> After[" + DEBUG_COST_FORMAT + "]", this.best_cost, cost2));
                assert (MathUtil.equals(this.best_cost, cost2, 2, 0.2)) : cost2 + " == " + this.best_cost + "\n" + PartitionPlan.createFromCatalog(info.catalogContext.database) + "\n"
                        + this.costmodel.getLastDebugMessage();
            }

            // Save checkpoint
            this.last_checkpoint = new TimestampType();
            if (this.checkpoint != null) {
                this.save(this.checkpoint);
                LOG.info("Saved Round #" + this.restart_ctr + " checkpoint to '" + this.checkpoint.getAbsolutePath() + "'");
            }

            this.total_search_time.stop();
        } // WHILE
        if (this.total_search_time.isStarted())
            this.total_search_time.stop();

        LOG.info("Final Solution Cost: " + String.format(DEBUG_COST_FORMAT, this.best_cost));
        LOG.info("Final Solution Memory: " + String.format(DEBUG_COST_FORMAT, this.best_memory));
        LOG.info("Final Search Time: " + String.format("%.2f sec", this.total_search_time.getTotalThinkTimeSeconds()));
        // if (this.initial_cost > this.best_cost)
        // LOG.warn("BAD MOJO! Initial Cost = " + this.initial_cost + " > " +
        // this.best_cost);
        // assert(this.best_cost <= this.initial_cost);
        this.setProcedureSinglePartitionFlags(this.best_solution, hints);
        return (this.best_solution);
    }

    /**
     * @param hints
     * @throws Exception
     */
    protected void calculateInitialSolution(final DesignerHints hints) throws Exception {
        LOG.info("Calculating Initial Solution using MostPopularPartitioner");

        this.initial_solution = new MostPopularPartitioner(this.designer, this.info).generate(hints);
        this.initial_solution.apply(this.info.catalogContext.database);

        // Check whether we have enough memory for the initial solution
        if (hints.max_memory_per_partition > 0) {
            long total_size = this.info.getMemoryEstimator().estimate(this.info.catalogContext.database, this.num_partitions);
            this.initial_memory = total_size / (double) hints.max_memory_per_partition;
            if (this.initial_memory > 1.0) {
                LOG.error("Invalid initial solution. Total size = " + StringUtil.formatSize(total_size) + "\n" + this.initial_solution);
            }
            assert (this.initial_memory <= 1.0) : "Not enough memory: " + this.initial_memory; // Never
                                                                                               // should
                                                                                               // happen!
        } else {
            hints.max_memory_per_partition = 0;
        }

        // Now calculate the cost. We do this after the memory estimation so
        // that we don't waste our time if it won't fit
        this.initial_cost = this.costmodel.estimateWorkloadCost(this.info.catalogContext, this.info.workload);

        // We need to examine whether the solution utilized all of the
        // partitions. If it didn't, then
        // we need to increase the entropy weight in the costmodel. This will
        // help steer us towards
        // a solution that fully utilizes partitions
        if (hints.enable_costmodel_idlepartition_penalty) {
            Set<Integer> untouched_partitions = this.costmodel.getUntouchedPartitions(this.num_partitions);
            if (debug.val)
                LOG.debug("Number of Idle Partitions: " + untouched_partitions.size());
            if (!untouched_partitions.isEmpty()) {
                double entropy_weight = this.costmodel.getEntropyWeight();
                entropy_weight *= this.num_partitions / (double) untouched_partitions.size();

                // Oh god this took forever to track down! If we want to
                // re-adjust the weights, we have to make
                // sure that we do it to the hints so that it is picked up by
                // everyone!
                // 2010-11-27: Actually no! We want to make sure we put it in
                // the local object so that it gets written
                // out when we checkpoint and then load the checkpoint back in
                this.last_entropy_weight = entropy_weight;
                hints.weight_costmodel_skew = this.last_entropy_weight;

                LOG.info("Initial Solution has " + untouched_partitions.size() + " unused partitions. New Entropy Weight: " + entropy_weight);
                this.costmodel.applyDesignerHints(hints);
                this.initial_cost = this.costmodel.estimateWorkloadCost(this.info.catalogContext, this.info.workload);
            }
        }

        if (debug.val) {
            LOG.debug("Initial Solution Cost: " + String.format(DEBUG_COST_FORMAT, this.initial_cost));
            LOG.debug("Initial Solution Memory: " + String.format(DEBUG_COST_FORMAT, this.initial_memory));
            LOG.debug("Initial Solution:\n" + this.initial_solution);
        }
        if (hints.shouldLogSolutionCosts()) {
            double singlep_txns = this.costmodel.getSinglePartitionProcedureHistogram().getSampleCount() / (double) this.costmodel.getProcedureHistogram().getSampleCount();
            hints.logSolutionCost(this.initial_cost, singlep_txns);
        }
    }

    /**
     * @param hints
     * @param restart_ctr
     * @throws Exception
     */
    protected boolean relaxCurrentSolution(final DesignerHints hints, int restart_ctr, Set<Table> table_attributes, Set<Procedure> proc_attributes) throws Exception {
        assert (this.init_called);
        RandomDistribution.DiscreteRNG rand = new RandomDistribution.Flat(this.rng, 0, this.orig_table_attributes.size());
        int num_tables = this.orig_table_attributes.size();

        // If the last search was exhaustive and its relaxation size was the
        // same as the total number of tables,
        // then we know that we're done! This won't happen too often...
        if (this.last_relax_size == num_tables && this.last_halt_reason == HaltReason.EXHAUSTED_SEARCH) {
            LOG.info("Exhaustively search solution space! Nothing more to do!");
            return (false);
        }

        // Apply the best solution to the catalog
        this.best_solution.apply(info.catalogContext.database);

        // TODO: Calculate relaxation weights
        // Map<Table, Double> relax_weights = new HashMap<Table, Double>();

        // Figure out what how far along we are in the search and use that to
        // determine how many to relax
        int relax_min = (int) Math.round(hints.relaxation_factor_min * num_tables);
        int relax_max = (int) Math.round(hints.relaxation_factor_max * num_tables);

        // We should probably try to do something smart here, but for now we can
        // just be random
        // int relax_size = (int)Math.round(RELAXATION_FACTOR_MIN * num_tables)
        // + (restart_ctr / 2);
        double elapsed_ratio = hints.getElapsedGlobalPercent();
        int relax_size = Math.max(hints.relaxation_min_size, (int) Math.max(this.last_relax_size, (int) Math.round(((relax_max - relax_min) * elapsed_ratio) + relax_min)));

        if (relax_size > num_tables)
            relax_size = num_tables;
        // if (relax_size > relax_max) relax_size = relax_max;

        // Check whether we've already examined all combinations of the tables
        // at this relaxation size
        // That means we should automatically increase our size
        if (restart_ctr > 0 && ((relax_size != this.last_relax_size) || (this.relaxed_sets_max != null && this.relaxed_sets_max.intValue() == this.relaxed_sets.size()))) {
            LOG.info(String.format("Already processed all %s relaxation sets of size %d. Increasing relaxation set size", this.relaxed_sets_max, relax_size));
            this.relaxed_sets_max = null;
            relax_size++;
            if (relax_size > num_tables)
                return (false);
        }

        if (this.relaxed_sets_max == null) {
            // n! / k!(n - k)!
            BigInteger nFact = MathUtil.factorial(num_tables);
            BigInteger kFact = MathUtil.factorial(relax_size);
            BigInteger nminuskFact = MathUtil.factorial(num_tables - relax_size);
            this.relaxed_sets_max = nFact.divide(kFact.multiply(nminuskFact));
            this.relaxed_sets.clear();
        }

        // If we exhausted our last search, then we want to make sure we
        // increase our
        // relaxation size...
        // if (this.last_halt_reason == HaltReason.EXHAUSTED_SEARCH &&
        // relax_size < relax_max) relax_size = this.last_relax_size + 1;

        assert (relax_size >= relax_min) : "Invalid Relax Size: " + relax_size;
        // assert(relax_size <= relax_max) : "Invalid Relax Size: " +
        // relax_size;
        assert (relax_size > 0) : "Invalid Relax Size: " + relax_size;
        assert (relax_size <= num_tables) : "Invalid Relax Size: " + relax_size;

        if (LOG.isInfoEnabled()) {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            m.put(" - relaxed_sets", this.relaxed_sets.size());
            m.put(" - relaxed_sets_max", this.relaxed_sets_max);
            m.put(" - last_relax_size", this.last_relax_size);
            m.put(" - last_halt_reason", this.last_halt_reason);
            m.put(" - last_elapsed_time", this.last_elapsed_time);
            m.put(" - last_backtracks", this.last_backtrack_count);
            m.put(" - elapsed_ratio", String.format("%.02f", elapsed_ratio));
            m.put(" - limit_local_time", String.format("%.02f", this.last_localtime_limit));
            m.put(" - limit_back_track", String.format("%.02f", this.last_backtrack_limit));
            m.put(" - best_cost", String.format(DEBUG_COST_FORMAT, this.best_cost));
            m.put(" - best_memory", this.best_memory);
            LOG.info("\n" + StringBoxUtil.box(String.format("LNS RESTART #%03d  [relax_size=%d]\n%s", restart_ctr, relax_size, StringUtil.formatMaps(m)), "+", 125));
        }

        // Select which tables we want to relax on this restart
        // We will keep looking until we find one that we haven't processed
        // before
        Collection<Table> relaxed_tables = new TreeSet<Table>(Collections.reverseOrder());
        while (true) {
            relaxed_tables.clear();
            Collection<Integer> rand_idxs = rand.getRandomIntSet(relax_size);
            if (trace.val)
                LOG.trace("Relaxed Table Identifiers: " + rand_idxs);
            for (int idx : rand_idxs) {
                assert (idx < this.orig_table_attributes.size()) : "Random Index is too large: " + idx + " " + this.orig_table_attributes.keySet();
                Table catalog_tbl = this.orig_table_attributes.get(idx);
                relaxed_tables.add(catalog_tbl);
            } // FOR (table)
            if (this.relaxed_sets.contains(relaxed_tables) == false) {
                break;
            }
        } // WHILE
        assert (relaxed_tables.size() == relax_size) : relax_size + " => " + relaxed_tables;
        LOG.info("Relaxed Tables: " + CatalogUtil.getDisplayNames(relaxed_tables));
        this.relaxed_sets.add(relaxed_tables);

        // Now for each of the relaxed tables, figure out what columns we want
        // to consider for swaps
        table_attributes.clear();
        proc_attributes.clear();
        Collection<Table> nonrelaxed_tables = new ArrayList<Table>(this.orig_table_attributes.asList());
        for (Table catalog_tbl : relaxed_tables) {
            this.costmodel.invalidateCache(catalog_tbl);

            nonrelaxed_tables.remove(catalog_tbl);
            table_attributes.add(catalog_tbl);
            for (Procedure catalog_proc : this.table_procedures.get(catalog_tbl)) {
                proc_attributes.add(catalog_proc);
            } // FOR

        } // FOR

        // Now estimate the size of a partition for the non-relaxed tables
        double nonrelaxed_memory = this.info.getMemoryEstimator().estimate(info.catalogContext.database, this.num_partitions, nonrelaxed_tables) / (double) hints.max_memory_per_partition;
        assert (nonrelaxed_memory >= 0.0) : "Invalid memory: " + nonrelaxed_memory;
        assert (nonrelaxed_memory < 1.0) : "Invalid memory: " + nonrelaxed_memory;

        // THOUGHTS:
        // (0) We think we actually need to relax very first time too...
        // (1) Make it less likely that we are going to relax a small/read-only
        // table
        //

        this.last_relax_size = relax_size;
        return (true);
    }

    /**
     * Local Search
     * 
     * @param hints
     * @param table_attributes
     * @param proc_attributes
     * @throws Exception
     */
    protected void localSearch(final DesignerHints hints, List<Table> table_attributes, List<Procedure> proc_attributes) throws Exception {
        // -------------------------------
        // Apply relaxation and invalidate caches!
        // -------------------------------
        for (Table catalog_tbl : table_attributes) {
            // catalog_tbl.setPartitioncolumn(null);
            this.costmodel.invalidateCache(catalog_tbl);
        } // FOR
        for (Procedure catalog_proc : proc_attributes) {
            catalog_proc.setPartitionparameter(NullProcParameter.PARAM_IDX);
            this.costmodel.invalidateCache(catalog_proc);
        } // FOR

        // Sanity Check: Make sure the non-relaxed tables come back with the
        // same partitioning attribute
        Map<CatalogType, CatalogType> orig_solution = new HashMap<CatalogType, CatalogType>();
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            if (!table_attributes.contains(catalog_tbl))
                orig_solution.put(catalog_tbl, catalog_tbl.getPartitioncolumn());
        }
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            if (!proc_attributes.contains(catalog_proc)) {
                ProcParameter catalog_param = catalog_proc.getParameters().get(catalog_proc.getPartitionparameter());
                orig_solution.put(catalog_proc, catalog_param);
            }
        }

        // -------------------------------
        // Calculate the number of backtracks and the local search time
        // we want to allow in this round.
        // -------------------------------
        if (hints.enable_local_search_increase) {
            if (this.last_halt_reason == HaltReason.BACKTRACK_LIMIT && this.last_backtrack_limit != null) {
                // Give them more backtracks
                this.last_backtrack_limit = this.last_backtrack_limit * hints.back_tracks_multiplier;
                LOG.info(String.format("Increasing BackTrack limit from %d to %.02f", hints.limit_back_tracks, this.last_backtrack_limit));
                hints.limit_back_tracks = (int) Math.round(this.last_backtrack_limit);
            } else if (this.last_halt_reason == HaltReason.LOCAL_TIME_LIMIT && this.last_localtime_limit != null) {
                // Give them more time
                this.last_localtime_limit = this.last_localtime_limit * hints.local_time_multiplier;
                LOG.info(String.format("Increasing LocalTime limit from %d to %.02f", hints.limit_local_time, this.last_localtime_limit));
                hints.limit_local_time = (int) Math.round(this.last_localtime_limit);
            }
        }

        // -------------------------------
        // GO GO LOCAL SEARCH!!
        // -------------------------------
        Pair<PartitionPlan, BranchAndBoundPartitioner.StateVertex> pair = this.executeLocalSearch(hints, this.agraph, table_attributes, proc_attributes);
        assert (pair != null);
        PartitionPlan result = pair.getFirst();
        BranchAndBoundPartitioner.StateVertex state = pair.getSecond();

        // -------------------------------
        // Validation
        // -------------------------------
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            if (catalog_tbl.getSystable() == false && orig_solution.containsKey(catalog_tbl)) {
                assert (orig_solution.get(catalog_tbl).equals(catalog_tbl.getPartitioncolumn())) : String.format("%s got changed: %s => %s", catalog_tbl, orig_solution.get(catalog_tbl),
                        catalog_tbl.getPartitioncolumn());
            }
        } // FOR
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            if (orig_solution.containsKey(catalog_proc)) {
                ProcParameter catalog_param = catalog_proc.getParameters().get(catalog_proc.getPartitionparameter());
                if (catalog_param == null) {
                    assert (orig_solution.get(catalog_proc) == null) : catalog_proc + " got changed: " + orig_solution.get(catalog_proc) + " => " + catalog_param + "\n" + result;
                } else {
                    assert (catalog_param.equals(orig_solution.get(catalog_proc))) : catalog_proc + " got changed: " + orig_solution.get(catalog_proc) + " => " + catalog_param + "\n" + result;
                }
            }
        } // FOR

        // -------------------------------
        // Comparison with current best solution
        // -------------------------------
        if (state.getCost() < this.best_cost) {
            LOG.info("New Best Solution Found from Local Search!");
            this.best_solution = result;
            this.best_cost = state.getCost();
            this.best_memory = state.getMemory() / (double) hints.max_memory_per_partition;
            LOG.info("Best Solution Cost: " + String.format(DEBUG_COST_FORMAT, this.best_cost));
            LOG.info("Best Solution Memory: " + String.format(DEBUG_COST_FORMAT, this.best_memory));
            LOG.info("Best Solution:\n" + this.best_solution);
        }
        this.best_solution.apply(info.catalogContext.database);
        return;
    }

    /**
     * @param hints
     * @param table_attributes
     * @param key_attributes
     * @return
     * @throws Exception
     */
    protected Pair<PartitionPlan, BranchAndBoundPartitioner.StateVertex> executeLocalSearch(final DesignerHints hints, final AccessGraph agraph, final List<Table> table_visit_order,
            final List<Procedure> proc_visit_order) throws Exception {
        assert (agraph != null) : "Missing AccessGraph!";

        BranchAndBoundPartitioner local_search = new BranchAndBoundPartitioner(this.designer, this.info, agraph, table_visit_order, proc_visit_order);
        local_search.setUpperBounds(hints, this.best_solution, this.best_cost, (long) (this.best_memory * hints.max_memory_per_partition));
        // local_search.setTraversalAttributes(key_attributes,
        // table_attributes.size());

        long start = System.currentTimeMillis();
        PartitionPlan result = local_search.generate(hints);
        this.last_elapsed_time = Math.round((System.currentTimeMillis() - start) / 1000);
        this.last_halt_reason = local_search.halt_reason;
        this.last_backtrack_count = local_search.getLastBackTrackCount();

        return (Pair.of(result, local_search.getBestVertex()));
    }

    /**
     * @param hints
     * @return
     * @throws Exception
     */
    protected Pair<Double, Double> recalculateCost(final DesignerHints hints) throws Exception {
        long memory = info.getMemoryEstimator().estimate(info.catalogContext.database, this.num_partitions, this.orig_table_attributes.keySet());
        double memory_ratio = memory / (double) hints.max_memory_per_partition;
        double cost = this.costmodel.estimateWorkloadCost(info.catalogContext, info.workload);
        return (Pair.of(cost, memory_ratio));
    }

    /**
     * @param hints
     * @param procs
     * @throws Exception
     */
    protected void recalculateProcParameters(final DesignerHints hints, Iterable<Procedure> procs) throws Exception {
        Map<Procedure, Integer> new_procparams = new HashMap<Procedure, Integer>();
        for (Procedure catalog_proc : procs) {
            if (catalog_proc.getSystemproc() || catalog_proc.getParameters().size() == 0)
                continue;
            // If we get back a null ProcParameter, then just the Procedure
            // alone
            ProcParameter catalog_proc_param = this.findBestProcParameter(hints, catalog_proc);
            if (catalog_proc_param != null)
                new_procparams.put(catalog_proc, catalog_proc_param.getIndex());
        } // FOR
        this.applyProcParameterSwap(hints, new_procparams);
    }

    /**
     * @param hints
     * @param catalog_proc
     * @return
     * @throws Exception
     */
    protected ProcParameter findBestProcParameter(final DesignerHints hints, final Procedure catalog_proc) throws Exception {
        assert (!catalog_proc.getSystemproc());
        assert (catalog_proc.getParameters().size() > 0);

        // Find all the ProcParameter correlations that map to the target column
        // in the Procedure
        // ParameterCorrelations correlations = info.getCorrelations();
        // assert(correlations != null);

        ProcParameter default_param = catalog_proc.getParameters().get(0);
        ObjectHistogram<Column> col_access_histogram = this.proc_column_histogram.get(catalog_proc);
        if (col_access_histogram == null) {
            if (debug.val)
                LOG.warn("No column access histogram for " + catalog_proc + ". Setting to default");
            return (default_param);
        }
        if (debug.val)
            LOG.debug(catalog_proc + " Column Histogram:\n" + col_access_histogram);

        // Loop through each Table and check whether its partitioning column is
        // referenced in this procedure
        Map<ProcParameter, List<Double>> param_weights = new HashMap<ProcParameter, List<Double>>();
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            if (catalog_tbl.getIsreplicated())
                continue;
            Column catalog_col = catalog_tbl.getPartitioncolumn();
            if (!col_access_histogram.contains(catalog_col))
                continue;
            long col_access_cnt = col_access_histogram.get(catalog_col);

            if (debug.val)
                LOG.debug(CatalogUtil.getDisplayName(catalog_col));
            // Now loop through the ProcParameters and figure out which ones are
            // correlated to the Column
            for (ProcParameter catalog_proc_param : catalog_proc.getParameters()) {
                // Skip if this is an array
                if (hints.enable_array_procparameter_candidates == false && catalog_proc_param.getIsarray())
                    continue;

                if (!param_weights.containsKey(catalog_proc_param)) {
                    param_weights.put(catalog_proc_param, new ArrayList<Double>());
                }
                List<Double> weights_list = param_weights.get(catalog_proc_param);
                Collection<ParameterMapping> pms = mappings.get(catalog_proc_param, catalog_col);
                if (pms != null) {
                    for (ParameterMapping c : pms) {
                        weights_list.add(c.getCoefficient() * col_access_cnt);
                    } // FOR
                }
                if (debug.val)
                    LOG.debug("  " + catalog_proc_param + ": " + weights_list);
            } // FOR
            if (debug.val)
                LOG.debug("");
        } // FOR (Table)

        final Map<ProcParameter, Double> final_param_weights = new HashMap<ProcParameter, Double>();
        for (Entry<ProcParameter, List<Double>> e : param_weights.entrySet()) {
            // The weights for each ProcParameter will be the geometric mean of
            // the correlation coefficients
            if (!e.getValue().isEmpty()) {
                double weights[] = new double[e.getValue().size()];
                for (int i = 0; i < weights.length; i++)
                    weights[i] = e.getValue().get(i);
                final_param_weights.put(e.getKey(), MathUtil.geometricMean(weights, MathUtil.GEOMETRIC_MEAN_ZERO));
            }
        } // FOR
        if (final_param_weights.isEmpty()) {
            if (debug.val)
                LOG.warn("Failed to find any ProcParameters for " + catalog_proc.getName() + " that map to partition columns");
            return (default_param);
        }
        Map<ProcParameter, Double> sorted = CollectionUtil.sortByValues(final_param_weights, true);
        assert (sorted != null);
        ProcParameter best_param = CollectionUtil.first(sorted.keySet());
        if (debug.val)
            LOG.debug("Best Param: " + best_param + " " + sorted);
        return (best_param);
    }

    protected void applyColumnSwap(DesignerHints hints, Table catalog_tbl, Column new_column) throws Exception {
        assert (catalog_tbl != null);
        assert (new_column != null);
        catalog_tbl.setIsreplicated(new_column instanceof ReplicatedColumn);
        catalog_tbl.setPartitioncolumn(new_column);
        if (hints.enable_costmodel_caching) {
            this.costmodel.invalidateCache(catalog_tbl);
        } else {
            this.costmodel.clear();
        }
    }

    protected void applyProcParameterSwap(DesignerHints hints, Map<Procedure, Integer> param_map) {
        for (Entry<Procedure, Integer> e : param_map.entrySet()) {
            int orig_param = e.getKey().getPartitionparameter();
            e.getKey().setPartitionparameter(e.getValue());
            if (orig_param != e.getValue()) {
                if (hints.enable_costmodel_caching) {
                    this.costmodel.invalidateCache(e.getKey());
                } else {
                    this.costmodel.clear();
                }
            }
        } // FOR
    }

    public String dumpCatalogState(Database catalog_db) {
        return (PartitionPlan.createFromCatalog(catalog_db).toString());
    }

    @SuppressWarnings("unchecked")
    public String debug() {
        Map<String, Object> maps[] = new Map[this.orig_table_attributes.size()];
        int i = 0;
        for (Table catalog_tbl : this.orig_table_attributes.keySet()) {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            m.put(catalog_tbl.getName(), "");
            m.put("Non-Replicated Size", this.table_nonreplicated_size.get(catalog_tbl));
            m.put("Replicated Size", this.table_replicated_size.get(catalog_tbl));
            m.put("Touching Procedures", CatalogUtil.debug(this.table_procedures.get(catalog_tbl)));
            m.put("Original Columns", CatalogUtil.debug(this.orig_table_attributes.get(catalog_tbl)));

            for (Column catalog_col : catalog_tbl.getColumns()) {
                Map<String, Object> inner = new ListOrderedMap<String, Object>();
                boolean has_col_procs = this.column_procedures.containsKey(catalog_col);
                boolean has_col_swaps = this.columnswap_procedures.containsKey(catalog_col);
                if (has_col_procs == false && has_col_swaps == false)
                    continue;

                // PROCEDURES
                String procs = "<NONE>";
                if (has_col_procs) {
                    procs = CatalogUtil.debug(this.column_procedures.get(catalog_col));
                }
                inner.put("Procedures", procs);

                // COLUMN SWAPS
                String swaps = "<NONE>";
                if (has_col_swaps) {
                    swaps = "";
                    for (Entry<Column, Set<Procedure>> e : this.columnswap_procedures.get(catalog_col).entrySet()) {
                        if (e.getValue().isEmpty() == false) {
                            if (swaps.isEmpty() == false)
                                swaps += "\n";
                            swaps += String.format("%s => %s", e.getKey().getName(), CatalogUtil.debug(e.getValue()));
                        }
                    } // FOR
                }
                inner.put("ColumnSwaps", swaps);

                m.put("+ " + catalog_col.fullName(), StringUtil.formatMaps(inner));
            } // FOR
            maps[i++] = m;
        }
        // sb.append(StringUtil.repeat("=", 100)).append("\n")
        // .append("TABLE INFORMATION\n")
        // .append(StringUtil.repeat("=", 100)).append("\n");
        // for (int i = 0, cnt = this.orig_table_attributes.size(); i < cnt;
        // i++) {
        // Table catalog_tbl = this.orig_table_attributes.get(i);
        // ListOrderedSet<Column> cols =
        // this.orig_table_attributes.get(catalog_tbl);
        //
        // sb.append(String.format("[%02d] %s\n", i, catalog_tbl.getName()));
        // sb.append("   Column Candidates [").append(cols.size()).append("]\n");
        // for (int ii = 0; ii < cols.size(); ii++) {
        // sb.append("      ")
        // .append(String.format("[%02d] %s\n", ii, cols.get(ii).getName()));
        // } // FOR (cols)
        // } // FOR (tables)
        // sb.append("\n");
        //
        // sb.append(StringUtil.repeat("=", 100)).append("\n")
        // .append("PROCEDURE INFORMATION\n")
        // .append(StringUtil.repeat("=", 100)).append("\n");
        // for (int i = 0, cnt = this.orig_proc_attributes.size(); i < cnt; i++)
        // {
        // Procedure catalog_proc = this.orig_proc_attributes.get(i);
        // ListOrderedSet<ProcParameter> params =
        // this.orig_proc_attributes.get(catalog_proc);
        //
        // sb.append(String.format("[%02d] %s\n", i, catalog_proc.getName()));
        // sb.append("   Parameter Candidates [").append(params.size()).append("]\n");
        // for (int ii = 0; ii < params.size(); ii++) {
        // sb.append("      ")
        // .append(String.format("[%02d] %s\n", ii, params.get(ii).getName()));
        // } // FOR (params)
        // } // FOR (procedres)
        // sb.append("\n");

        return (StringUtil.formatMaps(maps));
    }

    @SuppressWarnings("unchecked")
    public String debugHeader(DesignerHints hints) throws Exception {
        Map<String, Object> m[] = new Map[3];
        m[0] = new ListOrderedMap<String, Object>();
        m[0].put("Start Time", hints.getStartTime());
        m[0].put("Total Time", String.format("%.2f sec", this.total_search_time.getTotalThinkTimeSeconds()));
        m[0].put("Remaining Time", (hints.limit_total_time != null ? (hints.limit_total_time - this.total_search_time.getTotalThinkTimeSeconds()) + " sec" : "-"));
        m[0].put("Cost Model", info.getCostModel().getClass().getSimpleName());
        m[0].put("# of Transactions", info.workload.getTransactionCount());
        m[0].put("# of Partitions", info.catalogContext.numberOfPartitions);
        m[0].put("# of Intervals", info.getArgs().num_intervals);
        m[0].put("# of Restarts", (this.restart_ctr != null ? this.restart_ctr : "-"));
        m[0].put("Database Total Size", StringUtil.formatSize(info.getMemoryEstimator().estimateTotalSize(info.catalogContext.database)));
        m[0].put("Cluster Total Size", StringUtil.formatSize(info.getNumPartitions() * hints.max_memory_per_partition));

        m[1] = new ListOrderedMap<String, Object>();
        String fields[] = { "enable_checkpoints", "greedy_search", "relaxation_factor_min", "relaxation_factor_max", "relaxation_factor_min", "back_tracks_multiplier", "local_time_multiplier",
                "limit_total_time" };
        for (String f_name : fields) {
            Field f = DesignerHints.class.getField(f_name);
            assert (f != null);
            m[1].put("hints." + f_name, f.get(hints));
        } // FOR

        m[2] = new ListOrderedMap<String, Object>();
        m[2].put(info.workload.getProcedureHistogram().toString(), null);

        return (StringBoxUtil.box(StringUtil.formatMaps(m), "+"));
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, LNSPartitioner.class, JSONUtil.getSerializableFields(this.getClass()));
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, LNSPartitioner.class, true, JSONUtil.getSerializableFields(this.getClass()));
    }

}
