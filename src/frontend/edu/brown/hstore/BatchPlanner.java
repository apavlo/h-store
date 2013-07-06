/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.exceptions.MispredictionException;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.profilers.BatchPlannerProfiler;
import edu.brown.profilers.ProfileMeasurementUtil;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.uci.ics.jung.graph.DirectedSparseMultigraph;

/**
 * @author pavlo
 */
public class BatchPlanner {
    private static final Logger LOG = Logger.getLogger(BatchPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // STATIC DATA MEMBERS
    // ----------------------------------------------------------------------------

    private static final int FIRST_DEPENDENCY_ID = 1;

    /**
     * If the unique dependency ids option is enabled, all input/output
     * DependencyIds for WorkFragments will be globally unique.
     * @see HStoreConf.SiteConf.planner_unique_dependency_ids
     */
    private static final AtomicInteger NEXT_DEPENDENCY_ID = new AtomicInteger(FIRST_DEPENDENCY_ID);

    /**
     * Cached set of PlanFragment -> Set<PartitionIds>
     */
    private static Map<Statement, Map<PlanFragment, PartitionSet>> CACHED_FRAGMENT_PARTITION_MAPS[];

    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------

    private final HStoreConf hstore_conf;
    protected final CatalogContext catalogContext;
    protected final Procedure catalog_proc;
    protected final Statement catalog_stmts[];
    private final boolean stmt_is_readonly[];
    private final boolean stmt_is_replicatedonly[];
    private final List<PlanFragment> sorted_singlep_fragments[];
    private final List<PlanFragment> sorted_multip_fragments[];
    private final int batchSize;
    private final int nonReplicatedStmtCount;
    private final PartitionEstimator p_estimator;
    private BatchPlan plan;
    private final Map<Integer, PlanGraph> plan_graphs = new HashMap<Integer, PlanGraph>();
    private final Map<Integer, WorkFragment.Builder> round_builders = new HashMap<Integer, WorkFragment.Builder>();

    private final boolean enable_unique_ids;
    private final boolean force_singlePartition;
    private boolean prefetch = false;

    private final Map<Integer, Set<PlanVertex>> output_dependency_xref = new HashMap<Integer, Set<PlanVertex>>();
    private final List<Integer> output_dependency_xref_clear = new ArrayList<Integer>();
    private final List<PlanVertex> sorted_vertices = new ArrayList<PlanVertex>();

    // FAST SINGLE-PARTITION LOOKUP CACHE
    private final boolean cache_isSinglePartition[];
    private final int cache_fastLookups[][];
    private final BatchPlan cache_singlePartitionPlans[];
    private Map<Statement, Map<PlanFragment, PartitionSet>> cache_singlePartitionFragmentPartitions;

    // PROFILING
    private BatchPlannerProfiler profiler;

    // ----------------------------------------------------------------------------
    // INTERNAL PLAN GRAPH ELEMENTS
    // ----------------------------------------------------------------------------

    protected static class PlanVertex { // extends AbstractVertex {
        final PlanFragment catalog_frag;
        final int frag_id;
        final int stmt_index;
        final int round;
        final int input_dependency_id;
        final int output_dependency_id;
        final int hash_code;
        final boolean read_only;

        public PlanVertex(PlanFragment catalog_frag, int stmt_index, int round, int input_dependency_id,
                          int output_dependency_id, boolean is_local) {
            // super(catalog_frag);
            this.catalog_frag = catalog_frag;
            this.frag_id = catalog_frag.getId();
            this.stmt_index = stmt_index;
            this.round = round;
            this.input_dependency_id = input_dependency_id;
            this.output_dependency_id = output_dependency_id;
            this.read_only = catalog_frag.getReadonly();

            this.hash_code = this.frag_id | this.round << 20 | this.stmt_index << 26;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PlanVertex))
                return (false);
            return (this.hash_code == ((PlanVertex) obj).hash_code);
        }

        @Override
        public int hashCode() {
            return (this.hash_code);
        }

        @Override
        public String toString() {
            return String.format("<FragId=%02d, StmtIndex=%02d, Round=%02d, Input=%02d, Output=%02d>", this.frag_id,
                    this.stmt_index, this.round, this.input_dependency_id, this.output_dependency_id);
        }
    } // END CLASS

    protected static class PlanGraph extends DirectedSparseMultigraph<PlanVertex, Integer> {
        private static final long serialVersionUID = 1L;

        /**
         * The number of dispatch rounds that we have in this plan
         */
        private int num_rounds = 0;
        
        private PlanVertex sorted_vertices[];

        /**
         * Single-Partition
         */
        private long fragmentIds[];
        private int input_ids[];
        private int output_ids[];

        public PlanGraph() {
            // super(catalog_db);
        }
    } // END CLASS

    // ----------------------------------------------------------------------------
    // BATCH PLAN
    // ----------------------------------------------------------------------------

    /**
     * BatchPlan
     */
    public class BatchPlan {
        // ----------------------------------------------------------------------------
        // INVOCATION DATA MEMBERS
        // ----------------------------------------------------------------------------
        private boolean cached = false;

        private int base_partition = HStoreConstants.NULL_PARTITION_ID;
        private PlanGraph graph;
        private MispredictionException mispredict;

        /** Temporary buffer space for sorting the PlanFragments per Statement */
        private final List<PlanFragment> frag_list[];

        /** Round# -> Map{PartitionId, Set{PlanFragments}} **/
        private final Collection<PlanVertex> rounds[][];
        private int rounds_length;

        /**
         * StmtIndex -> Target Partition Ids
         */
        private final PartitionSet[] stmt_partitions;
        private final PartitionSet[] stmt_partitions_swap;

        /**
         * StmtIndex -> Map{PlanFragment, Set<PartitionIds>}
         */
        private final Map<PlanFragment, PartitionSet> frag_partitions[];
        private final Map<PlanFragment, PartitionSet> frag_partitions_swap[];

        /**
         * A bitmap of whether each query at the given index in the batch was single-partitioned or not
         */
        private final boolean singlepartition_bitmap[];

        /**
         * Whether the fragments of this batch plan consist of read-only operations
         **/
        protected boolean readonly = true;

        /**
         * Whether the batch plan can all be executed locally
         */
        protected boolean all_local = true;

        /**
         * Whether the fragments in the batch plan can be executed on a single site
         */
        protected boolean all_singlepartitioned = true;

        /** check if all local fragment work is non-transactional **/
        // protected boolean localFragsAreNonTransactional = true;

        /**
         * Default Constructor Must call init() before this BatchPlan can be used
         */
        @SuppressWarnings("unchecked")
        public BatchPlan(int max_round_size) {
            int batch_size = BatchPlanner.this.batchSize;
            int num_partitions = BatchPlanner.this.catalogContext.numberOfPartitions;

            // Round Data
            this.rounds = (Collection<PlanVertex>[][]) new Collection<?>[max_round_size][];
            for (int i = 0; i < this.rounds.length; i++) {
                this.rounds[i] = (Collection<PlanVertex>[]) new Collection<?>[num_partitions];
                // These lists will only be allocated when needed
            } // FOR

            // Batch Data
            this.frag_list = (List<PlanFragment>[]) new List<?>[batch_size];
            this.stmt_partitions = new PartitionSet[batch_size];
            this.stmt_partitions_swap = new PartitionSet[batch_size];
            this.frag_partitions = (Map<PlanFragment, PartitionSet>[]) new HashMap<?, ?>[batch_size];
            this.frag_partitions_swap = (Map<PlanFragment, PartitionSet>[]) new HashMap<?, ?>[batch_size];
            this.singlepartition_bitmap = new boolean[batch_size];
            for (int i = 0; i < batch_size; i++) {
                this.stmt_partitions[i] = new PartitionSet();
                this.frag_partitions[i] = new HashMap<PlanFragment, PartitionSet>();
            } // FOR
        }

        /**
         * @param base_partition
         * @param txn_id
         * @param batchSize
         */
        private BatchPlan init(int base_partition) {
            assert (this.cached == false);
            this.base_partition = base_partition;
            this.mispredict = null;

            this.readonly = true;
            this.all_local = true;
            this.all_singlepartitioned = true;
            
            for (int i = 0; i < this.frag_list.length; i++) {
                if (this.frag_list[i] != null)
                    this.frag_list[i] = null;
                if (this.stmt_partitions[i] != null && this.stmt_partitions_swap[i] == null)
                    this.stmt_partitions[i].clear();
            } // FOR
            for (int i = 0; i < this.rounds.length; i++) {
                for (int ii = 0; ii < this.rounds[i].length; ii++) {
                    if (this.rounds[i][ii] != null) this.rounds[i][ii].clear();
                } // FOR
            } // FOR

            return (this);
        }

        protected BatchPlanner getPlanner() {
            return (BatchPlanner.this);
        }
        protected PlanGraph getPlanGraph() {
            return (this.graph);
        }

        /**
         * Returns true if this txn was hit by a MispredictionException when we were
         * constructing this batch plan.
         */
        public boolean hasMisprediction() {
            return (this.mispredict != null);
        }

        /**
         * Returns the MispredictionException for this batch plan.
         */
        public MispredictionException getMisprediction() {
            return (this.mispredict);
        }

        /**
         * Convert this batch plan into a list of WorkFragment builders.
         * The stmtCounters is a list of the number of times that we have executed each 
         * query in the past for this transaction. The offset of each element in stmtCounters
         * corresponds to the stmtIndex in the SQLStmt batch. 
         * @param txn_id
         * @param stmtCounters
         * @param builders
         */
        public void getWorkFragmentsBuilders(Long txn_id, int[] stmtCounters, List<WorkFragment.Builder> builders) {
            BatchPlanner.this.createWorkFragmentsBuilders(txn_id, this, stmtCounters, builders);
        }

        protected int getBatchSize() {
            return (BatchPlanner.this.batchSize);
        }

        public int getFragmentCount() {
            return (this.graph.fragmentIds.length);
        }

        public long[] getFragmentIds() {
            return (this.graph.fragmentIds);
        }

        public int[] getOutputDependencyIds() {
            return (this.graph.output_ids);
        }

        public int[] getInputDependencyIds() {
            return (this.graph.input_ids);
        }
        
        /**
         * Return an array of PartitionSets where each element in the array
         * corresponds to the partitions that the SQLStmt in the batch will need
         * to execute on.
         * @return
         */
        public final PartitionSet[] getStatementPartitions() {
            return (this.stmt_partitions);
        }
        
        public boolean isReadOnly() {
            return (this.readonly);
        }

        public boolean isLocal() {
            return (this.all_local);
        }

        public boolean isSingleSited() {
            return (this.all_singlepartitioned);
        }

        public boolean isSingledPartitionedAndLocal() {
            return (this.all_singlepartitioned && this.all_local);
        }

        public boolean isCached() {
            return (this.cached);
        }

        @Override
        public String toString() {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Read Only", this.readonly);
            m.put("All Local", this.all_local);
            m.put("All Single-Partitioned", this.all_singlepartitioned);
            return StringUtil.formatMaps(m);
        }
    } // END CLASS

    /**
     * Testing Constructor
     * The batchSize is assumed to be the length of batchStmts
     * @param batchStmts
     * @param catalog_proc
     * @param p_estimator
     */
    public BatchPlanner(SQLStmt[] batchStmts, Procedure catalog_proc, PartitionEstimator p_estimator) {
        this(batchStmts, batchStmts.length, catalog_proc, p_estimator, false);
    }

    /**
     * Testing constructor where the planner is forced to choose single-partition queries
     * @param batchStmts
     * @param catalog_proc
     * @param p_estimator
     * @param forceSinglePartition
     */
    protected BatchPlanner(SQLStmt[] batchStmts, Procedure catalog_proc, PartitionEstimator p_estimator,
                           boolean forceSinglePartition) {
        this(batchStmts, batchStmts.length, catalog_proc, p_estimator, forceSinglePartition);
    }

    /**
     * Constructor
     * 
     * @param batchStmts
     * @param batchSize
     * @param catalog_proc
     * @param p_estimator
     * @param local_partition
     */
    public BatchPlanner(SQLStmt[] batchStmts, int batchSize, Procedure catalog_proc, PartitionEstimator p_estimator) {
        this(batchStmts, batchSize, catalog_proc, p_estimator, false);
    }

    /**
     * Full Constructor
     * 
     * @param batchStmts
     * @param batchSize
     * @param catalog_proc
     * @param p_estimator
     * @param forceSinglePartition
     */
    @SuppressWarnings("unchecked")
    public BatchPlanner(SQLStmt[] batchStmts,
                        int batchSize,
                        Procedure catalog_proc,
                        PartitionEstimator p_estimator,
                        boolean forceSinglePartition) {
        assert (catalog_proc != null);
        assert (p_estimator != null);

        this.hstore_conf = HStoreConf.singleton();
        this.catalog_proc = catalog_proc;
        this.catalogContext = p_estimator.getCatalogContext();
        this.batchSize = batchSize;
        this.p_estimator = p_estimator;
        this.plan = new BatchPlan(hstore_conf.site.planner_max_round_size);
        this.force_singlePartition = forceSinglePartition;
        this.enable_unique_ids = hstore_conf.site.planner_unique_dependency_ids;

        this.sorted_singlep_fragments = (List<PlanFragment>[]) new List<?>[this.batchSize];
        this.sorted_multip_fragments = (List<PlanFragment>[]) new List<?>[this.batchSize];

        this.catalog_stmts = new Statement[this.batchSize];
        this.stmt_is_readonly = new boolean[this.batchSize];
        this.stmt_is_replicatedonly = new boolean[this.batchSize];

        this.cache_isSinglePartition = (hstore_conf.site.planner_caching ? new boolean[this.batchSize] : null);
        this.cache_fastLookups = (hstore_conf.site.planner_caching ? new int[this.batchSize][] : null);
        this.cache_singlePartitionPlans = (hstore_conf.site.planner_caching ? new BatchPlan[this.catalogContext.numberOfPartitions] : null);
        int nonReplicatedStmtCnt = 0;
        for (int i = 0; i < this.batchSize; i++) {
            this.catalog_stmts[i] = batchStmts[i].getStatement();
            this.stmt_is_readonly[i] = batchStmts[i].getStatement().getReadonly();
            this.stmt_is_replicatedonly[i] = batchStmts[i].getStatement().getReplicatedonly() ||
                                             batchStmts[i].getStatement().getSecondaryindex();
            
            if (this.stmt_is_replicatedonly[i] == false) nonReplicatedStmtCnt++;
            if (trace.val)
                LOG.trace(String.format("INIT[%d] %s -> isReplicatedOnly[%s]",
                          i, this.catalog_stmts[i].fullName(), this.stmt_is_replicatedonly[i]));

            // CACHING
            // Since most batches are going to be single-partition, we will cache the
            // parameter offsets on how to determine whether a Statement is multi-partition or not
            if (hstore_conf.site.planner_caching) {
                this.cache_fastLookups[i] = p_estimator.getStatementEstimationParameters(this.catalog_stmts[i]);
                if (trace.val) 
                    LOG.trace(String.format("INIT[%d] %s Cached Fast-Lookup: %s",
                              i, this.catalog_stmts[i].fullName(), Arrays.toString(this.cache_fastLookups[i])));
            }
        } // FOR
        this.nonReplicatedStmtCount = nonReplicatedStmtCnt;

        // Static Cache Members
        if (CACHED_FRAGMENT_PARTITION_MAPS == null) {
            synchronized (BatchPlanner.class) {
                if (CACHED_FRAGMENT_PARTITION_MAPS == null)
                    BatchPlanner.clear(this.catalogContext.numberOfPartitions);
            } // SYNCH
        }
    }

    /**
     * Clear out internal cache
     * 
     * @param num_partitions
     *            The total number of partitions in the cluster
     */
    @SuppressWarnings("unchecked")
    public static synchronized void clear(int num_partitions) {
        CACHED_FRAGMENT_PARTITION_MAPS = (Map<Statement, Map<PlanFragment, PartitionSet>>[]) new Map<?, ?>[num_partitions];
        for (int i = 0; i < num_partitions; i++) {
            CACHED_FRAGMENT_PARTITION_MAPS[i] = new HashMap<Statement, Map<PlanFragment, PartitionSet>>();
        } // FOR
    }

    

    public Procedure getProcedure() {
        return this.catalog_proc;
    }
    public Statement[] getStatements() {
        return this.catalog_stmts;
    }
    public void setPrefetchFlag(boolean val) {
        this.prefetch = val;
    }

    /**
     * Return the Statement within this batch at the given offset
     * 
     * @return
     */
    public Statement getStatement(int idx) {
        return this.catalog_stmts[idx];
    }

    public int getBatchSize() {
        return (this.batchSize);
    }

    /**
     * Generate a new BatchPlan for a batch of queries requested by the txn
     * 
     * @param txn_id
     * @param base_partition
     * @param predict_partitions
     * @param touched_partitions
     * @param batchArgs
     * @return
     */
    public BatchPlan plan(final Long txn_id,
                          final int base_partition,
                          final PartitionSet predict_partitions,
                          final FastIntHistogram touched_partitions,
                          final ParameterSet[] batchArgs) {
        final boolean predict_singlePartitioned = (predict_partitions.size() == 1);
        
        if (hstore_conf.site.planner_profiling) {
            if (this.profiler == null)
                this.profiler = new BatchPlannerProfiler();
            this.profiler.plan_time.start();
            this.profiler.transactions.incrementAndGet();
        }
        if (debug.val) {
            LOG.debug(String.format("Constructing a new %s BatchPlan for %s txn #%d",
                      this.catalog_proc.getName(),
                      (predict_singlePartitioned ? "single-partition" : "distributed"), txn_id));
            if (trace.val) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                m.put("Batch Size", this.batchSize);
                for (int i = 0; i < this.batchSize; i++) {
                    String key = String.format("[%02d] %s", i, this.catalog_stmts[i].getName());
                    m.put(key, Arrays.toString(batchArgs[i].toArray()));
                }
                LOG.trace("Query Batch Dump\n" + StringUtil.formatMapsBoxed(m));
            }
        }

        // OPTIMIZATION: Check whether we can use a cached single-partition BatchPlan
        if (this.force_singlePartition || this.cache_fastLookups != null) {
            boolean is_allSinglePartition = true;

            // OPTIMIZATION: Skip all of this if we know that we're always
            // suppose to be single-partitioned
            if (this.force_singlePartition == false) {
                for (int stmt_index = 0; stmt_index < this.batchSize; stmt_index++) {
                    // If we don't have a cached fast-lookup here, then we need to check
                    // whether the statement is accessing only replicated tables and is read-only
                    if (this.cache_fastLookups[stmt_index] == null) {
                        if (this.stmt_is_replicatedonly[stmt_index] && this.stmt_is_readonly[stmt_index]) {
                            if (debug.val)
                                LOG.debug(String.format("[#%d-%02d] No fast look-ups for %s but stmt is replicated + read-only.",
                                          txn_id, stmt_index, this.catalog_stmts[stmt_index].fullName()));
                            this.cache_isSinglePartition[stmt_index] = true;
                        }
                        else {
                            if (debug.val)
                                LOG.debug(String.format("[#%d-%02d] No fast look-ups for %s. Cache is marked as not single-partitioned",
                                          txn_id, stmt_index, this.catalog_stmts[stmt_index].fullName()));
                            this.cache_isSinglePartition[stmt_index] = false;
                        }
                    }
                    // Otherwise, we'll use our fast look-ups to check to make sure that the 
                    // statement's input parameters match the txn's base partition
                    else {
                        if (debug.val)
                            LOG.debug(String.format("[#%d-%02d] Using fast-lookup caching for %s: %s", txn_id,
                                      stmt_index, this.catalog_stmts[stmt_index].fullName(),
                                      Arrays.toString(this.cache_fastLookups[stmt_index])));
                        Object params[] = batchArgs[stmt_index].toArray();
                        this.cache_isSinglePartition[stmt_index] = true;
                        for (int idx : this.cache_fastLookups[stmt_index]) {
                            int hash = p_estimator.getHasher().hash(params[idx], this.catalog_stmts[stmt_index]); 
                            if (hash != base_partition) {
                                if (debug.val)
                                    LOG.debug(String.format("[#%d-%02d] Failed to match cached partition info for %s at idx=%d: " +
                                    		 "hash[%d] != basePartition[%d]",
                                              txn_id, stmt_index, this.catalog_stmts[stmt_index].fullName(), idx,
                                              hash, base_partition));
                                this.cache_isSinglePartition[stmt_index] = false;
                                break;
                            }
                        } // FOR
                    }
                    if (trace.val)
                        LOG.trace(String.format("[#%d-%02d] cache_isSinglePartition[%s] = %s",
                                  txn_id, stmt_index,
                                  this.catalog_stmts[stmt_index].fullName(), this.cache_isSinglePartition[stmt_index]));
                    is_allSinglePartition = is_allSinglePartition && this.cache_isSinglePartition[stmt_index];
                } // FOR (Statement)
            }
            if (trace.val)
                LOG.trace(String.format("[#%d] is_allSinglePartition=%s", txn_id, is_allSinglePartition));

            // If all of the Statements are single-partition, then we can use
            // the cached BatchPlan if we already have one.
            // This saves a lot of trouble
            if (is_allSinglePartition && this.cache_singlePartitionPlans[base_partition] != null) {
                if (debug.val)
                    LOG.debug(String.format("[#%d] Using cached BatchPlan at partition #%02d: %s", txn_id,
                              base_partition, Arrays.toString(this.catalog_stmts)));
                if (hstore_conf.site.planner_profiling && profiler != null) {
                    profiler.plan_time.stop();
                    profiler.cached.incrementAndGet();
                }
                touched_partitions.put(base_partition, this.nonReplicatedStmtCount);
                return (this.cache_singlePartitionPlans[base_partition]);
            }
        }

        // Otherwise we have to construct a new BatchPlan
        this.plan.init(base_partition);

        // Only maintain the histogram of what partitions were touched if we
        // know that we're going to throw a MispredictionException
        Histogram<Integer> mispredict_h = null;
        boolean mispredict = false;

        // ----------------------------------------------------------------------------
        // MAIN LOGIC LOOP
        // This is where we go through each SQLStmt in the batch and figure out
        // what partitions it needs to touch.
        // ----------------------------------------------------------------------------
        for (int stmt_index = 0; stmt_index < this.batchSize; stmt_index++) {
            final Statement catalog_stmt = this.catalog_stmts[stmt_index];
            assert (catalog_stmt != null) :
                String.format("The Statement at index %d is null for %s",
                              stmt_index, this.catalog_proc);
            final Object params[] = batchArgs[stmt_index].toArray();
            if (trace.val)
                LOG.trace(String.format("[#%d-%02d] Calculating touched partitions plans for %s",
                          txn_id, stmt_index, catalog_stmt.fullName()));

            Map<PlanFragment, PartitionSet> frag_partitions = plan.frag_partitions[stmt_index];
            PartitionSet stmt_all_partitions = plan.stmt_partitions[stmt_index];

            boolean has_singlepartition_plan = catalog_stmt.getHas_singlesited();
            boolean is_replicated_only = this.stmt_is_replicatedonly[stmt_index];
            boolean is_read_only = this.stmt_is_readonly[stmt_index];
            boolean is_singlePartition = has_singlepartition_plan;
            boolean is_local = true;
            CatalogMap<PlanFragment> fragments = null;

            // OPTIMIZATION: Fast partition look-up caching
            // OPTIMIZATION: Read-only queries on replicated tables always just
            // go to the local partition
            // OPTIMIZATION: If we're force to be single-partitioned, pretend
            // that the table is replicated
            if ((this.cache_isSinglePartition != null && this.cache_isSinglePartition[stmt_index]) ||
                (is_replicated_only && is_read_only) ||
                (this.force_singlePartition)) {
                if (trace.val) {
                    if (this.cache_isSinglePartition[stmt_index]) {
                        LOG.trace(String.format("[#%d-%02d] Using fast-lookup for %s. " +
                        		  "Skipping PartitionEstimator",
                                  txn_id, stmt_index, catalog_stmt.fullName()));
                    } else {
                        LOG.trace(String.format("[#%d-%02d] %s is read-only and replicate-only." +
                        		  "Skipping PartitionEstimator",
                        		  txn_id, stmt_index, catalog_stmt.fullName()));
                    }
                }
                assert (has_singlepartition_plan);

                if (this.cache_singlePartitionFragmentPartitions == null) {
                    this.cache_singlePartitionFragmentPartitions = CACHED_FRAGMENT_PARTITION_MAPS[base_partition];
                }
                Map<PlanFragment, PartitionSet> cached_frag_partitions = this.cache_singlePartitionFragmentPartitions.get(catalog_stmt);
                if (cached_frag_partitions == null) {
                    cached_frag_partitions = new HashMap<PlanFragment, PartitionSet>();
                    PartitionSet p = this.catalogContext.getPartitionSetSingleton(base_partition);
                    for (PlanFragment catalog_frag : catalog_stmt.getFragments().values()) {
                        cached_frag_partitions.put(catalog_frag, p);
                    } // FOR
                    this.cache_singlePartitionFragmentPartitions.put(catalog_stmt, cached_frag_partitions);
                }
                if (plan.stmt_partitions_swap[stmt_index] == null) {
                    plan.stmt_partitions_swap[stmt_index] = plan.stmt_partitions[stmt_index];
                    plan.frag_partitions_swap[stmt_index] = plan.frag_partitions[stmt_index];
                }
                stmt_all_partitions = plan.stmt_partitions[stmt_index] = this.catalogContext.getPartitionSetSingleton(base_partition);
                frag_partitions = plan.frag_partitions[stmt_index] = cached_frag_partitions;
            }

            // Otherwise figure out whether the query can execute as
            // single-partitioned or not
            else {
                if (debug.val)
                    LOG.debug(String.format("[#%d-%02d] Computing touched partitions %s in txn #%d", txn_id,
                              stmt_index, catalog_stmt.fullName(), txn_id));

                if (plan.stmt_partitions_swap[stmt_index] != null) {
                    stmt_all_partitions = plan.stmt_partitions[stmt_index] = plan.stmt_partitions_swap[stmt_index];
                    plan.stmt_partitions_swap[stmt_index] = null;
                    stmt_all_partitions.clear();

                    frag_partitions = plan.frag_partitions[stmt_index] = plan.frag_partitions_swap[stmt_index];
                    plan.frag_partitions_swap[stmt_index] = null;
                }

                try {
                    // OPTIMIZATION: If we were told that the transaction is suppose to be
                    // single-partitioned, then we will throw the single-partitioned PlanFragments
                    // at the PartitionEstimator to get back what partitions each PlanFragment
                    // will need to go to. If we get multiple partitions, then we know that we
                    // mispredicted and we should throw a MispredictionException
                    // If we originally didn't predict that it was single-partitioned, then we
                    // actually still need to check whether the query should be single-partitioned or not.
                    // This is because a query may actually just want to execute on just one
                    // partition (note that it could be a local partition or the remote partition).
                    // We'll assume that it's single-partition <<--- Can we cache that??
                    while (true) {
                        if (is_singlePartition == false) stmt_all_partitions.clear();
                        fragments = (is_singlePartition ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
                        if (debug.val)
                            LOG.debug(String.format("[#%d-%02d] Estimating for %d %s-partition fragments",
                                      txn_id, stmt_index, fragments.size(),
                                      (is_singlePartition ? "single" : "multi")));

                        // PARTITION ESTIMATOR
                        if (hstore_conf.site.planner_profiling && profiler != null)
                            ProfileMeasurementUtil.swap(profiler.plan_time, profiler.partest_time);
                        this.p_estimator.getAllFragmentPartitions(frag_partitions,
                                                                  stmt_all_partitions,
                                                                  fragments.values(),
                                                                  params,
                                                                  base_partition);
                        if (hstore_conf.site.planner_profiling && profiler != null)
                            ProfileMeasurementUtil.swap(profiler.partest_time, profiler.plan_time);

                        int stmt_all_partitions_size = stmt_all_partitions.size();
                        if (is_singlePartition && stmt_all_partitions_size > 1) {
                            // If this was suppose to be multi-partitioned, then
                            // we want to stop right here!!
                            if (predict_singlePartitioned) {
                                if (trace.val)
                                    LOG.trace(String.format("Mispredicted txn #%d - Multiple Partitions %s",
                                              txn_id, stmt_all_partitions));
                                mispredict = true;
                                break;
                            }
                            // Otherwise we can let it wrap back around and construct the fragment
                            // mapping for the multi-partition PlanFragments
                            is_singlePartition = false;
                            continue;
                        }
                        is_local = (stmt_all_partitions_size == 1 && stmt_all_partitions.contains(base_partition));
                        if (is_local == false && predict_singlePartitioned) {
                            // Again, this is not what was suppose to happen!
                            if (trace.val)
                                LOG.trace(String.format("Mispredicted txn #%d - Remote Partitions %s",
                                          txn_id, stmt_all_partitions));
                            mispredict = true;
                            break;
                        } else if (predict_partitions.containsAll(stmt_all_partitions) == false) {
                            // Again, this is not what was suppose to happen!
                            if (trace.val)
                                LOG.trace(String.format("Mispredicted txn #%d - Unallocated Partitions %s / %s",
                                          txn_id, stmt_all_partitions, predict_partitions));
                            mispredict = true;
                            break;
                        }
                        // Score! We have a plan that works!
                        break;
                    } // WHILE
                    // Bad Mojo!
                } catch (Exception ex) {
                    String msg = "";
                    for (int i = 0; i < this.batchSize; i++) {
                        msg += String.format("[#%d-%02d] %s %s\n%5s\n", txn_id, i, catalog_stmt.fullName(),
                                catalog_stmt.getSqltext(), Arrays.toString(batchArgs[i].toArray()));
                    } // FOR
                    LOG.fatal("\n" + msg);
                    throw new RuntimeException("Unexpected error when planning " + catalog_stmt.fullName(), ex);
                }
            }
            if (debug.val)
                LOG.debug(String.format("[#%d-%02d] is_singlepartition=%s, partitions=%s",
                          txn_id, stmt_index, is_singlePartition, stmt_all_partitions));

            // Get a sorted list of the PlanFragments that we need to execute
            // for this query
            if (is_singlePartition) {
                if (this.sorted_singlep_fragments[stmt_index] == null) {
                    this.sorted_singlep_fragments[stmt_index] = PlanNodeUtil.getSortedPlanFragments(catalog_stmt, true);
                }
                plan.frag_list[stmt_index] = this.sorted_singlep_fragments[stmt_index];

                // Only mark that we touched these partitions if the Statement
                // is not on a replicated table or it's not read-only
                if (is_replicated_only == false || is_read_only == false) {
                    touched_partitions.put(stmt_all_partitions.get());
                }
            }
            // Distributed Query
            else {
                if (this.sorted_multip_fragments[stmt_index] == null) {
                    this.sorted_multip_fragments[stmt_index] = PlanNodeUtil.getSortedPlanFragments(catalog_stmt, false);
                }
                plan.frag_list[stmt_index] = this.sorted_multip_fragments[stmt_index];

                // Always mark that we are touching these partitions
                touched_partitions.put(stmt_all_partitions.values());

                // Note that will want to update is_singlePartitioned here for non-readonly replicated
                // querys when we have a one partition cluster because those queries don't have
                // single-partition query plans
                // if (this.num_partitions == 1 && is_replicated_only && is_read_only == false) {
                // is_singlePartition = true;
                // }
            }

            plan.readonly = plan.readonly && catalog_stmt.getReadonly();
            plan.all_singlepartitioned = plan.all_singlepartitioned && is_singlePartition;
            plan.all_local = plan.all_local && is_local;

            // Keep track of whether the current query in the batch was
            // single-partitioned or not
            plan.singlepartition_bitmap[stmt_index] = is_singlePartition;

            // Misprediction!!
            if (mispredict) {
                // If this is the first Statement in the batch that hits the mispredict,
                // then we need to create the histogram and populate it with the
                // partitions from the previous queries
                int start_idx = stmt_index;
                if (mispredict_h == null) {
                    mispredict_h = new FastIntHistogram();
                    start_idx = 0;
                }
                for (int i = start_idx; i <= stmt_index; i++) {
                    if (debug.val)
                        LOG.debug(String.format("Pending mispredict for txn #%d. " +
                                  "Checking whether to add partitions for batch statement %02d",
                                   txn_id, i));

                    // Make sure that we don't count the local partition if it
                    // was reading a replicated table.
                    if (this.stmt_is_replicatedonly[i] == false || 
                        (this.stmt_is_replicatedonly[i] && this.stmt_is_readonly[i] == false)) {
                        if (trace.val)
                            LOG.trace(String.format("%s touches non-replicated table. " +
                                      "Including %d partitions in mispredict histogram for txn #%d",
                                      this.catalog_stmts[i].fullName(), plan.stmt_partitions[i].size(), txn_id));
                        mispredict_h.put(plan.stmt_partitions[i]);
                    }
                } // FOR
                continue;
            }

            // ----------------------
            // DEBUG DUMP
            // ----------------------
            if (debug.val) {
                List<PlanFragment> _fragments = null;
                if (is_singlePartition && this.sorted_singlep_fragments[stmt_index] != null) {
                    _fragments = this.sorted_singlep_fragments[stmt_index];
                } else {
                    _fragments = this.sorted_multip_fragments[stmt_index];
                }

                Map<?, ?> maps[] = new Map[_fragments.size() + 1];
                int ii = 0;
                for (PlanFragment catalog_frag : _fragments) {
                    Map<String, Object> m = new LinkedHashMap<String, Object>();
                    PartitionSet p = plan.frag_partitions[stmt_index].get(catalog_frag);
                    boolean frag_local = (p.size() == 1 && p.contains(base_partition));
                    m.put(String.format("[%02d] Fragment", ii), catalog_frag.fullName());
                    m.put(String.format("     Partitions"), p);
                    m.put(String.format("     IsLocal"), frag_local);
                    ii++;
                    maps[ii] = m;
                } // FOR

                Map<String, Object> header = new LinkedHashMap<String, Object>();
                header.put("Batch Statement", String.format("#%d / %d", stmt_index, this.batchSize));
                header.put("Catalog Statement", catalog_stmt.fullName());
                header.put("Statement SQL", catalog_stmt.getSqltext());
                header.put("All Partitions", plan.stmt_partitions[stmt_index]);
                header.put("Local Partition", base_partition);
                header.put("IsSingledPartitioned", is_singlePartition);
                header.put("IsStmtLocal", is_local);
                header.put("IsReplicatedOnly", is_replicated_only);
                header.put("IsBatchLocal", plan.all_local);
                header.put("Fragments", _fragments.size());
                maps[0] = header;

                LOG.debug(String.format("[#%d-%02d]\n%s", txn_id, stmt_index, StringUtil.formatMapsBoxed(maps)));
            }
        } // FOR (Statement)

        // Check whether we have an existing graph exists for this batch
        // configuration
        // This is the only place where we need to synchronize
        int bitmap_hash = Arrays.hashCode(plan.singlepartition_bitmap);
        PlanGraph graph = this.plan_graphs.get(bitmap_hash);
        if (graph == null) { // assume fast case
            graph = this.buildPlanGraph(plan);
            this.plan_graphs.put(bitmap_hash, graph);
        }
        plan.graph = graph;
        plan.rounds_length = graph.num_rounds;

        if (hstore_conf.site.planner_profiling && profiler != null)
            profiler.plan_time.stop();

        // Create the MispredictException if any Statement in the loop above hit
        // it. We don't want to throw it because whoever called us may want to look
        // at the plan first
        if (mispredict_h != null) {
            plan.mispredict = new MispredictionException(txn_id, mispredict_h);
            if (debug.val)
                LOG.warn(String.format("Created %s for txn #%d\n%s",
                         plan.mispredict.getClass().getSimpleName(), txn_id,
                         plan.mispredict.getPartitions()));
        }
        // If this a single-partition plan and we have caching enabled, we'll
        // add this to our cached listing. We'll mark it as cached so that it is never
        // returned back to the BatchPlan object pool
        else if (this.cache_singlePartitionPlans != null &&
                 this.cache_singlePartitionPlans[base_partition] == null &&
                 this.plan.isSingledPartitionedAndLocal()) {
            this.cache_singlePartitionPlans[base_partition] = plan;
            this.plan.cached = true;
            this.plan = new BatchPlan(hstore_conf.site.planner_max_round_size);
            return this.cache_singlePartitionPlans[base_partition];
        }

        if (debug.val)
            LOG.debug(String.format("Created BatchPlan for txn #%d:\n%s", txn_id, this.plan.toString()));
        return (this.plan);
    }

    /**
     * Utility method for converting a BatchPlan into WorkFragment.Builders.
     * The stmtCounters is a list of the number of times that we have executed each 
     * query in the past for this transaction. The offset of each element in stmtCounters
     * corresponds to the stmtIndex in the SQLStmt batch. 
     * @param txn_id
     * @param plan
     * @param stmtCounters
     * @param graph
     * @param builders
     */
    protected void createWorkFragmentsBuilders(final Long txn_id,
                                               final BatchPlanner.BatchPlan plan,
                                               final int[] stmtCounters,
                                               final List<WorkFragment.Builder> builders) {

        if (hstore_conf.site.planner_profiling && profiler != null)
            profiler.fragment_time.start();
        if (debug.val)
            LOG.debug(String.format("Constructing list of WorkFragments to execute " +
            		  "[txn_id=#%d, base_partition=%d]",
                      txn_id, plan.base_partition));

        // 2013-05-14: I feel like that we could probably cache this somehow...
        for (PlanVertex v : plan.graph.sorted_vertices) {
            int stmt_index = v.stmt_index;
            for (int partition : plan.frag_partitions[stmt_index].get(v.catalog_frag).values()) {
                if (plan.rounds[v.round][partition] == null) {
                    plan.rounds[v.round][partition] = new ArrayList<PlanVertex>();
                }
                plan.rounds[v.round][partition].add(v);
            } // FOR
        } // FOR

        // The main idea of what we're trying to do here is to group together
        // all of the PlanFragments with the same input dependency ids into a single WorkFragment
        if (trace.val)
            LOG.trace("Generated " + plan.rounds_length + " rounds of tasks for txn #" + txn_id);
        for (int round = 0; round < plan.rounds_length; round++) {
            if (trace.val) LOG.trace(String.format("Txn #%d - Round %02d", txn_id, round));
            for (int partition = 0; partition < this.catalogContext.numberOfPartitions; partition++) {
                Collection<PlanVertex> vertices = plan.rounds[round][partition];
                if (vertices == null || vertices.isEmpty()) continue;

                this.round_builders.clear();
                for (PlanVertex v : vertices) { // Does this order matter?
                    // Check whether we can use an existing WorkFragment builder
                    WorkFragment.Builder partitionBuilder = this.round_builders.get(v.input_dependency_id);
                    if (partitionBuilder == null) {
                        partitionBuilder = WorkFragment.newBuilder().setPartitionId(partition);
                        this.round_builders.put(v.input_dependency_id, partitionBuilder);
                        partitionBuilder.setReadOnly(true);
                        partitionBuilder.setPrefetch(this.prefetch);
                    }

                    // Fragment Id
                    partitionBuilder.addFragmentId(v.frag_id);

                    // Not all fragments will have an input dependency so this
                    // could be the NULL_DEPENDENCY_ID
                    partitionBuilder.addInputDepId(v.input_dependency_id);
                    if (v.input_dependency_id != HStoreConstants.NULL_DEPENDENCY_ID) {
                        partitionBuilder.setNeedsInput(true);
                    }

                    // All fragments will produce some output
                    partitionBuilder.addOutputDepId(v.output_dependency_id);

                    // SQLStmt Counter
                    partitionBuilder.addStmtCounter(stmtCounters[v.stmt_index]);

                    // SQLStmt Index
                    partitionBuilder.addStmtIndex(v.stmt_index);
                    
                    // SQLStmt Ignore
                    // This query was already dispatched for prefetching, so we 
                    // actually don't want to really execute it.
                    partitionBuilder.addStmtIgnore(false);
                    
                    // ParameterSet Index
                    partitionBuilder.addParamIndex(v.stmt_index);

                    // Read-Only
                    if (v.read_only == false) {
                        partitionBuilder.setReadOnly(v.read_only);
                    }

                    if (trace.val)
                        LOG.trace(String.format("Fragment Grouping %d => " +
                                  "[txnId=#%d, partition=%d, fragDd=%d, input=%d, output=%d, stmtIndex=%d]",
                                  partitionBuilder.getFragmentIdCount(),
                                  txn_id, partition, v.frag_id,
                                  v.input_dependency_id, v.output_dependency_id, v.stmt_index));
                    
                } // FOR (frag_idx)

                for (WorkFragment.Builder builder : this.round_builders.values()) {
                    int fragmentCount = builder.getFragmentIdCount();
                    if (fragmentCount == 0) {
                        if (trace.val) {
                            LOG.warn(String.format("For some reason we thought it would be a good idea to " +
                            		 "construct a %s with no fragments! [txn_id=#%d]",
                                    WorkFragment.class.getSimpleName(), txn_id));
                            LOG.warn("In case you were wondering, this is a terrible idea, which is why we didn't do it!");
                        }
                        continue;
                    }
                    assert(builder.getOutputDepIdCount() == fragmentCount) :
                        "OutputDepId:" + builder.getOutputDepIdCount() + "!=" + fragmentCount;
                    assert(builder.getInputDepIdCount() == fragmentCount) :
                        "InputDepId:" + builder.getInputDepIdCount() + "!=" + fragmentCount;
                    assert(builder.getParamIndexCount() == fragmentCount) :
                        "ParamIndex:" + builder.getParamIndexCount() + "!=" + fragmentCount;
                    assert(builder.getStmtCounterCount() == fragmentCount) :
                        "StmtCounter:" + builder.getStmtCounterCount() + "!=" + fragmentCount;
                    assert(builder.getStmtIndexCount() == fragmentCount) :
                        "StmtIndex:" + builder.getStmtIndexCount() + "!=" + fragmentCount;
                    assert(builder.getStmtIgnoreCount() == fragmentCount) :
                        "StmtIgnore:" + builder.getStmtIgnoreCount() + "!=" + fragmentCount;
                    
                    builders.add(builder);
                } // FOR

                // if (debug.val) {
                // LOG.debug(String.format("New WorkFragment to run at partition #%d with %d fragments for txn #%d "
                // +
                // "[ids=%s, inputs=%s, outputs=%s]",
                // partition, num_frags, txn_id,
                // Arrays.toString(frag_ids), Arrays.toString(input_ids),
                // Arrays.toString(output_ids)));
                // if (trace.val)
                // LOG.trace("Fragment Contents: [txn_id=#" + txn_id + "]\n" +
                // task.toString());
                // }
            } // PARTITION
        } // ROUND
        assert (builders.size() > 0) : "Failed to generate any WorkFragments in this BatchPlan for txn #" + txn_id;
        if (debug.val)
            LOG.debug("Created " + builders.size() + " WorkFragment(s) for txn #" + txn_id);
        if (hstore_conf.site.planner_profiling && profiler != null)
            profiler.fragment_time.stop();
    }

    /**
     * Construct
     * 
     * @param plan
     * @return
     */
    protected PlanGraph buildPlanGraph(BatchPlanner.BatchPlan plan) {
        if (hstore_conf.site.planner_profiling && profiler != null)
            ProfileMeasurementUtil.swap(profiler.plan_time, profiler.graph_time);
        PlanGraph graph = new PlanGraph();

        this.sorted_vertices.clear();
        this.output_dependency_xref_clear.clear();

        int last_id = FIRST_DEPENDENCY_ID;
        for (int stmt_index = 0; stmt_index < this.batchSize; stmt_index++) {
            Map<PlanFragment, PartitionSet> frag_partitions = plan.frag_partitions[stmt_index];
            assert (frag_partitions != null) : "No Fragment->PartitionIds map for Statement #" + stmt_index;

            List<PlanFragment> fragments = plan.frag_list[stmt_index];
            assert (fragments != null);
            int num_fragments = fragments.size();
            graph.num_rounds = Math.max(num_fragments, graph.num_rounds);

            // Generate the synthetic DependencyIds for the query
            int last_output_id = HStoreConstants.NULL_DEPENDENCY_ID;
            for (int round = 0, cnt = num_fragments; round < cnt; round++) {
                PlanFragment catalog_frag = fragments.get(round);
                PartitionSet f_partitions = frag_partitions.get(catalog_frag);
                assert (f_partitions != null) :
                    String.format("No PartitionIds for [%02d] %s in Statement #%d", round,
                                  catalog_frag.fullName(), stmt_index);
                boolean f_local = (f_partitions.size() == 1 && f_partitions.contains(plan.base_partition));
                final Integer output_id = Integer.valueOf(this.enable_unique_ids ?
                            BatchPlanner.NEXT_DEPENDENCY_ID.getAndIncrement() : last_id++);

                PlanVertex v = new PlanVertex(catalog_frag,
                                              stmt_index,
                                              round,
                                              last_output_id,
                                              output_id.intValue(),
                                              f_local);
                Set<PlanVertex> dependencies = output_dependency_xref.get(output_id);
                if (dependencies == null) {
                    dependencies = new HashSet<PlanVertex>();
                    this.output_dependency_xref.put(output_id, dependencies);
                } else if (this.output_dependency_xref_clear.contains(output_id) == false) {
                    dependencies.clear();
                    this.output_dependency_xref_clear.add(output_id);
                }
                dependencies.add(v);

                graph.addVertex(v);
                this.sorted_vertices.add(v);
                last_output_id = output_id;
            }
        } // FOR

        // Setup Edges
        for (PlanVertex v0 : graph.getVertices()) {
            if (v0.input_dependency_id == HStoreConstants.NULL_DEPENDENCY_ID)
                continue;
            for (PlanVertex v1 : output_dependency_xref.get(v0.input_dependency_id)) {
                assert (!v0.equals(v1)) : v0;
                if (!graph.findEdgeSet(v0, v1).isEmpty())
                    continue;
                graph.addEdge(v0.input_dependency_id, v0, v1);
            } // FOR
        } // FOR

        // Single-Partition Cache
        Collections.sort(this.sorted_vertices, PLANVERTEX_COMPARATOR);
        final int num_vertices = this.sorted_vertices.size();
        graph.fragmentIds = new long[num_vertices];
        graph.input_ids = new int[num_vertices];
        graph.output_ids = new int[num_vertices];
        int i = 0;
        for (PlanVertex v : this.sorted_vertices) {
            graph.fragmentIds[i] = v.frag_id;
            graph.output_ids[i] = v.output_dependency_id;
            graph.input_ids[i] = v.input_dependency_id;
            i += 1;
        } // FOR
        graph.sorted_vertices = this.sorted_vertices.toArray(new PlanVertex[0]);

        if (hstore_conf.site.planner_profiling && profiler != null)
            ProfileMeasurementUtil.swap(profiler.graph_time, profiler.plan_time);
        return (graph);
    }

    private static Comparator<PlanVertex> PLANVERTEX_COMPARATOR = new Comparator<PlanVertex>() {
        @Override
        public int compare(PlanVertex o1, PlanVertex o2) {
            if (o1.stmt_index != o2.stmt_index)
                return (o1.stmt_index - o2.stmt_index);
            if (o1.round != o2.round)
                return (o1.round - o2.round);
            return (o1.frag_id - o2.frag_id);
        }
    };
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    public class Debug implements DebugContext {
        public BatchPlan getBatchPlan() {
            return (plan);
        }
        public BatchPlannerProfiler getProfiler() {
            return (profiler);
        }
        public boolean isCachedReplicatedOnly(int stmt_index) {
            return (stmt_is_replicatedonly[stmt_index]);
        }
        public boolean isCachedReadOnly(int stmt_index) {
            return (stmt_is_readonly[stmt_index]);
        }
        public int[] getCachedLookup(int stmt_index) {
            return (cache_fastLookups[stmt_index]);
        }
        public BatchPlan getCachedSinglePartitionPlan(int stmt_index) {
            return (cache_singlePartitionPlans[stmt_index]);
        }
        
    }
    
    private Debug cachedDebugContext;
    public Debug getDebugContext() {
        if (this.cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new Debug();
        }
        return this.cachedDebugContext;
    }
}
