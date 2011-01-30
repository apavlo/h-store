package org.voltdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.IGraph;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

/**
 * @author pavlo
 */
public class BatchPlanner {
    private static final Logger LOG = Logger.getLogger(BatchPlanner.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // STATIC DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected static final AtomicInteger NEXT_DEPENDENCY_ID = new AtomicInteger(1000);

    public static final int MAX_BATCH_SIZE = 64;
    
    public static final int MAX_ROUND_SIZE = 10;

    public static final int PLAN_POOL_INITIAL_SIZE = 5000;
    
    /**
     * BatchPlan Object Pool
     */
    private static final ObjectPool PLAN_POOL = new StackObjectPool(new BasePoolableObjectFactory() {
        @Override
        public Object makeObject() throws Exception {
            return (new BatchPlanner.BatchPlan());
        }
        @Override
        public void activateObject(Object arg0) throws Exception {
            BatchPlanner.BatchPlan plan = (BatchPlanner.BatchPlan)arg0;
            plan.txn_id = null;
        }
    }, PLAN_POOL_INITIAL_SIZE, PLAN_POOL_INITIAL_SIZE);

    /**
     * Pre-load a bunch of BatchPlans so that we don't have to make them as needed
     * @param num_partitions
     */
    public static void preload(int num_partitions) {
        List<BatchPlan> plans = new ArrayList<BatchPlan>();
        try {
            for (int i = 0; i < PLAN_POOL_INITIAL_SIZE; i++) {
                BatchPlan plan = (BatchPlan)PLAN_POOL.borrowObject();
                plan.init(-1l, -1l, 0, 0, num_partitions);
                plans.add(plan);
            } // FOR
            
            for (BatchPlan plan : plans) {
                plan.finished();
                PLAN_POOL.returnObject(plan);
            } // FOR
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    // Used for turning ParameterSets into ByteBuffers
//    protected final FastSerializer fs = new FastSerializer();
    
    // the set of dependency ids for the expected results of the batch
    // one per sql statment
    protected final ArrayList<Integer> depsToResume = new ArrayList<Integer>();

    protected final Catalog catalog;
    protected final Procedure catalog_proc;
    protected final Statement catalog_stmts[];
    protected final List<PlanFragment> sorted_singlep_fragments[];
    protected final List<PlanFragment> sorted_multip_fragments[];
    protected final int batchSize;
    protected final PartitionEstimator p_estimator;
    private final int num_partitions;

    private final Map<Integer, PlanGraph> plan_graphs = new HashMap<Integer, PlanGraph>(); 
    
    // ----------------------------------------------------------------------------
    // INTERNAL PLAN GRAPH ELEMENTS
    // ----------------------------------------------------------------------------

    protected static class PlanVertex extends AbstractVertex {
        final int frag_id;
        final int stmt_index;
        final int round;
        final Integer input_dependency_id;
        final Integer output_dependency_id;
        final int hash_code;

        public PlanVertex(PlanFragment catalog_frag,
                          int stmt_index,
                          int round,
                          Integer input_dependency_id,
                          Integer output_dependency_id,
                          boolean is_local) {
            super(catalog_frag);
            this.frag_id = catalog_frag.getId();
            this.stmt_index = stmt_index;
            this.round = round;
            this.input_dependency_id = input_dependency_id;
            this.output_dependency_id = output_dependency_id;
            
            this.hash_code = this.frag_id | this.round<<16 | this.stmt_index<<24;  
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PlanVertex)) return (false);
            return (this.hash_code == ((PlanVertex)obj).hash_code);
        }
        
        @Override
        public int hashCode() {
            return (this.hash_code);
        }
        
        @Override
        public String toString() {
            return String.format("<FragId=%02d, StmtIndex=%02d, Round=%02d, Input=%02d, Output=%02d>",
                                 this.frag_id, this.stmt_index, this.round, this.input_dependency_id, this.output_dependency_id);
        }
    }
    
    private static class PlanEdge extends AbstractEdge {
        final Integer dep_id;
        public PlanEdge(IGraph<PlanVertex, PlanEdge> graph, Integer dep_id) {
            super(graph);
            this.dep_id = dep_id;
        }
        
        @Override
        public String toString() {
            return this.dep_id.toString();
        }
    }
    
    private static class PlanGraph extends AbstractDirectedGraph<PlanVertex, PlanEdge> {
        private static final long serialVersionUID = 1L;
        
        private final Map<Integer, Set<PlanVertex>> output_dependency_xref = new HashMap<Integer, Set<PlanVertex>>();
        
        private int max_rounds;
        
        public PlanGraph(Database catalog_db) {
            super(catalog_db);
        }
        
        @Override
        public boolean addVertex(PlanVertex v) {
            Integer output_id = v.output_dependency_id;
            assert(output_id != null) : "Unexpected: " + v;
            
            if (!this.output_dependency_xref.containsKey(output_id)) {
                this.output_dependency_xref.put(output_id, new HashSet<PlanVertex>());
            }
            this.output_dependency_xref.get(output_id).add(v);
            return super.addVertex(v);
        }
        
        public Set<PlanVertex> getOutputDependencies(Integer output_id) {
            return (this.output_dependency_xref.get(output_id));
        }
        
    }

    // ----------------------------------------------------------------------------
    // BATCH PLAN
    // ----------------------------------------------------------------------------
    
    /**
     * BatchPlan
     */
    public static class BatchPlan {
        // ----------------------------------------------------------------------------
        // INVOCATION DATA MEMBERS
        // ----------------------------------------------------------------------------
        
        private Long txn_id;
        private long client_handle;
        private int batchSize;
        private Integer base_partition;
        private boolean initialized = false;

        /** The serialized ByteBuffers for each Statement in the batch */ 
        private final ByteBuffer param_buffers[] = new ByteBuffer[MAX_BATCH_SIZE];
        private final FastSerializer param_serializers[] = new FastSerializer[MAX_BATCH_SIZE];
        
        /** Temporary buffer space for sorting the PlanFragments per Statement */
        private final List<PlanFragment> frag_list[];
        
        /**
         * We need to generate a list of FragmentTaskMessages that we will ship off to either
         * remote execution sites or be executed locally. Note that we have to separate
         * any tasks that have a input dependency from those that don't,  because
         * we can only block at the FragmentTaskMessage level (as opposed to blocking by PlanFragment)
         */
        private final List<FragmentTaskMessage> ftasks = new ArrayList<FragmentTaskMessage>();
        
        /** Round# -> Map{PartitionId, Set{PlanFragments}} **/
        private final Set<PlanVertex> rounds[][];
        private int rounds_length;

        /** Statement Target Partition Ids **/
        private final Set<Integer>[] stmt_partitions;

        /**
         * StmtIndex -> Map{PlanFragment, Set<PartitionIds>} 
         */
        private final Map<PlanFragment, Set<Integer>> frag_partitions[];
        
        /**
         * A bitmap of whether each query at the given index in the batch was single-partitioned or not
         */
        private final boolean singlepartition_bitmap[] = new boolean[MAX_BATCH_SIZE];

        // ----------------------------------------------------------------------------
        // DO WE STILL NEED THESE???
        // ----------------------------------------------------------------------------
        
        /** Whether the fragments of this batch plan consist of read-only operations **/
        protected boolean readonly = true;
        
        /** Whether the batch plan can all be executed locally **/
        protected boolean all_local = true;
        
        /** Whether the fragments in the batch plan can be executed on a single site **/
        protected boolean all_singlesited = true;    

        /** check if all local fragment work is non-transactional **/
        protected boolean localFragsAreNonTransactional = true;
        
        /**
         * Default Constructor
         * Must call init() before this BatchPlan can be used
         */
        @SuppressWarnings("unchecked")
        public BatchPlan() {
            // Round Data
            this.rounds = (Set<PlanVertex>[][])new Set<?>[MAX_ROUND_SIZE][];
            
            // Batch Data
            this.frag_list = (List<PlanFragment>[])new List<?>[MAX_BATCH_SIZE];
            this.stmt_partitions = (Set<Integer>[])new Set<?>[MAX_BATCH_SIZE];
            this.frag_partitions = (Map<PlanFragment, Set<Integer>>[])new HashMap<?, ?>[MAX_BATCH_SIZE];           
        }

        /**
         * 
         * @param txn_id
         * @param client_handle
         * @param base_partition
         * @param batchSize
         * @param num_partitions
         */
        @SuppressWarnings("unchecked")
        private void init(long txn_id, long client_handle, int base_partition, int batchSize, int num_partitions) {
            assert(this.txn_id == null);
            this.txn_id = txn_id;
            this.client_handle = client_handle;
            this.batchSize = batchSize;
            this.base_partition = base_partition;
            
            // Need to initialize some internal data strucutres the first time we are called
            // This is because we can't pass in the number of partitions when we create the object from the pool
            // the stmt_partitions array the first time we're called
            if (this.initialized == false) {
                for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                    this.stmt_partitions[i] = new HashSet<Integer>();
                    this.frag_partitions[i] = new HashMap<PlanFragment, Set<Integer>>();
                    this.param_serializers[i] = new FastSerializer();
                } // FOR
             
                for (int i = 0; i < this.rounds.length; i++) {
                    this.rounds[i] = (Set<PlanVertex>[])new Set<?>[num_partitions];
                    for (int ii = 0; ii < num_partitions; ii++) {
                        this.rounds[i][ii] = new HashSet<PlanVertex>();
                    } // FOR
                } // FOR
                this.initialized = true;
            }
        }
        
        /**
         * Marks this BatchPlan as completed (i.e., all of the PlanFragments have
         * been executed the and the results have been returned. This must be called before
         * returning back to the user-level VoltProcedure
         */
        public void finished() {
            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                this.frag_list[i] = null;
                this.stmt_partitions[i].clear();
                this.param_serializers[i].clear();
                for (Set<Integer> s : this.frag_partitions[i].values()) {
                    s.clear();
                }
            } // FOR
            for (int i = 0; i < this.rounds.length; i++) {
                for (int ii = 0; ii < this.rounds[i].length; ii++) {
                    this.rounds[i][ii].clear();
                } // FOR
            } // FOR
            
            try {
                BatchPlanner.PLAN_POOL.returnObject(this);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        
        /**
         * Return the list of FragmentTaskMessages that need to be executed for this BatchPlan
         * @return
         */
        public List<FragmentTaskMessage> getFragmentTaskMessages() {
            return (this.ftasks);
        }

        protected int getLocalFragmentCount(int base_partition) {
            int cnt = 0;
            for (FragmentTaskMessage ftask : this.ftasks) {
                if (ftask.getDestinationPartitionId() == base_partition) cnt++;
            } // FOR
            return (cnt);
        }
        protected int getRemoteFragmentCount(int base_partition) {
            int cnt = 0;
            for (FragmentTaskMessage ftask : this.ftasks) {
                if (ftask.getDestinationPartitionId() != base_partition) cnt++;
            } // FOR
            return (cnt);
        }
        
        public int getBatchSize() {
            return (this.batchSize);
        }
        
        /**
         * Get an array of sets of partition ids for this plan
         * Note that you can't rely on the  
         * @return
         */
        public Set<Integer>[] getStatementPartitions() {
            return (this.stmt_partitions);
        }
        
        public boolean isReadOnly() {
            return (this.readonly);
        }
        
        public boolean isLocal() {
            return (this.all_local);
        }
        
        public boolean isSingleSited() {
            return (this.all_singlesited);
        }
        
        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("Read Only:        ").append(this.readonly).append("\n")
             .append("All Local:        ").append(this.all_local).append("\n")
             .append("All Single-Sited: ").append(this.all_singlesited).append("\n")
             .append("------------------------------\n")
             .append(StringUtil.join("\n", this.ftasks));
            return (b.toString());
        }
    } // END CLASS
    
    /**
     * Testing Constructor
     */
    public BatchPlanner(SQLStmt[] batchStmts, Procedure catalog_proc, PartitionEstimator p_estimator) {
        this(batchStmts, batchStmts.length, catalog_proc, p_estimator);
        
    }

    /**
     * Constructor
     * @param batchStmts
     * @param batchSize
     * @param catalog_proc
     * @param p_estimator
     * @param local_partition
     */
    @SuppressWarnings("unchecked")
    public BatchPlanner(SQLStmt[] batchStmts, int batchSize, Procedure catalog_proc, PartitionEstimator p_estimator) {
        assert(catalog_proc != null);
        assert(p_estimator != null);

        this.batchSize = batchSize;
        this.catalog_proc = catalog_proc;
        this.catalog = catalog_proc.getCatalog();
        this.p_estimator = p_estimator;
        this.num_partitions = CatalogUtil.getNumberOfPartitions(catalog_proc);

        this.sorted_singlep_fragments = (List<PlanFragment>[])new List<?>[this.batchSize];
        this.sorted_multip_fragments = (List<PlanFragment>[])new List<?>[this.batchSize];
        
        this.catalog_stmts = new Statement[this.batchSize];
        for (int i = 0; i < this.batchSize; i++) {
            this.catalog_stmts[i] = batchStmts[i].catStmt;
        } // FOR
    }

    public Statement[] getStatements() {
        return this.catalog_stmts;
    }
    
    /**
     * 
     * @param txn_id
     * @param client_handle
     * @param batchArgs
     * @param predict_singlepartitioned
     * @return
     */
    public BatchPlan plan(long txn_id, long client_handle, int base_partition, ParameterSet[] batchArgs, boolean predict_singlepartitioned) {
        if (debug.get()) LOG.debug(String.format("Constructing a new %s BatchPlan for txn #%d", this.catalog_proc.getName(), txn_id));
        
        BatchPlan plan = null;
        try {
            plan = (BatchPlan)BatchPlanner.PLAN_POOL.borrowObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        plan.init(txn_id, client_handle, base_partition, this.batchSize, this.num_partitions);

        // ----------------------
        // DEBUG DUMP
        // ----------------------
        if (trace.get()) {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            m.put("Batch Size", this.batchSize);
            for (int i = 0; i < this.batchSize; i++) {
                m.put(String.format("[%02d] %s", i, this.catalog_stmts[i].getName()), Arrays.toString(batchArgs[i].toArray()));
            }
            LOG.trace("\n" + StringUtil.formatMapsBoxed(m));
        }
       
        for (int stmt_index = 0; stmt_index < this.batchSize; stmt_index++) {
            final Statement catalog_stmt = this.catalog_stmts[stmt_index];
            assert(catalog_stmt != null) : "The Statement at index " + stmt_index + " is null for " + this.catalog_proc;
            final ParameterSet paramSet = batchArgs[stmt_index];
            final Object params[] = paramSet.toArray();
            if (trace.get()) LOG.trace(String.format("[%02d] Constructing fragment plans for %s", stmt_index, catalog_stmt.fullName()));
            
            final Map<PlanFragment, Set<Integer>> frag_partitions = plan.frag_partitions[stmt_index];
            final Set<Integer> stmt_all_partitions = plan.stmt_partitions[stmt_index];

            boolean has_singlepartition_plan = catalog_stmt.getHas_singlesited();
            boolean stmt_localFragsAreNonTransactional = plan.localFragsAreNonTransactional;
            boolean mispredict = false;
            boolean is_singlepartition = has_singlepartition_plan;
            boolean is_local = true;
            CatalogMap<PlanFragment> fragments = null;
            
            try {
                // Optimization: If we were told that the transaction is suppose to be single-partitioned, then we will
                // throw the single-partitioned PlanFragments at the PartitionEstimator to get back what partitions
                // each PlanFragment will need to go to. If we get multiple partitions, then we know that we mispredicted and
                // we should throw a MispredictionException
                // If we originally didn't predict that it was single-partitioned, then we actually still need to check
                // whether the query should be single-partitioned or not. This is because a query may actually just want
                // to execute on just one partition (note that it could be a local partition or the remote partition).
                // We'll assume that it's single-partition <<--- Can we cache that??
                while (true) {
                    stmt_all_partitions.clear();
                    
                    fragments = (is_singlepartition ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
                    this.p_estimator.getAllFragmentPartitions(frag_partitions,
                                                              stmt_all_partitions,
                                                              fragments, params, plan.base_partition);
                    is_local = (stmt_all_partitions.size() == 1 && stmt_all_partitions.contains(plan.base_partition));
                    if (is_singlepartition && stmt_all_partitions.size() > 1) {
                        // If this was suppose to be multi-partitioned, then we want to stop right here!!
                        if (predict_singlepartitioned) {
                            if (trace.get()) LOG.trace(String.format("Mispredicted txn #%d - Multiple Partitions"));
                            mispredict = true;
                            break;
                        }
                        // Otherwise we can let it wrap back around and construct the fragment mapping for the
                        // multi-partition PlanFragments
                        is_singlepartition = false;
                        continue;
                    } else if (is_local == false && predict_singlepartitioned) {
                        // Again, this is not what was suppose to happen!
                        if (trace.get()) LOG.trace(String.format("Mispredicted txn #%d - Remote Partitions"));
                        mispredict = true;
                        break;
                    }
                    // Score! We have a plan that works!
                    break;
                } // WHILE
            } catch (Exception ex) {
                String msg = "";
                for (int i = 0; i < this.batchSize; i++) {
                    msg += String.format("[%02d] %s %s\n     %s\n", i, catalog_stmt.fullName(), catalog_stmt.getSqltext(), Arrays.toString(batchArgs[i].toArray()));
                }
                LOG.fatal("\n" + msg);
                throw new RuntimeException("Unexpected error when planning " + catalog_stmt.fullName(), ex);
            }
            
            // Misprediction!!
            if (mispredict) throw new MispredictionException(txn_id);

            // Get a sorted list of the PlanFragments that we need to execute for this query
            if (is_singlepartition) {
                if (this.sorted_singlep_fragments[stmt_index] == null) {
                    this.sorted_singlep_fragments[stmt_index] = QueryPlanUtil.getSortedPlanFragments(catalog_stmt, true); 
                }
                plan.frag_list[stmt_index] = this.sorted_singlep_fragments[stmt_index];
            } else {
                if (this.sorted_multip_fragments[stmt_index] == null) {
                    this.sorted_multip_fragments[stmt_index] = QueryPlanUtil.getSortedPlanFragments(catalog_stmt, false); 
                }
                plan.frag_list[stmt_index] = this.sorted_multip_fragments[stmt_index];
            }
            
            plan.readonly = plan.readonly && catalog_stmt.getReadonly();
            plan.localFragsAreNonTransactional = plan.localFragsAreNonTransactional || stmt_localFragsAreNonTransactional;
            plan.all_singlesited = plan.all_singlesited && is_singlepartition;
            plan.all_local = plan.all_local && is_local;

            // Keep track of whether the current query in the batch was single-partitioned or not
            plan.singlepartition_bitmap[stmt_index] = is_singlepartition;
            
            // ----------------------
            // DEBUG DUMP
            // ----------------------
            if (debug.get()) {
                @SuppressWarnings("unchecked")
                Map maps[] = new Map[fragments.size() + 1];
                int ii = 0;
                for (PlanFragment catalog_frag : fragments) {
                    Map<String, Object> m = new ListOrderedMap<String, Object>();
                    Set<Integer> p = plan.frag_partitions[stmt_index].get(catalog_frag);
                    boolean frag_local = (p.size() == 1 && p.contains(base_partition));
                    m.put(String.format("[%02d] Fragment", ii), catalog_frag.fullName());
                    m.put(String.format("     Partitions"), p);
                    m.put(String.format("     IsLocal"), frag_local);
                    ii++;
                    maps[ii] = m;
                } // FOR

                Map<String, Object> header = new ListOrderedMap<String, Object>();
                header.put("Batch Statement#", String.format("%02d / %02d", stmt_index, this.batchSize));
                header.put("Catalog Statement", catalog_stmt.fullName());
                header.put("Statement SQL", catalog_stmt.getSqltext());
                header.put("All Partitions", plan.stmt_partitions[stmt_index]);
                header.put("Local Partition", base_partition);
                header.put("IsSingledSited", is_singlepartition);
                header.put("IsStmtLocal", is_local);
                header.put("IsBatchLocal", plan.all_local);
                header.put("Fragments", fragments.size());
                maps[0] = header;

                LOG.debug("\n" + StringUtil.formatMapsBoxed(maps));
            }
        } // FOR
        
        // Check whether we have an existing graph exists for this batch configuration
        PlanGraph graph = null;
        int bitmap_hash = Arrays.hashCode(plan.singlepartition_bitmap);
        synchronized (this) {
            graph = this.plan_graphs.get(bitmap_hash);
            if (graph == null) {
                graph = this.buildPlanGraph(plan);
                this.plan_graphs.put(bitmap_hash, graph);
            }
        } // SYNCHRONIZED
        this.buildFragmentTaskMessages(plan, graph, batchArgs);
        
        if (debug.get()) LOG.debug("Created BatchPlan:\n" + plan.toString());
        return (plan);
    }

    /**
     * Construct a map from PartitionId->FragmentTaskMessage
     * Note that a FragmentTaskMessage may contain multiple fragments that need to be executed
     * @param txn_id
     * @return
     */
    private void buildFragmentTaskMessages(final BatchPlanner.BatchPlan plan, final PlanGraph graph, final ParameterSet[] batchArgs) {
        long txn_id = plan.txn_id;
        long client_handle = plan.client_handle;
        if (debug.get()) LOG.debug("Constructing list of FragmentTaskMessages to execute [txn_id=#" + txn_id + ", base_partition=" + plan.base_partition + "]");
//        try {
//            GraphVisualizationPanel.createFrame(this.plan_graph).setVisible(true);
//            Thread.sleep(100000000);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            System.exit(1);
//        }

        plan.ftasks.clear();
        plan.rounds_length = graph.max_rounds;
        for (int i = 0; i < plan.rounds_length; i++) {
            for (Set<PlanVertex> s : plan.rounds[i]) {
                s.clear();
            } // FOR
        } // FOR
        
        for (PlanVertex v : graph.getVertices()) {
            int stmt_index = v.stmt_index;
            PlanFragment catalog_frag = v.getCatalogItem();
            Set<Integer> partitions = plan.frag_partitions[stmt_index].get(catalog_frag);
            for (int p : partitions) {
                plan.rounds[v.round][p].add(v);
            } // FOR
        } // FOR
        
        // Pre-compute Statement Parameter ByteBuffers
        for (int stmt_index = 0; stmt_index < this.batchSize; stmt_index++) {
            try {
                batchArgs[stmt_index].writeExternal(plan.param_serializers[stmt_index]);
                plan.param_buffers[stmt_index] = plan.param_serializers[stmt_index].getBuffer();
            } catch (Exception ex) {
                LOG.fatal("Failed to serialize parameters for Statement #" + stmt_index, ex);
                System.exit(1);
            }
            assert(plan.param_buffers[stmt_index] != null) : "Parameter ByteBuffer is null for Statement #" + stmt_index;
        } // FOR
        
        if (trace.get()) LOG.trace("Generated " + plan.rounds_length + " rounds of tasks for txn #"+ txn_id);
        for (int round = 0; round < plan.rounds_length; round++) {
            if (trace.get()) LOG.trace(String.format("Txn #%d - Round %02d", txn_id, round)); //  + " - Round " + e.getKey() + ": " + e.getValue().size() + " partitions");

            for (int partition = 0; partition < this.num_partitions; partition++) {
                Set<PlanVertex> vertices = plan.rounds[round][partition];
                if (vertices.isEmpty()) continue;
            
                int num_frags = vertices.size();
                long frag_ids[] = new long[num_frags];
                int input_ids[] = new int[num_frags];
                int output_ids[] = new int[num_frags];
                int stmt_indexes[] = new int[num_frags];
                ByteBuffer params[] = new ByteBuffer[num_frags];
        
                int i = 0;
                for (PlanVertex v : vertices) { // Does this order matter?
                    // Fragment Id
                    frag_ids[i] = v.frag_id;
                    
                    // Not all fragments will have an input dependency
                    input_ids[i] = (v.input_dependency_id == null ? ExecutionSite.NULL_DEPENDENCY_ID : v.input_dependency_id);
                    
                    // All fragments will produce some output
                    output_ids[i] = v.output_dependency_id;
                    
                    // SQLStmt Index
                    stmt_indexes[i] = v.stmt_index;
                    
                    // Parameters
                    params[i] = plan.param_buffers[v.stmt_index];
                    
                    if (trace.get())
                        LOG.trace("Fragment Grouping " + i + " => [" +
                                   "txn_id=#" + txn_id + ", " +
                                   "frag_id=" + frag_ids[i] + ", " +
                                   "input=" + input_ids[i] + ", " +
                                   "output=" + output_ids[i] + ", " +
                                   "stmt_indexes=" + stmt_indexes[i] + "]");
                    
                    i += 1;
                } // FOR (frag_idx)
            
                if (i == 0) {
                    if (trace.get()) {
                        LOG.warn("For some reason we thought it would be a good idea to construct a FragmentTaskMessage with no fragments! [txn_id=#" + txn_id + "]");
                        LOG.warn("In case you were wondering, this is a terrible idea, which is why we didn't do it!");
                    }
                    continue;
                }
            
                FragmentTaskMessage task = new FragmentTaskMessage(
                        plan.base_partition.intValue(),
                        partition,
                        txn_id,
                        client_handle,
                        false, // IGNORE
                        frag_ids,
                        input_ids,
                        output_ids,
                        params,
                        stmt_indexes,
                        false); // FIXME(pavlo) Final task?
                task.setFragmentTaskType(BatchPlanner.this.catalog_proc.getSystemproc() ? FragmentTaskMessage.SYS_PROC_PER_PARTITION : FragmentTaskMessage.USER_PROC);
                plan.ftasks.add(task);
                
                if (debug.get()) {
                    LOG.debug(String.format("New FragmentTaskMessage to run at partition #%d with %d fragments for txn #%d " +
                                            "[ids=%s, inputs=%s, outputs=%s]",
                                            partition, num_frags, txn_id,
                                            Arrays.toString(frag_ids), Arrays.toString(input_ids), Arrays.toString(output_ids)));
                    if (trace.get()) LOG.trace("Fragment Contents: [txn_id=#" + txn_id + "]\n" + task.toString());
                }
            } // PARTITION
        } // ROUND            
        assert(plan.ftasks.size() > 0) : "Failed to generate any FragmentTaskMessages in this BatchPlan for txn #" + txn_id;
        if (debug.get()) LOG.debug("Created " + plan.ftasks.size() + " FragmentTaskMessage(s) for txn #" + txn_id);
    }

    
    /**
     * Construct 
     * @param plan
     * @return
     */
    private PlanGraph buildPlanGraph(BatchPlanner.BatchPlan plan) {
        PlanGraph graph = new PlanGraph(CatalogUtil.getDatabase(this.catalog_proc));

        graph.max_rounds = 0;
        for (int stmt_index = 0; stmt_index < this.batchSize; stmt_index++) {
            Map<PlanFragment, Set<Integer>> frag_partitions = plan.frag_partitions[stmt_index]; 
            assert(frag_partitions != null) : "No Fragment->PartitionIds map for Statement #" + stmt_index;
            List<PlanFragment> fragments = plan.frag_list[stmt_index]; 
            assert(fragments != null);
            graph.max_rounds = Math.max(fragments.size(), graph.max_rounds);
            
            // Generate the synthetic DependencyIds for the query
            Integer last_output_id = null;
            for (int round = 0, cnt = fragments.size(); round < cnt; round++) {
                PlanFragment catalog_frag = fragments.get(round);
                Set<Integer> f_partitions = frag_partitions.get(catalog_frag);
                assert(f_partitions != null) : String.format("No PartitionIds for [%02d] %s in Statement #%d", round, catalog_frag.fullName(), stmt_index);
                boolean f_local = (f_partitions.size() == 1 && f_partitions.contains(plan.base_partition));
                int output_id = BatchPlanner.NEXT_DEPENDENCY_ID.getAndIncrement();
    
                PlanVertex v = new PlanVertex(catalog_frag,
                                              stmt_index,
                                              round,
                                              last_output_id,
                                              output_id,
                                              f_local);
                graph.addVertex(v);
                last_output_id = output_id;
            }
        } // FOR

        for (PlanVertex v0 : graph.getVertices()) {
            Integer input_id = v0.input_dependency_id;
            if (input_id == null) continue;
            for (PlanVertex v1 : graph.getOutputDependencies(input_id)) {
                assert(!v0.equals(v1)) : v0;
                if (!graph.findEdgeSet(v0, v1).isEmpty()) continue;
                PlanEdge e = new PlanEdge(graph, input_id);
                graph.addEdge(e, v0, v1);
            } // FOR
        } // FOR
        

        return (graph);
    }

    /**
     * List of DependencyIds that need to be satisfied before we return control
     * back to the Java control code
     * @return
     */
    public List<Integer> getDependencyIdsNeededToResume() {
        return (this.depsToResume);
    }

}
