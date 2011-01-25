package org.voltdb;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.*;
import org.voltdb.messaging.*;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.IGraph;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.graphs.VertexTreeWalker.TraverseOrder;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;

/**
 * @author pavlo
 */
public class BatchPlanner {
    private static final Logger LOG = Logger.getLogger(BatchPlanner.class);
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected static final AtomicInteger NEXT_DEPENDENCY_ID = new AtomicInteger(1000);
    
    // Used for turning ParameterSets into ByteBuffers
    protected final FastSerializer fs = new FastSerializer();
    
    // the set of dependency ids for the expected results of the batch
    // one per sql statment
    protected final ArrayList<Integer> depsToResume = new ArrayList<Integer>();

    protected final Catalog catalog;
    protected final Procedure catalog_proc;
    protected final Statement catalog_stmts[];
    protected final SQLStmt[] batchStmts;
    protected final int batchSize;
    protected final PartitionEstimator p_estimator;
    protected final int initiator_id;

    private class PlanVertex extends AbstractVertex {
        final Integer frag_id;
        final Integer input_dependency_id;
        final Integer output_dependency_id;
        final ParameterSet params;
        final Integer partition;
        final int stmt_index;
        final int hash; 

        public PlanVertex(PlanFragment catalog_frag,
                          Integer frag_id,
                          Integer input_dependency_id,
                          Integer output_dependency_id,
                          ParameterSet params,
                          Integer partition,
                          int stmt_index,
                          boolean is_local) {
            super(catalog_frag);
            this.frag_id = frag_id;
            this.input_dependency_id = input_dependency_id;
            this.output_dependency_id = output_dependency_id;
            this.params = params;
            this.partition = partition;
            this.stmt_index = stmt_index;
            
            this.hash = (catalog_frag.hashCode() * 31) +  this.partition.hashCode();
        }
        
        @Override
        public int hashCode() {
            return (this.hash);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PlanVertex)) return (false);
            PlanVertex other = (PlanVertex)obj;
            if (this.input_dependency_id != other.input_dependency_id ||
                this.output_dependency_id != other.output_dependency_id ||
                this.params.equals(other.params) != true ||
                this.partition.equals(other.partition) != true ||
                this.stmt_index != other.stmt_index ||
                this.getCatalogItem().equals(other.getCatalogItem()) != true    
            ) return (false);
            return (true);
        }
        
        @Override
        public String toString() {
            return String.format("<%s [Partition#%02d]>", this.getCatalogItem().getName(), this.partition);
        }
    }
    
    private class PlanEdge extends AbstractEdge {
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
    
    private class PlanGraph extends AbstractDirectedGraph<PlanVertex, PlanEdge> {
        private static final long serialVersionUID = 1L;
        
        private final Map<Integer, Set<PlanVertex>> output_dependency_xref = new HashMap<Integer, Set<PlanVertex>>();
        
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
        
        public Set<PlanVertex> getOutputDependencies(int output_id) {
            return (this.output_dependency_xref.get(output_id));
        }
        
    }
    
    
    
    /**
     * BatchPlan
     */
    public class BatchPlan {
        private final int local_partition;

        private final PlanGraph plan_graph;
        private boolean plan_graph_ready = false;

        private final Set<PlanVertex> local_fragments = new HashSet<PlanVertex>();
        private final Set<PlanVertex> remote_fragments = new HashSet<PlanVertex>();
        
        
        // Whether the fragments of this batch plan consist of read-only operations
        protected boolean readonly = true;
        
        // Whether the batch plan can all be executed locally
        protected boolean all_local = true;
        
        // Whether the fragments in the batch plan can be executed on a single site
        protected boolean all_singlesited = true;    

        // check if all local fragment work is non-transactional
        protected boolean localFragsAreNonTransactional = true;
        
        // Partition -> FragmentIdx
        protected Map<Integer, ListOrderedSet<Integer>> partition_frag_xref = new HashMap<Integer, ListOrderedSet<Integer>>(); 
        
        // Statement Target Partition Ids
        protected final int[][] stmt_partition_ids = new int[BatchPlanner.this.batchSize][];
        
        /**
         * Constructor
         */
        public BatchPlan(int local_partition) {
            this.local_partition = local_partition;
            this.plan_graph = new PlanGraph(CatalogUtil.getDatabase(catalog));
        }
        
        /**
         * Construct a map from PartitionId->FragmentTaskMessage
         * Note that a FragmentTaskMessage may contain multiple fragments that need to be executed
         * @param txn_id
         * @return
         */
        public List<FragmentTaskMessage> getFragmentTaskMessages(long txn_id, long clientHandle) {
            assert(this.plan_graph_ready);
            final boolean debug = LOG.isDebugEnabled();
            final boolean trace = LOG.isTraceEnabled();
            if (debug) LOG.debug("Constructing list of FragmentTaskMessages to execute [txn_id=#" + txn_id + ", local_partition=" + local_partition + "]");
//            try {
//                GraphVisualizationPanel.createFrame(this.plan_graph).setVisible(true);
//                Thread.sleep(100000000);
//            } catch (Exception ex) {
//                ex.printStackTrace();
//                System.exit(1);
//            }
            
            // FIXME Map<Integer, ByteBuffer> buffer_params = new HashMap<Integer, ByteBuffer>(this.allFragIds.size());

            
            // We need to generate a list of FragmentTaskMessages that we will ship off to either
            // remote execution sites or be executed locally. Note that we have to separate
            // any tasks that have a input dependency from those that don't,  because
            // we can only block at the FragmentTaskMessage level (as opposed to blocking by PlanFragment)
            final List<FragmentTaskMessage> ftasks = new ArrayList<FragmentTaskMessage>();
            
            // Round# -> Map<PartitionId, Set<PlanFragments>>
            final TreeMap<Integer, Map<Integer, Set<PlanVertex>>> rounds = new TreeMap<Integer, Map<Integer, Set<PlanVertex>>>();
            assert(!this.plan_graph.getRoots().isEmpty()) : this.plan_graph.getRoots();
            final List<PlanVertex> roots = new ArrayList<PlanVertex>(this.plan_graph.getRoots());
            for (PlanVertex root : roots) {
                new VertexTreeWalker<PlanVertex>(this.plan_graph, TraverseOrder.LONGEST_PATH) {
                    @Override
                    protected void callback(PlanVertex element) {
                        Integer round = null;
                        // If the current element is one of the roots, then we want to put it in a separate
                        // round so that it can be executed without needing all of the other input to come back first
                        // 2010-07-26: NO! For now because we always have to dispatch multi-partition fragments from the coordinator,
                        // then we can't combine the fragments for the local partition together. They always need
                        // to be sent our serially. Yes, I know it's lame but go complain Evan and get off my case!
                        //if (roots.contains(element)) {
                        //    round = -1 - roots.indexOf(element);
                        //} else {
                            round = this.getDepth();
                        //}
                        
                        Integer partition = element.partition;
                        if (!rounds.containsKey(round)) {
                            rounds.put(round, new HashMap<Integer, Set<PlanVertex>>());
                        }
                        if (!rounds.get(round).containsKey(partition)) {
                            rounds.get(round).put(partition, new HashSet<PlanVertex>());
                        }
                        rounds.get(round).get(partition).add(element);
                    }
                }.traverse(root);
            } // FOR
            
            if (trace) LOG.trace("Generated " + rounds.size() + " rounds of tasks for txn #"+ txn_id);
            for (Entry<Integer, Map<Integer, Set<PlanVertex>>> e : rounds.entrySet()) {
                if (trace) LOG.trace("Txn #" + txn_id + " - Round " + e.getKey() + ": " + e.getValue().size() + " partitions");
                for (Integer partition : e.getValue().keySet()) {
                    Set<PlanVertex> vertices = e.getValue().get(partition);
                
                    int num_frags = vertices.size();
                    long frag_ids[] = new long[num_frags];
                    int input_ids[] = new int[num_frags];
                    int output_ids[] = new int[num_frags];
                    int stmt_indexes[] = new int[num_frags];
                    ByteBuffer params[] = new ByteBuffer[num_frags];
            
                    int i = 0;
                    for (PlanVertex v : vertices) {
                        assert(v.partition.equals(partition));
                        
                        // Fragment Id
                        frag_ids[i] = v.frag_id;
                        
                        // Not all fragments will have an input dependency
                        input_ids[i] = (v.input_dependency_id == null ? ExecutionSite.NULL_DEPENDENCY_ID : v.input_dependency_id);
                        
                        // All fragments will produce some output
                        output_ids[i] = v.output_dependency_id;
                        
                        // SQLStmt Index
                        stmt_indexes[i] = v.stmt_index;
                        
                        // Parameters
                        params[i] = null; // FIXME buffer_params.get(v);
                        if (params[i] == null) {
                            try {
                                FastSerializer fs = new FastSerializer();
                                v.params.writeExternal(fs);
                                params[i] = fs.getBuffer();
                            } catch (Exception ex) {
                                LOG.fatal("Failed to serialize parameters for FragmentId #" + frag_ids[i], ex);
                                System.exit(1);
                            }
                            if (trace) LOG.trace("Stored ByteBuffer for " + v);
                            // FIXME buffer_params.put(frag_idx, params[i]);
                        }
                        assert(params[i] != null) : "Parameter ByteBuffer is null for partition #" + v.partition + " fragment index #" + i + "\n"; //  + buffer_params;
                        
                        if (trace)
                            LOG.trace("Fragment Grouping " + i + " => [" +
                                       "txn_id=#" + txn_id + ", " +
                                       "frag_id=" + frag_ids[i] + ", " +
                                       "input=" + input_ids[i] + ", " +
                                       "output=" + output_ids[i] + ", " +
                                       "stmt_indexes=" + stmt_indexes[i] + "]");
                        
                        i += 1;
                    } // FOR (frag_idx)
                
                    if (i == 0) {
                        if (trace) {
                            LOG.warn("For some reason we thought it would be a good idea to construct a FragmentTaskMessage with no fragments! [txn_id=#" + txn_id + "]");
                            LOG.warn("In case you were wondering, this is a terrible idea, which is why we didn't do it!");
                        }
                        continue;
                    }
                
                    FragmentTaskMessage task = new FragmentTaskMessage(
                            BatchPlanner.this.initiator_id,
                            partition,
                            txn_id,
                            clientHandle,
                            false, // IGNORE
                            frag_ids,
                            input_ids,
                            output_ids,
                            params,
                            stmt_indexes,
                            false); // FIXME(pavlo) Final task?
                    task.setFragmentTaskType(BatchPlanner.this.catalog_proc.getSystemproc() ? FragmentTaskMessage.SYS_PROC_PER_PARTITION : FragmentTaskMessage.USER_PROC);
                    if (debug) LOG.debug("New FragmentTaskMessage to run at partition #" + partition + " with " + num_frags + " fragments for txn #" + txn_id + " " + 
                                         "[ids=" + Arrays.toString(frag_ids) + ", inputs=" + Arrays.toString(input_ids) + ", outputs=" + Arrays.toString(output_ids) + "]");
                    if (trace) LOG.trace("Fragment Contents: [txn_id=#" + txn_id + "]\n" + task.toString());
                    ftasks.add(task);
                    
                } // PARTITION
            } // ROUND            
            assert(ftasks.size() > 0) : "Failed to generate any FragmentTaskMessages in this BatchPlan for txn #" + txn_id;
            if (debug) LOG.debug("Created " + ftasks.size() + " FragmentTaskMessage(s) for txn #" + txn_id);
            return (ftasks);
        }
        
        protected void buildPlanGraph() {
            assert(this.plan_graph.getVertexCount() > 0);
            
            for (PlanVertex v0 : this.plan_graph.getVertices()) {
                Integer input_id = v0.input_dependency_id;
                if (input_id == null) continue;
                for (PlanVertex v1 : this.plan_graph.getOutputDependencies(input_id)) {
                    assert(!v0.equals(v1)) : v0;
                    if (!this.plan_graph.findEdgeSet(v0, v1).isEmpty()) continue;
                    PlanEdge e = new PlanEdge(this.plan_graph, input_id);
                    this.plan_graph.addEdge(e, v0, v1);
                } // FOR
            } // FOR
            this.plan_graph_ready = true;
        }
        
        /**
         * Adds a new FragmentId that needs to be executed on some number of partitions
         * @param frag_id
         * @param output_dependency_id
         * @param params
         * @param partitions
         * @param is_local
         */
        public void addFragment(PlanFragment catalog_frag,
                                Integer input_dependency_id,
                                Integer output_dependency_id,
                                ParameterSet params,
                                Set<Integer> partitions,
                                int stmt_index,
                                boolean is_local) {
            this.plan_graph_ready = false;
            int frag_id = Integer.parseInt(catalog_frag.getName());
            if (LOG.isTraceEnabled())
                LOG.trace("New Fragment: [" +
                            "frag_id=" + frag_id + ", " +
                            "output_dep_id=" + output_dependency_id + ", " +
                            "input_dep_id=" + input_dependency_id + ", " +
                            "params=" + params + ", " +
                            "partitons=" + partitions + ", " +
                            "stmt_index=" + stmt_index + ", " +
                            "is_local=" + is_local + "]");
                           
            for (Integer partition : partitions) {
                PlanVertex v = new PlanVertex(catalog_frag,
                                              frag_id,
                                              input_dependency_id,
                                              output_dependency_id,
                                              params,
                                              partition,
                                              stmt_index,
                                              is_local);
                this.plan_graph.addVertex(v);
                if (is_local) {
                    this.local_fragments.add(v);
                } else {
                    this.remote_fragments.add(v);
                }
            }
        }
        
        public int getBatchSize() {
            return (BatchPlanner.this.batchSize);
        }
        
        public Statement[] getStatements() {
            return (BatchPlanner.this.catalog_stmts);
        }
        
        public int[][] getStatementPartitions() {
            return (this.stmt_partition_ids);
        }
        
        /**
         * 
         * @return
         */
        public int getRemoteFragmentCount() {
            return (this.remote_fragments.size());
        }
        
        /**
         * 
         * @return
         */
        public int getLocalFragmentCount() {
            return (this.local_fragments.size());
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
             .append("# of Fragments:   ").append(this.plan_graph.getVertexCount()).append("\n")
             .append("------------------------------\n");

            /*
            for (int i = 0, cnt = this.allFragIds.size(); i < cnt; i++) {
                int frag_id = this.allFragIds.get(i);
                Set<Integer> partitions = this.allFragPartitions.get(i);
                ParameterSet params = this.allParams.get(i);
                b.append("  [" + i + "] ")
                  .append("FragId=" + frag_id + ", ")
                  .append("Partitions=" + partitions + ", ")
                  .append("Params=[" + params + "]\n");
            } // FOR
            */
            return (b.toString());
        }
    } // END CLASS
    

    
    /**
     * Constructor
     */
    protected BatchPlanner(SQLStmt[] batchStmts, Procedure catalog_proc, PartitionEstimator p_estimator, int initiator_id) {
        this(batchStmts, batchStmts.length, catalog_proc, p_estimator, initiator_id);
        
    }
    
    /**
     * Constructor
     * @param batchStmts
     * @param batchSize
     * @param catalog_proc
     * @param p_estimator
     * @param local_partition
     */
    public BatchPlanner(SQLStmt[] batchStmts, int batchSize, Procedure catalog_proc, PartitionEstimator p_estimator, int initiator_id) {
        assert(catalog_proc != null);
        assert(p_estimator != null);

        this.batchStmts = batchStmts;
        this.batchSize = batchSize;
        this.catalog_proc = catalog_proc;
        this.catalog = catalog_proc.getCatalog();
        this.p_estimator = p_estimator;
        this.initiator_id = initiator_id;
        
        this.catalog_stmts = new Statement[this.batchSize];
        for (int i = 0; i < this.batchSize; i++) {
            this.catalog_stmts[i] = this.batchStmts[i].catStmt;
            assert(this.catalog_stmts[i] != null);
        } // FOR
    }
   
    /**
     * 
     * @param batchArgs
     */
    public BatchPlan plan(ParameterSet[] batchArgs, int local_partition) {
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();
        
        if (debug) LOG.debug("Constructing a new BatchPlan for " + this.catalog_proc);
        BatchPlan plan = new BatchPlan(local_partition);
        
        for (int stmt_index = 0; stmt_index < this.batchSize; ++stmt_index) {
            final SQLStmt stmt = this.batchStmts[stmt_index];
            assert(stmt != null) : "The SQLStmt object at index " + stmt_index + " is null for " + this.catalog_proc;
            final Statement catalog_stmt = stmt.catStmt;
            final ParameterSet paramSet = batchArgs[stmt_index];
            if (trace) LOG.trace("Constructing fragment plans for " + stmt.catStmt);
            
            plan.readonly = plan.readonly && catalog_stmt.getReadonly();

            // ----------------------
            // DEBUG DUMP
            // ----------------------
            StringBuilder buffer = null;
            boolean singlesited_plan = catalog_stmt.getHas_singlesited();
            Set<Integer> all_partitions = new HashSet<Integer>();
            
            ArrayList<PlanFragment> stmt_frags = new ArrayList<PlanFragment>();
            ArrayList<Set<Integer>> stmt_partitions = new ArrayList<Set<Integer>>();
            ArrayList<ParameterSet> stmt_params = new ArrayList<ParameterSet>();
            ArrayList<Set<Integer>> stmt_input_dependencies = new ArrayList<Set<Integer>>();
            ArrayList<Set<Integer>> stmt_output_dependencies = new ArrayList<Set<Integer>>();
            boolean stmt_localFragsAreNonTransactional = plan.localFragsAreNonTransactional;
            
            //
            // This is where we will figure out whether this Statement needs to be executed on
            // the local partition or blasted out to multiple partitions
            // We will loop through once and look at the single-partition plan
            // If that fails, then we will loop back around and grab the multi-partition plan
            //
            Object params[] = paramSet.toArray();
            while (true) {
                CatalogMap<PlanFragment> fragments = (singlesited_plan ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
                buffer = new StringBuilder();
                if (!all_partitions.isEmpty()) {
                    if (trace) LOG.trace("Planning using multi-partition fragments");
                    all_partitions.clear();
                    stmt_frags.clear();
                    stmt_params.clear();
                    stmt_partitions.clear();
                } else {
                    if (trace) LOG.trace("Attempting to plan using single-partition fragments");
                }
              
                // Estimate what partition each fragment needs to be sent to
                int ii = 0;
                for (PlanFragment catalog_frag : fragments) {
                    Set<Integer> frag_partitions = null;
                    try {
                        frag_partitions = this.p_estimator.getPartitions(catalog_frag, params, local_partition);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    // If we didn't get back any partitions, then we know that it has to be executed locally
                    // In the future we could do some tricks like sending this Fragment over to somebody
                    // that has the most partitions on the same machine to minimize network traffic.
                    if (frag_partitions.isEmpty()) frag_partitions.add(local_partition);
                    
                    all_partitions.addAll(frag_partitions);
                    stmt_frags.add(catalog_frag);
                    stmt_params.add(paramSet);
                    stmt_partitions.add(frag_partitions);
                    
                    // If any frag is transactional, update this check
                    if (catalog_frag.getNontransactional() == false) {
                        stmt_localFragsAreNonTransactional = true;
                    }
                                        
                    // ----------------------
                    // DEBUG DUMP
                    // ----------------------
                    if (debug) {
                        if (ii > 0) buffer.append("\n");
                        boolean frag_local = (frag_partitions.size() == 1 && frag_partitions.contains(local_partition));
                        buffer.append("   Fragment[" + ii + "]:   " + catalog_frag.getName() + "\n");
                        buffer.append("   Partitions[" + ii + "]: " + frag_partitions + "\n");
                        buffer.append("   IsLocal[" + ii + "]:    " + frag_local + "\n");
                        
                        try {
                            AbstractPlanNode root = QueryPlanUtil.deserializePlanFragment(catalog_frag);
                            buffer.append("   Plan[" + ii + "]:\n").append(PlanNodeUtil.debug(root));
                        } catch (Exception ex) {
                            LOG.fatal("Failed to deserialize PlanNode for " + catalog_stmt, ex);
                            System.exit(1);
                        }
                    }
                    ii++;
                } // FOR
                if (all_partitions.size() == 1 || !singlesited_plan) break;
                singlesited_plan = false;
            } // WHILE
            plan.localFragsAreNonTransactional = plan.localFragsAreNonTransactional || stmt_localFragsAreNonTransactional;
            boolean is_singlesited = (all_partitions.size() == 1);
            plan.all_singlesited = plan.all_singlesited && is_singlesited;
            boolean is_local = (is_singlesited && all_partitions.contains(local_partition));
            plan.all_local = plan.all_local && is_local;
            
            // Generate the synthetic DependencyIds for the query
            try {
                BatchPlanner.generateDependencyIds(stmt_frags, stmt_input_dependencies, stmt_output_dependencies);
            } catch (Exception ex) {
                LOG.fatal("Failed to generate synthetic DependencyIds for " + catalog_stmt, ex);
                System.exit(1);
            }
            
            // Update the Statement->PartitionId array
            // This is needed by TransactionEstimator
            plan.stmt_partition_ids[stmt_index] = new int[all_partitions.size()];
            int idx = 0;
            for (int partition_id : all_partitions) {
                plan.stmt_partition_ids[stmt_index][idx++] = partition_id;
            }
            
            // SPECIAL CASE: Local INSERT/UPDATE/DELETE queries don't have an output dependency id
            // but we still need to block the VoltProcedure so that it waits until the operations
            // are finished and they can get back the # of tuples modified
//            if (is_local && stmt.catStmt.getQuerytype() != QueryType.SELECT.getValue()) {
//                LOG.info("Attempting to add local dependency for " + stmt_fragIds);
//                assert(stmt_output_dependencies.size() == 1);
//                assert(stmt_output_dependencies.get(0).isEmpty());
//                stmt_output_dependencies.get(0).add(--local_dependency_ctr);
//            } else {
//                LOG.info("Plan does not require local dependency ids for " + stmt_fragIds);
//                LOG.info("all_partitions=" + all_partitions);
//                LOG.info("local_partition=" + local_partition);
//            }
            
            for (int ii = 0, cnt = stmt_frags.size(); ii < cnt; ii++) {
                Set<Integer> frag_partitions = stmt_partitions.get(ii);
                ParameterSet frag_params = stmt_params.get(ii);
                Set<Integer> frag_input_dependency_ids = stmt_input_dependencies.get(ii);
                assert(frag_input_dependency_ids.size() <= 1);
                Set<Integer> frag_output_dependency_ids = stmt_output_dependencies.get(ii);
                assert(frag_output_dependency_ids.size() <= 1);
                
                boolean frag_local = (frag_partitions.size() == 1 && frag_partitions.contains(local_partition));
                plan.addFragment(
                        stmt_frags.get(ii), 
                        CollectionUtil.getFirst(frag_input_dependency_ids),
                        CollectionUtil.getFirst(frag_output_dependency_ids),
                        frag_params,
                        frag_partitions,
                        stmt_index,
                        frag_local);
            } // FOR
                        
            if (trace) {
                LOG.trace("BatchPlanner Output " + stmt.catStmt + "\n" +
                          "Batch[" + stmt_index + "]: " + stmt.getText() + "\n" +
                          "Initiator Id:     " + this.initiator_id + "\n" + 
                          "All Partitions:   " + all_partitions + "\n" +
                          "Local Partition:  " + local_partition + "\n" +
                          "IsSingledSited:   " + is_singlesited + "\n" +
                          "IsStmtLocal:      " + is_local + "\n" +
                          "IsBatchLocal:     " + plan.all_local + "\n" +
                          "Fragments:        " + stmt_frags.size() + "\n" +
                          // buffer + "\n" +
                          "--------------------------------\n");
            }
        } // FOR (SQLStmt)
        plan.buildPlanGraph();
        
        if (debug) LOG.debug("Created BatchPlan:\n" + plan.toString());
        return (plan);
    }

    /**
     * For the given PlanFragments, generate the dependency ids between them
     * @param stmt_frags
     * @param stmt_input_dependencies
     * @param stmt_output_dependencies
     */
    protected static void generateDependencyIds(List<PlanFragment> stmt_frags, List<Set<Integer>> stmt_input_dependencies, List<Set<Integer>> stmt_output_dependencies) throws Exception {
        for (int i = 0, cnt = stmt_frags.size(); i < cnt; i++) {
            stmt_input_dependencies.add(new HashSet<Integer>());
            stmt_output_dependencies.add(new HashSet<Integer>());
        } // FOR
        
        Integer last_output_id = null;
        for (PlanFragment catalog_frag : QueryPlanUtil.getSortedPlanFragments(stmt_frags)) {
            int idx = stmt_frags.indexOf(catalog_frag);
            assert(idx >= 0);
            
            Integer output_id = NEXT_DEPENDENCY_ID.getAndIncrement();
            stmt_output_dependencies.get(idx).add(output_id);
            if (last_output_id != null) {
                stmt_input_dependencies.get(idx).add(last_output_id);
            }
            last_output_id = output_id;
        } // FOR
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
