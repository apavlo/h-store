package edu.brown.markov;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.*;
import edu.brown.utils.*;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * 
 * @author pavlo
 * 
 */
public class TransactionEstimator {
    private static final Logger LOG = Logger.getLogger(TransactionEstimator.class.getName());

    /**
     * The amount of change in visitation of vertices we would tolerate before we need to recompute the graph.
     * TODO (pavlo): Saurya says: Should this be in MarkovGraph?
     */
    private static final double RECOMPUTE_TOLERANCE = (double) 0.5;

    private transient Database catalog_db;
    private transient int num_partitions;
    private transient PartitionEstimator p_estimator;
    private transient ParameterCorrelations correlations;
    private transient boolean enable_recomputes = false;
    
    private final HashMap<Procedure, MarkovGraph> procedure_graphs = new HashMap<Procedure, MarkovGraph>();
    private final transient Map<Long, State> xact_states = new ConcurrentHashMap<Long, State>();
    private final AtomicInteger xact_count = new AtomicInteger(0); 
    
    /**
     * The current state of a transaction
     */
    public final class State {
        private final int base_partition;
        private final long start_time;
        private final MarkovGraph markov;
        private final List<Vertex> estimated_path;
        private final double estimated_path_confidence;
        private final List<Vertex> actual_path = new ArrayList<Vertex>();
        private final Set<Integer> touched_partitions = new HashSet<Integer>();
        private final Map<Statement, Integer> query_instance_cnts = new HashMap<Statement, Integer>();
        
        private Vertex current;
        private Estimate last_estimate;

        /**
         * Constructor
         * @param markov - the graph that this txn is using
         * @param estimated_path - the initial path estimation from MarkovPathEstimator
         */
        public State(int base_partition, MarkovGraph markov, List<Vertex> estimated_path, double confidence) {
            this.base_partition = base_partition;
            this.markov = markov;
            this.start_time = System.currentTimeMillis();
            this.estimated_path = estimated_path;
            this.estimated_path_confidence = confidence;
            this.setCurrent(markov.getStartVertex());
        }
        
        public MarkovGraph getMarkovGraph() {
            return (this.markov);
        }
        
        public int getBasePartition() {
            return (this.base_partition);
        }
        
        /**
         * Get the number of milli-seconds that have passed since the txn started
         * @return
         */
        public long getExecutionTimeOffset() {
            return (System.currentTimeMillis() - this.start_time);
        }
        
        public int updateQueryInstanceCount(Statement catalog_stmt) {
            Integer cnt = this.query_instance_cnts.get(catalog_stmt);
            if (cnt == null) cnt = 0;
            this.query_instance_cnts.put(catalog_stmt, cnt + 1);
            return (cnt);
        }
        
        public Set<Integer> getTouchedPartitions() {
            return (this.touched_partitions);
        }
        public void addTouchedPartitions(Collection<Integer> partitions) {
            this.touched_partitions.addAll(partitions);
        }

        public List<Vertex> getEstimatedPath() {
            return (this.estimated_path);
        }
        public double getEstimatedPathConfidence() {
            return (this.estimated_path_confidence);
        }

        public List<Vertex> getActualPath() {
            return (this.actual_path);
        }

        public Vertex getCurrent() {
            return (this.current);
        }

        /**
         * Set the current vertex for this transaction and update the actual path
         * @param current
         */
        public void setCurrent(Vertex current) {
            if (this.current != null) assert(this.current.equals(current) == false);
            this.actual_path.add(current);
            this.current = current;
        }

        public long getStartTime() {
            return this.start_time;
        }

        public Estimate getLastEstimate() {
            return this.last_estimate;
        }

        public void setLastEstimate(Estimate last_estimate) {
            this.last_estimate = last_estimate;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{")
              .append("Current=").append(this.current).append(", ")
              .append("Touched=").append(this.touched_partitions)
              .append("}");
            return sb.toString();
        }
    } // END CLASS

    /**
     * An estimation for a transaction given its current state
     */
    public final class Estimate {
        // Global
        private double singlepartition;
        private double userabort;

        // Partition-specific
        private final double finished[];
        private final double read[];
        private final double write[];
        private Long time;

        public Estimate() {
            this.finished = new double[TransactionEstimator.this.num_partitions];
            this.read = new double[TransactionEstimator.this.num_partitions];
            this.write = new double[TransactionEstimator.this.num_partitions];
        }

        // ----------------------------------------------------------------------------
        // Convenience methods using EstimationThresholds object
        // ----------------------------------------------------------------------------
        public boolean isSinglePartition(EstimationThresholds t) {
            return (this.singlepartition >= t.getSinglePartitionThreshold());
        }
        public boolean isUserAbort(EstimationThresholds t) {
            return (this.userabort >= t.getAbortThreshold());
        }
        public boolean isReadOnlyPartition(EstimationThresholds t, int partition_idx) {
            return (this.read[partition_idx] >= t.getReadThreshold());
        }
        public boolean isWritePartition(EstimationThresholds t, int partition_idx) {
            return (this.write[partition_idx] >= t.getWriteThreshold());
        }
        public boolean isFinishedPartition(EstimationThresholds t, int partition_idx) {
            return (this.finished[partition_idx] >= t.getDoneThreshold());
        }
        public boolean isTargetPartition(EstimationThresholds t, int partition_idx) {
            return ((1 - this.finished[partition_idx]) >= t.getDoneThreshold());
        }

        public double getReadOnlyProbablity(int partition_idx) {
            return (this.read[partition_idx]);
        }

        public double getWriteProbability(int partition_idx) {
            return (this.write[partition_idx]);
        }

        public double getFinishedProbability(int partition_idx) {
            return (this.finished[partition_idx]);
        }

        public long getExecutionTime() {
            return time;
        }
        
        private Set<Integer> getPartitions(double values[], double limit, boolean inverse) {
            Set<Integer> ret = new HashSet<Integer>();
            for (int i = 0; i < values.length; i++) {
                if (inverse) {
                    if ((1 - values[i]) >= limit) ret.add(i);
                } else {
                    if (values[i] >= limit) ret.add(i);
                }
            } // FOR
            return (ret);
        }

        public Set<Integer> getReadOnlyPartitions(EstimationThresholds t) {
            return (this.getPartitions(this.read, t.getReadThreshold(), false));
        }

        public Set<Integer> getWritePartitions(EstimationThresholds t) {
            return (this.getPartitions(this.write, t.getWriteThreshold(), false));
        }

        public Set<Integer> getFinishedPartitions(EstimationThresholds t) {
            return (this.getPartitions(this.finished, t.getDoneThreshold(), false));
        }
        
        public Set<Integer> getTargetPartitions(EstimationThresholds t) {
            return (this.getPartitions(this.finished, t.getDoneThreshold(), true));
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            final String f = "%.02f"; 
            
            sb.append("Single-Partition: ").append(String.format(f, this.singlepartition)).append("\n");
            sb.append("User Abort:       ").append(String.format(f, this.userabort)).append("\n");
            sb.append("\n");
            
            sb.append("              \tRead\tWrite\tFinish\n");
            for (int i = 0; i < TransactionEstimator.this.num_partitions; i++) {
                sb.append(String.format("Partition %02d:\t", i));
                sb.append(String.format(f, this.read[i])).append("\t");
                sb.append(String.format(f, this.write[i])).append("\t");
                sb.append(String.format(f, this.finished[i])).append("\n");
            } // FOR
            return (sb.toString());
        }
    } // END CLASS

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * @param p_estimator
     * @param correlations
     */
    public TransactionEstimator(PartitionEstimator p_estimator, ParameterCorrelations correlations) {
//        this.base_partition = base_partition;
        this.p_estimator = p_estimator;
        this.catalog_db = this.p_estimator.getDatabase();
        this.num_partitions = CatalogUtil.getNumberOfPartitions(this.catalog_db);
        this.correlations = (correlations == null ? new ParameterCorrelations() : correlations);
    }

    /**
     * Constructor
     * 
     * @param catalog_db
     */
    public TransactionEstimator(int base_partition, PartitionEstimator p_estimator) {
        this(p_estimator, null);
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------

    public void enableGraphRecomputes() {
       this.enable_recomputes = true;
    }
    
    public ParameterCorrelations getCorrelations() {
        return this.correlations;
    }

//    public int getBasePartition() {
//        return this.base_partition;
//    }

    public PartitionEstimator getPartitionEstimator() {
        return this.p_estimator;
    }

    public void addMarkovGraphs(Set<MarkovGraph> markovs) {
        for (MarkovGraph m : markovs) {
            this.addMarkovGraph(m.getProcedure(), m);
        } // FOR
    }
    
    public void addMarkovGraphs(Map<Procedure, MarkovGraph> markovs) {
        this.procedure_graphs.putAll(markovs);
    }

    public void addMarkovGraph(Procedure catalog_proc, MarkovGraph graph) {
        this.procedure_graphs.put(catalog_proc, graph);
    }

    public MarkovGraph getMarkovGraph(Procedure catalog_proc) {
        return (this.procedure_graphs.get(catalog_proc));
    }

    public MarkovGraph getMarkovGraph(String catalog_key) {
        return (this.getMarkovGraph(CatalogKey.getFromKey(this.catalog_db, catalog_key, Procedure.class)));
    }
    
    /**
     * Return the internal State object for the given transaction id
     * @param txn_id
     * @return
     */
    public State getState(long txn_id) {
        State s = this.xact_states.get(txn_id);
        assert(s != null) : "Unexpected Transaction #" + txn_id;
        return (s);
    }
    
    /**
     * Return the initial path estimation for the given transaction id
     * @param txn_id
     * @return
     */
    protected List<Vertex> getInitialPath(long txn_id) {
        State s = this.xact_states.get(txn_id);
        assert(s != null) : "Unexpected Transaction #" + txn_id;
        return (s.getEstimatedPath());
    }
    protected double getConfidence(long txn_id) {
        State s = this.xact_states.get(txn_id);
        assert(s != null) : "Unexpected Transaction #" + txn_id;
        return (s.getEstimatedPathConfidence());
    }

    // ----------------------------------------------------------------------------
    // RUNTIME METHODS
    // ----------------------------------------------------------------------------

    
    /**
     * Returns true if we will be able to calculate estimations for the given Procedure
     * 
     * @param catalog_proc
     * @return
     */
    public boolean canEstimate(Procedure catalog_proc) {
        return (catalog_proc.getSystemproc() == false && procedure_graphs.containsKey(catalog_proc));
    }
    
    /**
     * Sets up the beginning of a transaction. Returns an estimate of where this
     * transaction will go.
     * 
     * @param xact_id
     * @param catalog_proc
     * @param BASE_PARTITION
     * @return an estimate for the transaction's future
     */
    public Estimate startTransaction(long xact_id, Procedure catalog_proc, Object args[]) {
        assert (catalog_proc != null);

        // If we don't have a graph for this procedure, we should probably just return null
        // This will be the case for all sysprocs
        if (!procedure_graphs.containsKey(catalog_proc)) {
            LOG.debug("No MarkovGraph exists for '" + catalog_proc + "'"); //  on partition #" + this.base_partition);
            return (null);
            // fillIn(estimate,xact_states.get(xact_id));
            // return estimate;
        }

        MarkovGraph graph = this.procedure_graphs.get(catalog_proc);
        assert (graph != null);
        // ??? graph.resetCounters();

        Vertex start = graph.getStartVertex();
        start.addInstanceTime(xact_id, System.currentTimeMillis());
        
        Integer base_partition = null; 
        try {
            base_partition = this.p_estimator.getBasePartition(catalog_proc, args);
        } catch (Exception ex) {
            LOG.fatal(String.format("Failed to calculate base partition for <%s, %s>", catalog_proc.getName(), Arrays.toString(args)), ex);
            System.exit(1);
        }
        assert(base_partition != null);
        
        // Calculate initial path estimate
        MarkovPathEstimator path_estimate = null;
        try {
            path_estimate = this.estimatePath(graph, base_partition, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert(path_estimate != null);
        
        State state = new State(base_partition, graph, path_estimate.getVisitPath(), path_estimate.getConfidence());
        this.xact_states.put(xact_id, state);
        Estimate estimate = this.generateEstimate(state);
        
        this.xact_count.incrementAndGet();
        return (estimate);
    }

    /**
     * Takes a series of queries and executes them in order given the partition
     * information. Provides an estimate of where the transaction might go next.
     * @param xact_id
     * @param catalog_stmts
     * @param partitions
     * @return
     */
    public Estimate executeQueries(long xact_id, Statement catalog_stmts[], Integer partitions[][]) {
        assert (catalog_stmts.length == partitions.length);       
        State state = this.xact_states.get(xact_id);
        if (state == null) {
            String msg = "No state information exists for txn #" + xact_id;
            LOG.debug(msg);
            return (null);
            // throw new RuntimeException(msg);
        }
        
        // Roll through the Statements in this batch and move the current vertex
        // for the txn's State handle along the path in the MarkovGraph
        for (int i = 0; i < catalog_stmts.length; i++) {
            List<Integer> stmt_partitions = new ArrayList<Integer>();
            for (int p : partitions[i]) stmt_partitions.add(p);
            this.consume(xact_id, state, catalog_stmts[i], stmt_partitions);
        } // FOR
        
        // Now 
        Estimate estimate = this.generateEstimate(state);
        assert(estimate != null);
        
        // Once the workload shifts we detect it and trigger this method. Recomputes
        // the graph with the data we collected with the current workload method.
        if (this.enable_recomputes && state.getMarkovGraph().shouldRecompute(this.xact_count.get(), RECOMPUTE_TOLERANCE)) {
            state.getMarkovGraph().recomputeGraph();
        }
        return (estimate);
    }

    /**
     * The transaction with provided xact_id is finished
     * @param xact_id finished transaction
     */
    public State commit(long xact_id) {
        return (this.completeTransaction(xact_id, Vertex.Type.COMMIT));
    }

    /**
     * The transaction with provided xact_id has aborted
     * @param xact_id
     */
    public State abort(long xact_id) {
        return (this.completeTransaction(xact_id, Vertex.Type.ABORT));
    }

    /**
     * The transaction for the given xact_id is in limbo, so we just want to remove it
     * @param xact_id
     * @return
     */
    public State ignore(long xact_id) {
        // We can just remove its state and pass it back
        // We don't care if it's valid or not
        return (this.xact_states.remove(xact_id));
    }
    
    /**
     * 
     * @param xact_id
     * @param vtype
     * @return
     */
    private State completeTransaction(long xact_id, Vertex.Type vtype) {
        State s = this.xact_states.remove(xact_id);
        if (s == null) {
            String msg = "No state information exists for txn #" + xact_id;
            LOG.debug(msg);
            return (null);
        }
        
        // We need to update the counter information in our MarkovGraph so that we know
        // that the procedure may transition to the ABORT vertex from where ever it was before 
        MarkovGraph g = s.getMarkovGraph();
        Vertex next_v = g.getSpecialVertex(vtype);
        assert(next_v != null) : "Missing " + vtype;
        s.setCurrent(next_v);
        
        // If no edge exists to the next vertex, then we need to create one
        Edge next_e = g.findEdge(s.getCurrent(), next_v);
        if (next_e == null) {
            next_e = g.addToEdge(s.getCurrent(), next_v);
        }
        assert(next_e != null);
        
        // Update counters
        next_v.incrementInstancehits();
        next_v.addInstanceTime(xact_id, s.getExecutionTimeOffset());
        next_e.incrementInstancehits();

        return (s);
    }

    public void removeTransaction(long txn_id) {
        this.xact_states.remove(txn_id);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL ESTIMATION METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Given an empty estimate object and the current Vertex, we fill in the
     * relevant information for the transaction coordinator to use.
     * @param estimate
     *            - the Estimate object which will be filled in
     * @param current
     *            - the Vertex we are currently at in the MarkovGraph
     */
    protected Estimate generateEstimate(State state) {
        Estimate estimate = new Estimate();
        Vertex current = state.getCurrent();
        estimate.singlepartition = current.getSingleSitedProbability();
        estimate.userabort = current.getAbortProbability();
        estimate.time = current.getExecutiontime();
        for (int i : CatalogUtil.getAllPartitionIds(this.catalog_db)) {
            estimate.finished[i] = current.getDoneProbability(i);
            estimate.read[i] = current.getReadOnlyProbability(i);
            estimate.write[i] = current.getWriteProbability(i);
        } // FOR
        state.setLastEstimate(estimate);
        return (estimate);
    }

    /**
     * At the start of a transaction, estimate the execution path that the
     * transaction will likely take during its lifetime
     * 
     * @param markov
     * @param args
     * @throws Exception
     */
    protected MarkovPathEstimator estimatePath(final MarkovGraph markov, final int base_partition, final Object args[]) throws Exception {
        MarkovPathEstimator estimator = new MarkovPathEstimator(markov, this, base_partition, args);
        estimator.traverse(markov.getStartVertex());
        return (estimator);
    }
    
    /**
     * Figure out the next vertex that the txn will transition to for the give Statement catalog object
     * and the partitions that it will touch when it is executed. If no vertex exists, we will create
     * it and dynamically add it to our MarkovGraph
     * @param xact_id
     * @param state
     * @param catalog_stmt
     * @param partitions
     */
    protected void consume(long xact_id, State state, Statement catalog_stmt, Collection<Integer> partitions) {
        final boolean trace = LOG.isTraceEnabled(); 
        
        // Update the number of times that we have executed this query in the txn
        int queryInstanceIndex = state.updateQueryInstanceCount(catalog_stmt);
        
        MarkovGraph g = state.getMarkovGraph();
        assert(g != null);
        
        // Examine all of the vertices that are adjacent to our current vertex
        // and see which vertex we are going to move to next
        Collection<Edge> edges = g.getOutEdges(state.getCurrent()); 
        if (trace) LOG.trace("Examining " + edges.size() + " edges from " + state.getCurrent() + " for Txn #" + xact_id);
        Vertex next_v = null;
        Edge next_e = null;
        for (Edge e : edges) {
            Vertex v = g.getDest(e);
            if (v.isEqual(catalog_stmt, partitions, state.getTouchedPartitions(), queryInstanceIndex)) {
                if (trace) LOG.trace("Found next vertex " + v + " for Txn #" + xact_id);
                next_v = v;
                next_e = e;
                break;
            }
        } // FOR
        
        // If we fail to find the next vertex, that means we have to dynamically create a new 
        // one. The graph is self-managed, so we don't need to worry about whether 
        // we need to recompute probabilities.
        if (next_v == null) {
            next_v = new Vertex(catalog_stmt,
                                Vertex.Type.QUERY,
                                queryInstanceIndex,
                                partitions,
                                state.getTouchedPartitions());
            g.addVertex(next_v);
            next_e = g.addToEdge(state.getCurrent(), next_v);
            if (trace) LOG.trace("Created new edge/vertex from " + state.getCurrent() + " for Txn #" + xact_id);
        }

        // Update the counters and other info for the next vertex and edge
        next_v.addInstanceTime(xact_id, state.getExecutionTimeOffset());
        next_v.incrementInstancehits();
        next_e.incrementInstancehits();
        
        // Update the state information
        state.setCurrent(next_v);
        state.addTouchedPartitions(partitions);
        if (trace) LOG.trace("Updated State Information for Txn #" + xact_id + ": " + state);
    }

    // ----------------------------------------------------------------------------
    // HELPER METHODS
    // ----------------------------------------------------------------------------
    
    public State processTransactionTrace(TransactionTrace txn_trace) throws Exception {
        long txn_id = txn_trace.getTransactionId();
        Estimate last_est = this.startTransaction(txn_id, txn_trace.getCatalogItem(this.catalog_db), txn_trace.getParams());
        assert(last_est != null);
        State s = this.getState(txn_id);
        assert(s != null);
        for (Entry<Integer, List<QueryTrace>> e : txn_trace.getQueryBatches().entrySet()) {
            int batch_size = e.getValue().size();
            
            // Generate the data structures we will need to give to the TransactionEstimator
            Statement catalog_stmts[] = new Statement[batch_size];
            Integer partitions[][] = new Integer[batch_size][];
            for (int i = 0; i < batch_size; i++) {
                QueryTrace query_trace = e.getValue().get(i);
                assert(query_trace != null);
                catalog_stmts[i] = query_trace.getCatalogItem(catalog_db);
                
                Set<Integer> stmt_partitions = this.p_estimator.getAllPartitions(query_trace, s.getBasePartition());
                assert(stmt_partitions.isEmpty() == false);
                partitions[i] = stmt_partitions.toArray(new Integer[stmt_partitions.size()]);
            } // FOR
            
            last_est = this.executeQueries(txn_id, catalog_stmts, partitions);
        } // FOR (batches)
        return (txn_trace.isAborted() ? this.abort(txn_id) : this.commit(txn_id));
    }
    
    
    // ----------------------------------------------------------------------------
    // YE OLDE MAIN METHOD
    // ----------------------------------------------------------------------------
    
    /**
     * Utility method for constructing a TransactionEstimator object per
     * partition in
     * 
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD);
        
        // First construct all of the MarkovGraphs
        final PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        MarkovGraphsContainer graphs_per_partition = MarkovUtil.createBasePartitionGraphs(args.catalog_db, args.workload, p_estimator);
        assert(graphs_per_partition != null);
        
        // Then construct a TransactionEstimator per partition/procedure
        // TODO(pavlo): Do we need to be able to serialize a TransactionEstimator?
        // System.err.println("Number of graphs: " + MarkovUtil.load(args.catalog_db, args.getParam(ArgumentsParser.PARAM_MARKOV), CatalogUtil.getAllPartitions(args.catalog_db)).get(0).size());
        
    }

}