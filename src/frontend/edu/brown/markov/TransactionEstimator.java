package edu.brown.markov;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;

import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.*;
import edu.brown.graphs.GraphvizExport;
import edu.brown.utils.*;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * 
 * @author pavlo
 */
public class TransactionEstimator {
    private static final Logger LOG = Logger.getLogger(TransactionEstimator.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // STATIC DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The amount of change in visitation of vertices we would tolerate before we need to recompute the graph.
     * TODO (pavlo): Saurya says: Should this be in MarkovGraph?
     */
    private static final double RECOMPUTE_TOLERANCE = (double) 0.5;

    private static final ObjectPool ESTIMATOR_POOL = new StackObjectPool(new MarkovPathEstimator.Factory());
    
    private static ObjectPool STATE_POOL; 
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    private final Database catalog_db;
    private final int num_partitions;
    private final PartitionEstimator p_estimator;
    private final ParameterCorrelations correlations;
    private final MarkovGraphsContainer markovs;
    private final Map<Long, State> txn_states = new ConcurrentHashMap<Long, State>();
    private final AtomicInteger txn_count = new AtomicInteger(0);
    
    private transient boolean enable_recomputes = false;
    
    // ----------------------------------------------------------------------------
    // TRANSACTION STATE
    // ----------------------------------------------------------------------------
    
    /**
     * The current state of a transaction
     */
    public static final class State implements Poolable {
        private final List<Vertex> actual_path = new ArrayList<Vertex>();
        private final Set<Integer> touched_partitions = new HashSet<Integer>();
        private final Map<Statement, Integer> query_instance_cnts = new HashMap<Statement, Integer>();
        private final List<Estimate> estimates = new ArrayList<Estimate>();
        private final int num_partitions;

        private long txn_id;
        private int base_partition;
        private long start_time;
        private MarkovGraph markov;
        private MarkovPathEstimator initial_estimator;
        private int num_estimates;
        
        private transient Vertex current;

        /**
         * State Factory
         */
        public static class Factory extends BasePoolableObjectFactory {
            private int num_partitions;
            
            public Factory(int num_partitions) {
                this.num_partitions = num_partitions;
            }
            
            @Override
            public Object makeObject() throws Exception {
                State s = new State(this.num_partitions);
                return s;
            }
            public void passivateObject(Object obj) throws Exception {
                State s = (State)obj;
                s.finish();
            };
        };
        
        /**
         * Constructor
         * @param markov - the graph that this txn is using
         * @param estimated_path - the initial path estimation from MarkovPathEstimator
         */
        private State(int num_partitions) {
            this.num_partitions = num_partitions;
        }
        
        public void init(long txn_id, int base_partition, MarkovGraph markov, MarkovPathEstimator initial_estimator, long start_time) {
            this.txn_id = txn_id;
            this.base_partition = base_partition;
            this.markov = markov;
            this.start_time = start_time;
            this.initial_estimator = initial_estimator;
            this.setCurrent(markov.getStartVertex());    
        }
        
        @Override
        public void finish() {
            // Return the MarkovPathEstimator
            try {
                TransactionEstimator.ESTIMATOR_POOL.returnObject(this.getInitialEstimator());
            } catch (Exception ex) {
                throw new RuntimeException("Failed to return MarkovPathEstimator for txn" + this.txn_id, ex);
            }
            
            this.actual_path.clear();
            this.touched_partitions.clear();
            this.query_instance_cnts.clear();
            this.current = null;
            
            // We maintain a local cache of Estimates, so there is no pool to return them to
            for (int i = 0; i < this.num_estimates; i++) {
                this.estimates.get(i).finish();
            } // FOR
            this.num_estimates = 0;
        }
        
        /**
         * Get the next Estimate object for this State
         * @return
         */
        protected synchronized Estimate getNextEstimate(Vertex v) {
            Estimate next = null;
            if (this.num_estimates < this.estimates.size()) {
                next = this.estimates.get(this.num_estimates);
            } else {
                next = new Estimate(this.num_partitions);
                this.estimates.add(next);
            }
            next.init(v, this.num_estimates++);
            return (next);
        }

        public long getTransactionId() {
            return (this.txn_id);
        }
        public long getStartTime() {
            return this.start_time;
        }
        public MarkovGraph getMarkovGraph() {
            return (this.markov);
        }
        public int getBasePartition() {
            return (this.base_partition);
        }
        public int getEstimateCount() {
            return (this.num_estimates);
        }
        public List<Estimate> getEstimates() {
            return (this.estimates);
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
        
        /**
         * Get the number of milli-seconds that have passed since the txn started
         * @return
         */
        public long getExecutionTimeOffset() {
            return (System.currentTimeMillis() - this.start_time);
        }
        
        public long getExecutionTimeOffset(long stop) {
            return (stop - this.start_time);
        }
        
        public int updateQueryInstanceCount(Statement catalog_stmt) {
            Integer cnt = this.query_instance_cnts.get(catalog_stmt);
            if (cnt == null) cnt = 0;
            this.query_instance_cnts.put(catalog_stmt, cnt.intValue() + 1);
            return (cnt.intValue());
        }
        
        public Set<Integer> getEstimatedPartitions() {
            return (this.initial_estimator.getAllPartitions());
        }
        public Set<Integer> getEstimatedReadPartitions() {
            return (this.initial_estimator.getReadPartitions());
        }
        public Set<Integer> getEstimatedWritePartitions() {
            return (this.initial_estimator.getWritePartitions());
        }
        private MarkovPathEstimator getInitialEstimator() {
            return (this.initial_estimator);
        }
        public List<Vertex> getEstimatedPath() {
            return (this.initial_estimator.getVisitPath());
        }
        public double getEstimatedPathConfidence() {
            return (this.initial_estimator.getConfidence());
        }
        
        public Set<Integer> getTouchedPartitions() {
            return (this.touched_partitions);
        }
        public void addTouchedPartitions(Collection<Integer> partitions) {
            this.touched_partitions.addAll(partitions);
        }
        public List<Vertex> getActualPath() {
            return (this.actual_path);
        }

        /**
         * Return the initial Estimate made for this transaction before it began execution
         * @return
         */
        public Estimate getInitialEstimate() {
            return (this.num_estimates > 0 ? this.estimates.get(0) : null);
        }

        public Estimate getLastEstimate() {
            return this.estimates.get(this.num_estimates-1);
        }
        
        @Override
        public String toString() {
            Map<String, Object> m0 = new ListOrderedMap<String, Object>();
            m0.put("TransactionId", this.txn_id);
            m0.put("Procedure", this.markov.getProcedure().getName());
            
            Map<String, Object> m1 = new ListOrderedMap<String, Object>();
            m1.put("Initial Partitions", this.getEstimatedPartitions());
            m1.put("Initial Confidence", this.getEstimatedPathConfidence());
            m1.put("Initial Estimate", this.getInitialEstimate().vertex.debug());
            
            Map<String, Object> m2 = new ListOrderedMap<String, Object>();
            m2.put("Actual Partitions", this.getTouchedPartitions());
            m2.put("Current Estimate", this.current.debug());
            
            return StringUtil.formatMaps(m0, m1, m2);
        }
    } // END CLASS

    // ----------------------------------------------------------------------------
    // TRANSACTION ESTIMATE
    // ----------------------------------------------------------------------------
    
    /**
     * An estimation for a transaction given its current state
     */
    public static final class Estimate implements Poolable {
        // Global
        private double singlepartition;
        private double userabort;

        // Partition-specific
        private final double finished[];
        private Set<Integer> finished_partitions;
        private Set<Integer> target_partitions;
        
        private final double read[];
        private Set<Integer> read_partitions;
        
        private final double write[];
        private Set<Integer> write_partitions;
 
        private transient Vertex vertex;
        private transient int batch;
        private transient Long time;

        private Estimate(int num_partitions) {
            this.finished = new double[num_partitions];
            this.read = new double[num_partitions];
            this.write = new double[num_partitions];
        }
        
        /**
         * Given an empty estimate object and the current Vertex, we fill in the
         * relevant information for the transaction coordinator to use.
         * @param estimate the Estimate object which will be filled in
         * @param v the Vertex we are currently at in the MarkovGraph
         */
        public Estimate init(Vertex v, int batch) {
            this.batch = batch;
            this.vertex = v;
            
            this.singlepartition = v.getSingleSitedProbability();
            this.userabort = v.getAbortProbability();
            this.time = v.getExecutiontime();
            for (int i = 0; i < this.write.length; i++) {
                this.finished[i] = v.getDoneProbability(i);
                this.read[i] = v.getReadOnlyProbability(i);
                this.write[i] = v.getWriteProbability(i);
            } // FOR
            return (this);
        }
        
        @Override
        public void finish() {
            this.finished_partitions.clear();
            this.target_partitions.clear();
            this.read_partitions.clear();
            this.write_partitions.clear();
        }
        
        /**
         * The last vertex in this batch
         * @return
         */
        public Vertex getVertex() {
            return vertex;
        }
        
        /**
         * Return that BatchId for this Estimate
         * @return
         */
        public int getBatchId() {
            return (this.batch);
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
        
        private void getPartitions(Set<Integer> partitions, double values[], double limit, boolean inverse) {
            partitions.clear();
            for (int i = 0; i < values.length; i++) {
                if (inverse) {
                    if ((1 - values[i]) >= limit) partitions.add(i);
                } else {
                    if (values[i] >= limit) partitions.add(i);
                }
            } // FOR
        }

        /**
         * Get the partitions that this transaction will only read from
         * @param t
         * @return
         */
        public Set<Integer> getReadOnlyPartitions(EstimationThresholds t) {
            if (this.read_partitions == null) this.read_partitions = new HashSet<Integer>();
            this.getPartitions(this.read_partitions, this.read, t.getReadThreshold(), false);
            return (this.read_partitions);
        }
        /**
         * Get the partitions that this transaction will write to
         * @param t
         * @return
         */
        public Set<Integer> getWritePartitions(EstimationThresholds t) {
            if (this.write_partitions == null) this.write_partitions = new HashSet<Integer>();
            this.getPartitions(this.write_partitions, this.write, t.getWriteThreshold(), false);
            return (this.write_partitions);
        }
        /**
         * Get the partitions that this transaction is finished with at this point in the transaction
         * @param t
         * @return
         */
        public Set<Integer> getFinishedPartitions(EstimationThresholds t) {
            if (this.finished_partitions == null) this.finished_partitions = new HashSet<Integer>();
            this.getPartitions(this.finished_partitions, this.finished, t.getDoneThreshold(), false);
            return (this.finished_partitions);
        }
        /**
         * Get the partitions that this transaction will need to read/write data on 
         * @param t
         * @return
         */
        public Set<Integer> getTargetPartitions(EstimationThresholds t) {
            if (this.target_partitions == null) this.target_partitions = new HashSet<Integer>();
            this.getPartitions(this.target_partitions, this.finished, t.getDoneThreshold(), true);
            return (this.target_partitions);
        }
        
        @Override
        public String toString() {
            final String f = "%-6.02f"; 
            
            Map<String, Object> m0 = new ListOrderedMap<String, Object>();
            m0.put("Batch Estimate", "#" + this.batch);
            m0.put("Vertex", this.vertex);
            m0.put("Single-Partition", String.format(f, this.singlepartition));
            m0.put("User Abort", String.format(f, this.userabort));
            
            Map<String, Object> m1 = new ListOrderedMap<String, Object>();
            m1.put("", String.format("%-6s%-6s%-6s", "Read", "Write", "Finish"));
            for (int i = 0; i < this.read.length; i++) {
                String val = String.format(f + f + f, this.read[i], this.write[i], this.finished[i]);
                m1.put(String.format("Partition %02d", i), val);
            } // FOR
            return (StringUtil.formatMapsBoxed(m0, m1));
        }
    } // END CLASS

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * @param p_estimator
     * @param correlations
     * @param markovs
     */
    public TransactionEstimator(PartitionEstimator p_estimator, ParameterCorrelations correlations, MarkovGraphsContainer markovs) {
        this.p_estimator = p_estimator;
        this.markovs = markovs;
        this.catalog_db = this.p_estimator.getDatabase();
        this.num_partitions = CatalogUtil.getNumberOfPartitions(this.catalog_db);
        this.correlations = (correlations == null ? new ParameterCorrelations() : correlations);
        
        // HACK: Initialize the STATE_POOL
        synchronized (LOG) {
            if (STATE_POOL == null) {
                STATE_POOL = new StackObjectPool(new State.Factory(this.num_partitions));
            }
        } // SYNC
    }

    /**
     * Constructor
     * 
     * @param catalog_db
     */
    public TransactionEstimator(int base_partition, PartitionEstimator p_estimator) {
        this(p_estimator, null, new MarkovGraphsContainer());
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------

    public static ObjectPool getStatePool() {
        return (STATE_POOL);
    }
    
    public void enableGraphRecomputes() {
       this.enable_recomputes = true;
    }
    
    public ParameterCorrelations getCorrelations() {
        return this.correlations;
    }

    public PartitionEstimator getPartitionEstimator() {
        return this.p_estimator;
    }

    protected MarkovGraphsContainer getMarkovs() {
        return (this.markovs);
    }
    
    public void addMarkovGraphs(MarkovGraphsContainer markovs) {
        this.markovs.copy(markovs);
    }
    
    /**
     * Return the internal State object for the given transaction id
     * @param txn_id
     * @return
     */
    public State getState(long txn_id) {
        return (this.txn_states.get(txn_id));
    }
    
    /**
     * Returns true if this TransactionEstimator is following a transaction
     * @param txn_id
     * @return
     */
    public boolean hasState(long txn_id) {
        return (this.txn_states.containsKey(txn_id));
    }
    
    /**
     * Return the initial path estimation for the given transaction id
     * @param txn_id
     * @return
     */
    protected List<Vertex> getInitialPath(long txn_id) {
        State s = this.txn_states.get(txn_id);
        assert(s != null) : "Unexpected Transaction #" + txn_id;
        return (s.getEstimatedPath());
    }
    protected double getConfidence(long txn_id) {
        State s = this.txn_states.get(txn_id);
        assert(s != null) : "Unexpected Transaction #" + txn_id;
        return (s.getEstimatedPathConfidence());
    }
    
    // ----------------------------------------------------------------------------
    // RUNTIME METHODS
    // ----------------------------------------------------------------------------
   
    /**
     * Sets up the beginning of a transaction. Returns an estimate of where this
     * transaction will go.
     * 
     * @param txn_id
     * @param catalog_proc
     * @param BASE_PARTITION
     * @return an estimate for the transaction's future
     */
    public State startTransaction(long txn_id, Procedure catalog_proc, Object args[]) {
        Integer base_partition = null; 
        try {
            base_partition = this.p_estimator.getBasePartition(catalog_proc, args);
        } catch (Exception ex) {
            LOG.fatal(String.format("Failed to calculate base partition for <%s, %s>", catalog_proc.getName(), Arrays.toString(args)), ex);
            System.exit(1);
        }
        assert(base_partition != null);
        
        return (this.startTransaction(txn_id, base_partition.intValue(), catalog_proc, args));
    }
        
    /**
     * 
     * @param txn_id
     * @param base_partition
     * @param catalog_proc
     * @param args
     * @return
     */
    public State startTransaction(long txn_id, int base_partition, Procedure catalog_proc, Object args[]) {
        assert (catalog_proc != null);
        long start_time = System.currentTimeMillis();

        // If we don't have a graph for this procedure, we should probably just return null
        // This will be the case for all sysprocs
        if (this.markovs == null) return (null);
        MarkovGraph markov = this.markovs.getFromParams(txn_id, base_partition, args, catalog_proc);
        if (markov == null) {
            if (debug.get()) LOG.debug(String.format("No %s MarkovGraph exists for txn #%d", catalog_proc.getName(), txn_id));
            return (null);
        }
        
        Vertex start = markov.getStartVertex();
        MarkovPathEstimator estimator = null;
        try {
            estimator = (MarkovPathEstimator)ESTIMATOR_POOL.borrowObject();
            estimator.init(markov, this, base_partition, args);
            estimator.enableForceTraversal(true);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        // Calculate initial path estimate
        if (trace.get()) LOG.trace("Estimating initial execution path for txn #" + txn_id);
        synchronized (markov) {
            start.addInstanceTime(txn_id, start_time);
            try {
                estimator.traverse(start);
            } catch (Throwable e) {
                LOG.fatal("Failed to estimate path", e);
                try {
                    GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, false, markov.getPath(estimator.getVisitPath()));
                    System.err.println("GRAPH DUMP: " + gv.writeToTempFile(catalog_proc));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                throw new RuntimeException(e);
            }
        } // SYNCH
        assert(estimator != null);

        if (trace.get()) LOG.trace("Creating new State txn #" + txn_id);
        State state = null;
        try {
            state = (State)STATE_POOL.borrowObject();
            state.init(txn_id, base_partition, markov, estimator, start_time);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        State old = this.txn_states.put(txn_id, state);
        assert(old == null);

        // The initial estimate should be based on the second-to-last vertex in the initial path estimation
        List<Vertex> initial_path = estimator.getVisitPath();
        int path_size = initial_path.size();
        if (trace.get()) LOG.trace(String.format("Found initial execution path of length %d for txn #%d", path_size, txn_id));
        if (path_size > 0) {
            int idx = 1;
            Vertex last_v = null;
            do {
                last_v = initial_path.get(path_size - idx);
                idx++;
            } while ((last_v.isQueryVertex() == false) && (idx < path_size));
            assert(last_v != null);

            if (last_v.isQueryVertex() == false) {
                LOG.warn(String.format("Failed to find a query vertex in %s for txn #%d", catalog_proc.getName(), txn_id));
            } else {
                Estimate est = state.getNextEstimate(last_v);
                assert(est != null);
                assert(est.getBatchId() == 0);
            }
        }
        
        this.txn_count.incrementAndGet();
        return (state);
    }

    /**
     * Takes a series of queries and executes them in order given the partition
     * information. Provides an estimate of where the transaction might go next.
     * @param xact_id
     * @param catalog_stmts
     * @param partitions
     * @return
     */
    public Estimate executeQueries(long xact_id, Statement catalog_stmts[], Set<Integer> partitions[]) {
        assert (catalog_stmts.length == partitions.length);       
        State state = this.txn_states.get(xact_id);
        if (state == null) {
            if (debug.get()) {
                String msg = "No state information exists for txn #" + xact_id;
                LOG.debug(msg);
            }
            return (null);
        }
        return (this.executeQueries(state, catalog_stmts, partitions));
    }
    
    /**
     * 
     * @param state
     * @param catalog_stmts
     * @param partitions
     * @return
     */
    public Estimate executeQueries(State state, Statement catalog_stmts[], Set<Integer> partitions[]) {
        // Roll through the Statements in this batch and move the current vertex
        // for the txn's State handle along the path in the MarkovGraph
        synchronized (state.getMarkovGraph()) {
            for (int i = 0; i < catalog_stmts.length; i++) {
                this.consume(state, catalog_stmts[i], partitions[i]);
            } // FOR
        } // SYNCH
        
        Estimate estimate = state.getNextEstimate(state.current);
        assert(estimate != null);
        
        // Once the workload shifts we detect it and trigger this method. Recomputes
        // the graph with the data we collected with the current workload method.
        if (this.enable_recomputes && state.getMarkovGraph().shouldRecompute(this.txn_count.get(), RECOMPUTE_TOLERANCE)) {
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
        return (this.txn_states.remove(xact_id));
    }
    
    /**
     * 
     * @param txn_id
     * @param vtype
     * @return
     */
    private State completeTransaction(long txn_id, Vertex.Type vtype) {
        State s = this.txn_states.remove(txn_id);
        if (s == null) {
            if (trace.get()) LOG.warn("No state information exists for txn #" + txn_id);
            return (null);
        }
        long start_time = System.currentTimeMillis();
        
        // We need to update the counter information in our MarkovGraph so that we know
        // that the procedure may transition to the ABORT vertex from where ever it was before 
        MarkovGraph g = s.getMarkovGraph();
        Vertex current = s.getCurrent();
        Vertex next_v = g.getSpecialVertex(vtype);
        assert(next_v != null) : "Missing " + vtype;
        Edge next_e = null;
        
        // If no edge exists to the next vertex, then we need to create one
        synchronized (g) {
            next_e = g.findEdge(current, next_v);
            if (next_e == null) next_e = g.addToEdge(current, next_v);

            // Update counters
            next_v.incrementInstancehits();
            next_v.addInstanceTime(txn_id, s.getExecutionTimeOffset(start_time));
            next_e.incrementInstancehits();
        } // SYNCH
        assert(next_e != null);
        s.setCurrent(next_v); // In case somebody wants to do post-processing...
        
        return (s);
    }

    public void removeTransaction(long txn_id) {
        this.txn_states.remove(txn_id);
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL ESTIMATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Figure out the next vertex that the txn will transition to for the give Statement catalog object
     * and the partitions that it will touch when it is executed. If no vertex exists, we will create
     * it and dynamically add it to our MarkovGraph
     * @param xact_id
     * @param state
     * @param catalog_stmt
     * @param partitions
     */
    protected void consume(State state, Statement catalog_stmt, Collection<Integer> partitions) {
        // Update the number of times that we have executed this query in the txn
        int queryInstanceIndex = state.updateQueryInstanceCount(catalog_stmt);
        
        MarkovGraph g = state.getMarkovGraph();
        assert(g != null);
        
        // Examine all of the vertices that are adjacent to our current vertex
        // and see which vertex we are going to move to next
        Vertex current = state.getCurrent();
        assert(current != null);
        Vertex next_v = null;
        Edge next_e = null;

        // Synchronize on the single vertex so that it's more fine-grained than the entire graph
        Collection<Edge> edges = g.getOutEdges(current); 
        if (trace.get()) LOG.trace("Examining " + edges.size() + " edges from " + current + " for Txn #" + state.txn_id);
        for (Edge e : edges) {
            Vertex v = g.getDest(e);
            if (v.isEqual(catalog_stmt, partitions, state.getTouchedPartitions(), queryInstanceIndex)) {
                if (trace.get()) LOG.trace("Found next vertex " + v + " for Txn #" + state.txn_id);
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
            next_e = g.addToEdge(current, next_v);
            if (trace.get()) LOG.trace("Created new edge/vertex from " + state.getCurrent() + " for Txn #" + state.txn_id);
        }

        // Update the counters and other info for the next vertex and edge
        next_v.addInstanceTime(state.txn_id, state.getExecutionTimeOffset());
        next_v.incrementInstancehits();
        next_e.incrementInstancehits();
        
        // Update the state information
        state.setCurrent(next_v);
        state.addTouchedPartitions(partitions);
        if (trace.get()) LOG.trace("Updated State Information for Txn #" + state.txn_id + ":\n" + state);
    }

    // ----------------------------------------------------------------------------
    // HELPER METHODS
    // ----------------------------------------------------------------------------
    
    @SuppressWarnings("unchecked")
    public State processTransactionTrace(TransactionTrace txn_trace) throws Exception {
        final boolean t = trace.get();
        final boolean d = debug.get();
        
        long txn_id = txn_trace.getTransactionId();
        if (d) LOG.debug("Processing TransactionTrace #" + txn_id);
        if (t) LOG.trace(txn_trace.debug(this.catalog_db));
        State s = this.startTransaction(txn_id, txn_trace.getCatalogItem(this.catalog_db), txn_trace.getParams());
        assert(s != null) : "Null TransactionEstimator.State for txn #" + txn_id;
        
        for (Entry<Integer, List<QueryTrace>> e : txn_trace.getBatches().entrySet()) {
            int batch_size = e.getValue().size();
            if (t) LOG.trace(String.format("Batch #%d: %d traces", e.getKey(), batch_size));
            
            // Generate the data structures we will need to give to the TransactionEstimator
            Statement catalog_stmts[] = new Statement[batch_size];
            Set<Integer> partitions[] = (Set<Integer>[])new Set<?>[batch_size];
            for (int i = 0; i < batch_size; i++) {
                QueryTrace query_trace = e.getValue().get(i);
                assert(query_trace != null);
                catalog_stmts[i] = query_trace.getCatalogItem(catalog_db);
                
                partitions[i] = this.p_estimator.getAllPartitions(query_trace, s.getBasePartition());
                assert(partitions[i].isEmpty() == false) : "No partitions for " + query_trace;
            } // FOR
        
            synchronized (s.getMarkovGraph()) {
                this.executeQueries(s, catalog_stmts, partitions);
            } // SYNCH
        } // FOR (batches)
        if (txn_trace.isAborted()) this.abort(txn_id);
        else this.commit(txn_id);
        
//        System.err.println(StringUtil.join("\n", s.getActualPath()));
        assert(s.getActualPath().size() == (txn_trace.getQueryCount() + 2));
        return (s);
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