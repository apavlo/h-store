package edu.brown.markov;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.graphs.GraphvizExport;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.Poolable;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.dtxn.TransactionState;

/**
 * 
 * @author pavlo
 */
public class TransactionEstimator {
    private static final Logger LOG = Logger.getLogger(TransactionEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();
    
    // ----------------------------------------------------------------------------
    // STATIC DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The amount of change in visitation of vertices we would tolerate before we need to recompute the graph.
     * TODO (pavlo): Saurya says: Should this be in MarkovGraph?
     */
    private static final double RECOMPUTE_TOLERANCE = (double) 0.5;

    private static ObjectPool ESTIMATOR_POOL;
    
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
    private final HStoreConf hstore_conf;
    
    /**
     * We can maintain a cache of the last successful MarkovPathEstimator per MarkovGraph
     */
    private final Map<MarkovGraph, MarkovPathEstimator> cached_estimators = new HashMap<MarkovGraph, MarkovPathEstimator>();
    
    private transient boolean enable_recomputes = false;
    
    // ----------------------------------------------------------------------------
    // TRANSACTION STATE
    // ----------------------------------------------------------------------------
    
    /**
     * The current state of a transaction
     */
    public static final class State implements Poolable {
        private final List<Vertex> actual_path = new ArrayList<Vertex>();
        private final List<Edge> actual_path_edges = new ArrayList<Edge>();
        private final Set<Integer> touched_partitions = new HashSet<Integer>();
        private final Map<Statement, Integer> query_instance_cnts = new HashMap<Statement, Integer>();
        private final List<MarkovEstimate> estimates = new ArrayList<MarkovEstimate>();
        private final int num_partitions;

        private long txn_id = -1;
        private int base_partition;
        private long start_time;
        private MarkovGraph markov;
        private MarkovPathEstimator initial_estimator;
        private MarkovEstimate initial_estimate;
        private int num_estimates;
        
        private transient Vertex current;

        /**
         * State Factory
         */
        public static class Factory extends CountingPoolableObjectFactory<State> {
            private int num_partitions;
            
            public Factory(int num_partitions) {
                super(HStoreConf.singleton().site.txn_profiling);
                this.num_partitions = num_partitions;
            }
            
            @Override
            public State makeObjectImpl() throws Exception {
                return (new State(this.num_partitions));
            }
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
            this.initial_estimate = initial_estimator.getEstimate();
            this.setCurrent(markov.getStartVertex(), null);
        }
        
        @Override
        public boolean isInitialized() {
            return (this.txn_id != -1);
        }
        
        @Override
        public void finish() {
            // Only return the MarkovPathEstimator to it's object pool if it hasn't been cached
            if (this.initial_estimator.isCached() == false) {
                if (d) LOG.debug(String.format("Initial MarkovPathEstimator is not marked as cached for txn #%d. Returning to pool... [hashCode=%d]",
                                               this.txn_id, this.initial_estimator.hashCode()));
                try {
                    TransactionEstimator.ESTIMATOR_POOL.returnObject(this.initial_estimator);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to return MarkovPathEstimator for txn" + this.txn_id, ex);
                }
            } else if (d) {
                LOG.debug(String.format("Initial MarkovPathEstimator is marked as cached for txn #%d. Will not return to pool... [hashCode=%d]",
                                        this.txn_id, this.initial_estimator.hashCode()));
            }
         
            // We maintain a local cache of Estimates, so there is no pool to return them to
            // The MarkovPathEstimator is responsible for its own MarkovEstimate object, so we don't
            // want to return that here.
            for (int i = 0; i < this.num_estimates; i++) {
                assert(this.estimates.get(i) != this.initial_estimate) :
                    String.format("MarkovEstimate #%d == Initial MarkovEstimate for txn #%d [hashCode=%d]", i, this.txn_id, this.initial_estimate.hashCode());
                this.estimates.get(i).finish();
            } // FOR
            this.num_estimates = 0;
            
            this.markov.incrementTransasctionCount();
            this.txn_id = -1;
            this.actual_path.clear();
            this.actual_path_edges.clear();
            this.touched_partitions.clear();
            this.query_instance_cnts.clear();
            this.current = null;
            this.initial_estimator = null;
            this.initial_estimate = null;
        }
        
        /**
         * Get the next Estimate object for this State
         * @return
         */
        protected synchronized MarkovEstimate createNextEstimate(Vertex v) {
            MarkovEstimate next = null;
            if (this.num_estimates < this.estimates.size()) {
                next = this.estimates.get(this.num_estimates);
            } else {
                next = new MarkovEstimate(this.num_partitions);
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
        public Procedure getProcedure() {
            return (this.markov.getProcedure());
        }
        public String getFormattedName() {
            return (TransactionState.formatTxnName(this.markov.getProcedure(), this.txn_id));
        }
        
        /**
         * Get the number of MarkovEstimates generated for this transaction
         * @return
         */
        public int getEstimateCount() {
            return (this.num_estimates);
        }
        public List<MarkovEstimate> getEstimates() {
            return (Collections.unmodifiableList(this.estimates.subList(0, this.num_estimates)));
        }
        public Vertex getCurrent() {
            return (this.current);
        }
        /**
         * Set the current vertex for this transaction and update the actual path
         * @param current
         */
        public void setCurrent(Vertex current, Edge e) {
            if (this.current != null) assert(this.current.equals(current) == false);
            this.actual_path.add(current);
            if (e != null) this.actual_path_edges.add(e);
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
        
        public List<Vertex> getInitialPath() {
            return (this.initial_estimator.getVisitPath());
        }
        public float getInitialPathConfidence() {
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
        public MarkovEstimate getInitialEstimate() {
            return (this.initial_estimate);
        }

        public MarkovEstimate getLastEstimate() {
            return (this.num_estimates > 0 ? this.estimates.get(this.num_estimates-1) : this.initial_estimate);
        }
        
        @Override
        public String toString() {
            Map<String, Object> m0 = new ListOrderedMap<String, Object>();
            m0.put("TransactionId", this.txn_id);
            m0.put("Procedure", this.markov.getProcedure().getName());
            m0.put("MarkovGraph Id", this.markov.getGraphId());
            
            Map<String, Object> m1 = new ListOrderedMap<String, Object>();
            m1.put("Initial Partitions", this.initial_estimator.getTouchedPartitions());
            m1.put("Initial Confidence", this.getInitialPathConfidence());
            m1.put("Initial Estimate", this.getInitialEstimate().toString());
            
            Map<String, Object> m2 = new ListOrderedMap<String, Object>();
            m2.put("Actual Partitions", this.getTouchedPartitions());
            m2.put("Current Estimate", this.current.debug());
            
            return StringUtil.formatMaps(m0, m1, m2);
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
        this.hstore_conf = HStoreConf.singleton();
        if (this.markovs != null && this.markovs.hasher == null) this.markovs.setHasher(this.p_estimator.getHasher());
        
        // HACK: Initialize the STATE_POOL
        synchronized (LOG) {
            if (STATE_POOL == null) {
                if (d) LOG.debug("Creating TransactionEstimator.State Object Pool");
                STATE_POOL = new StackObjectPool(new State.Factory(this.num_partitions), HStoreConf.singleton().site.pool_estimatorstates_idle);
                
                if (d) LOG.debug("Creating MarkovPathEstimator Object Pool");
                ESTIMATOR_POOL = new StackObjectPool(new MarkovPathEstimator.Factory(this.num_partitions), HStoreConf.singleton().site.pool_pathestimators_idle);
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
    
    public static ObjectPool getEstimatorPool() {
        return (ESTIMATOR_POOL);
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

    public MarkovGraphsContainer getMarkovs() {
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
        return (s.getInitialPath());
    }
    protected double getConfidence(long txn_id) {
        State s = this.txn_states.get(txn_id);
        assert(s != null) : "Unexpected Transaction #" + txn_id;
        return (s.getInitialPathConfidence());
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
            assert(base_partition != null);
        } catch (Throwable ex) {
            throw new RuntimeException(String.format("Failed to calculate base partition for <%s, %s>", catalog_proc.getName(), Arrays.toString(args)), ex);
        }
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
        if (d) LOG.debug(String.format("Starting estimation for new %s [partition=%d]",
                                       TransactionState.formatTxnName(catalog_proc, txn_id), base_partition));

        // If we don't have a graph for this procedure, we should probably just return null
        // This will be the case for all sysprocs
        if (this.markovs == null) return (null);
        MarkovGraph markov = this.markovs.getFromParams(txn_id, base_partition, args, catalog_proc);
        if (markov == null) {
            if (d) LOG.debug("No MarkovGraph is available for " + TransactionState.formatTxnName(catalog_proc, txn_id));
            return (null);
        }
        
        Vertex start = markov.getStartVertex();
        assert(start != null) : "The start vertex is null. This should never happen!";
        MarkovPathEstimator estimator = null;
        
        // We'll reuse the last MarkovPathEstimator (and it's path) if the graph has been accurate for
        // other previous transactions. This prevents us from having to recompute the path every single time,
        // especially for single-partition transactions where the clustered MarkovGraphs are accurate
        if (hstore_conf.site.markov_path_caching && markov.getAccuracyRatio() >= hstore_conf.site.markov_path_caching_threshold) {
            estimator = this.cached_estimators.get(markov);
        }
            
        // Otherwise we have to recalculate everything from scatch again
        if (estimator == null) {
            if (d) LOG.debug("Recalculating initial path estimate for " + TransactionState.formatTxnName(catalog_proc, txn_id)); 
            try {
                estimator = (MarkovPathEstimator)ESTIMATOR_POOL.borrowObject();
                estimator.init(markov, this, base_partition, args);
                estimator.enableForceTraversal(true);
            } catch (Exception ex) {
                LOG.error("Failed to intiialize new MarkovPathEstimator for " + TransactionState.formatTxnName(catalog_proc, txn_id));
                throw new RuntimeException(ex);
            }
            
            // Calculate initial path estimate
            if (t) LOG.trace("Estimating initial execution path for " + TransactionState.formatTxnName(catalog_proc, txn_id));
            synchronized (markov) {
                start.addInstanceTime(txn_id, start_time);
                try {
                    estimator.traverse(start);
                    // if (catalog_proc.getName().equalsIgnoreCase("NewBid")) throw new Exception ("Fake!");
                } catch (Throwable e) {
                    LOG.error("Failed to estimate path for " + TransactionState.formatTxnName(catalog_proc, txn_id), e);
                    try {
                        GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(estimator.getVisitPath()));
                        LOG.error("GRAPH #" + markov.getGraphId() + " DUMP: " + gv.writeToTempFile(catalog_proc));
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                    throw new RuntimeException(e);
                }
            } // SYNCH
        } else {
            if (d) LOG.info(String.format("Using cached MarkovPathEstimator for %s [hashCode=%d, ratio=%.02f]",
                                          TransactionState.formatTxnName(catalog_proc, txn_id), estimator.getEstimate().hashCode(), markov.getAccuracyRatio()));
            assert(estimator.isCached()) :
                String.format("The cached MarkovPathEstimator used by %s does not have its cached flag set [hashCode=%d]",
                              TransactionState.formatTxnName(catalog_proc, txn_id), estimator.hashCode());
            assert(estimator.getEstimate().isValid()) :
                String.format("Invalid MarkovEstimate for cache Estimator used by %s [hashCode=%d]",
                              TransactionState.formatTxnName(catalog_proc, txn_id), estimator.getEstimate().hashCode());
            estimator.getEstimate().incrementReusedCounter();
        }
        assert(estimator != null);
        if (t) {
            List<Vertex> path = estimator.getVisitPath();
            LOG.trace(String.format("Estimated Path for %s [length=%d]\n%s",
                                    TransactionState.formatTxnName(catalog_proc, txn_id), path.size(),
                                    StringUtil.join("\n----------------------\n", path, "debug")));
            LOG.trace(String.format("MarkovEstimate for %s\n%s", TransactionState.formatTxnName(catalog_proc, txn_id), estimator.getEstimate()));
        }
        
        if (d) LOG.debug(String.format("Creating new State %s [touchedPartitions=%s]", TransactionState.formatTxnName(catalog_proc, txn_id), estimator.getTouchedPartitions()));
        State state = null;
        try {
            state = (State)STATE_POOL.borrowObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        // Calling init() will set the initial MarkovEstimate for the State
        state.init(txn_id, base_partition, markov, estimator, start_time);
        State old = this.txn_states.put(txn_id, state);
        assert(old == null) : "Duplicate transaction id " + TransactionState.formatTxnName(catalog_proc, txn_id);

        this.txn_count.incrementAndGet();
        return (state);
    }

    /**
     * Takes a series of queries and executes them in order given the partition
     * information. Provides an estimate of where the transaction might go next.
     * @param txn_id
     * @param catalog_stmts
     * @param partitions
     * @return
     */
    public MarkovEstimate executeQueries(long txn_id, Statement catalog_stmts[], Set<Integer> partitions[]) {
        assert (catalog_stmts.length == partitions.length);       
        State state = this.txn_states.get(txn_id);
        if (state == null) {
            if (d) {
                String msg = "No state information exists for txn #" + txn_id;
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
    public MarkovEstimate executeQueries(State state, Statement catalog_stmts[], Set<Integer> partitions[]) {
        if (d) LOG.debug(String.format("Processing %d queries for txn #%d", catalog_stmts.length, state.txn_id));
        
        // Roll through the Statements in this batch and move the current vertex
        // for the txn's State handle along the path in the MarkovGraph
        synchronized (state.getMarkovGraph()) {
            for (int i = 0; i < catalog_stmts.length; i++) {
                this.consume(state, catalog_stmts[i], partitions[i]);
            } // FOR
        } // SYNCH
        
        MarkovEstimate estimate = state.createNextEstimate(state.current);
        assert(estimate != null);
        if (d) LOG.debug(String.format("Next MarkovEstimate for txn #%d\n%s", state.txn_id, estimate));
        assert(estimate.isValid()) : String.format("Invalid MarkovEstimate for txn #%d\n%s", state.txn_id, estimate);
        
        // Once the workload shifts we detect it and trigger this method. Recomputes
        // the graph with the data we collected with the current workload method.
        if (this.enable_recomputes && state.getMarkovGraph().shouldRecompute(this.txn_count.get(), RECOMPUTE_TOLERANCE)) {
            state.getMarkovGraph().calculateProbabilities();
        }
        
        return (estimate);
    }

    /**
     * The transaction with provided txn_id is finished
     * @param txn_id finished transaction
     */
    public State commit(long txn_id) {
        return (this.completeTransaction(txn_id, Vertex.Type.COMMIT));
    }

    /**
     * The transaction with provided txn_id has aborted
     * @param txn_id
     */
    public State abort(long txn_id) {
        return (this.completeTransaction(txn_id, Vertex.Type.ABORT));
    }

    /**
     * The transaction for the given txn_id is in limbo, so we just want to remove it
     * Removes the transaction State without doing any final processing
     * @param txn_id
     * @return
     */
    public State mispredict(long txn_id) {
        if (d) LOG.debug(String.format("Removing State info for txn #%d", txn_id));
        // We can just remove its state and pass it back
        // We don't care if it's valid or not
        State s = this.txn_states.remove(txn_id);
        if (s != null) s.markov.incrementMispredictionCount();
        return (s);
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
            LOG.warn("No state information exists for txn #" + txn_id);
            return (null);
        }
        long timestamp = System.currentTimeMillis();
        if (d) LOG.debug(String.format("Cleaning up state info for txn #%d [type=%s]", txn_id, vtype));
        
        // We need to update the counter information in our MarkovGraph so that we know
        // that the procedure may transition to the ABORT vertex from where ever it was before 
        MarkovGraph g = s.getMarkovGraph();
        Vertex current = s.getCurrent();
        Vertex next_v = g.getSpecialVertex(vtype);
        assert(next_v != null) : "Missing " + vtype;
        
        // If no edge exists to the next vertex, then we need to create one
        synchronized (g) {
            Edge next_e = g.findEdge(current, next_v);
            if (next_e == null) next_e = g.addToEdge(current, next_v);
            s.setCurrent(next_v, next_e); // For post-txn processing...

            // Update counters
            // We want to update the counters for the entire path right here so that
            // nobody gets incomplete numbers if they recompute probabilities
            for (Vertex v : s.actual_path) v.incrementInstanceHits();
            for (Edge e : s.actual_path_edges) e.incrementInstanceHits();
            next_v.addInstanceTime(txn_id, s.getExecutionTimeOffset(timestamp));
        } // SYNCH
        
        // Store this as the last accurate MarkovPathEstimator for this graph
        if (hstore_conf.site.markov_path_caching && this.cached_estimators.containsKey(s.markov) == false && s.initial_estimate.isValid()) {
            synchronized (this.cached_estimators) {
                if (this.cached_estimators.containsKey(s.markov) == false) {
                    s.initial_estimator.setCached(true);
                    if (d) LOG.debug(String.format("Storing cached MarkovPathEstimator for %s used by txn #%d [cached=%s, hashCode=%d]",
                                                   s.markov, txn_id, s.initial_estimator.isCached(), s.initial_estimator.hashCode()));
                    this.cached_estimators.put(s.markov, s.initial_estimator);
                }
            } // SYNCH
        }
        return (s);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL ESTIMATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Figure out the next vertex that the txn will transition to for the give Statement catalog object
     * and the partitions that it will touch when it is executed. If no vertex exists, we will create
     * it and dynamically add it to our MarkovGraph
     * @param txn_id
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
        if (t) LOG.trace("Examining " + edges.size() + " edges from " + current + " for Txn #" + state.txn_id);
        for (Edge e : edges) {
            Vertex v = g.getDest(e);
            if (v.isEqual(catalog_stmt, partitions, state.getTouchedPartitions(), queryInstanceIndex)) {
                if (t) LOG.trace("Found next vertex " + v + " for Txn #" + state.txn_id);
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
            if (t) LOG.trace("Created new edge/vertex from " + state.getCurrent() + " for Txn #" + state.txn_id);
        }

        // Update the counters and other info for the next vertex and edge
        next_v.addInstanceTime(state.txn_id, state.getExecutionTimeOffset());
        
        // Update the state information
        state.setCurrent(next_v, next_e);
        state.addTouchedPartitions(partitions);
        if (t) LOG.trace("Updated State Information for Txn #" + state.txn_id + ":\n" + state);
    }

    // ----------------------------------------------------------------------------
    // HELPER METHODS
    // ----------------------------------------------------------------------------
    
    @SuppressWarnings("unchecked")
    public State processTransactionTrace(TransactionTrace txn_trace) throws Exception {
        long txn_id = txn_trace.getTransactionId();
        if (d) {
            LOG.debug("Processing TransactionTrace #" + txn_id);
            if (t) LOG.trace(txn_trace.debug(this.catalog_db));
        }
        State s = this.startTransaction(txn_id, txn_trace.getCatalogItem(this.catalog_db), txn_trace.getParams());
        assert(s != null) : "Null TransactionEstimator.State for txn #" + txn_id;
        
        for (Entry<Integer, List<QueryTrace>> e : txn_trace.getBatches().entrySet()) {
            int batch_size = e.getValue().size();
            if (t) LOG.trace(String.format("Batch #%d: %d traces", e.getKey(), batch_size));
            
            // Generate the data structures we will need to give to the TransactionEstimator
            Statement catalog_stmts[] = new Statement[batch_size];
            Set<Integer> partitions[] = (Set<Integer>[])new Set<?>[batch_size];
            this.populateQueryBatch(e.getValue(), s.getBasePartition(), catalog_stmts, partitions);
        
            synchronized (s.getMarkovGraph()) {
                this.executeQueries(s, catalog_stmts, partitions);
            } // SYNCH
        } // FOR (batches)
        if (txn_trace.isAborted()) this.abort(txn_id);
        else this.commit(txn_id);
        
        assert(s.getEstimateCount() == txn_trace.getBatchCount());
        assert(s.getActualPath().size() == (txn_trace.getQueryCount() + 2));
        return (s);
    }
    
    private void populateQueryBatch(List<QueryTrace> queries, int base_partition, Statement catalog_stmts[], Set<Integer> partitions[]) throws Exception {
        int i = 0;
        for (QueryTrace query_trace : queries) {
            assert(query_trace != null);
            catalog_stmts[i] = query_trace.getCatalogItem(catalog_db);
            partitions[i] = this.p_estimator.getAllPartitions(query_trace, base_partition);
            assert(partitions[i].isEmpty() == false) : "No partitions for " + query_trace;
            i++;
        } // FOR
    }

}