package edu.brown.hstore.estimators;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.Pair;

import edu.brown.graphs.GraphvizExport;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovPathEstimator;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.pools.TypedObjectPool;
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * Markov Model-based Transaction Estimator
 * @author pavlo
 */
public class MarkovEstimator extends TransactionEstimator {
    private static final Logger LOG = Logger.getLogger(MarkovEstimator.class);
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
     * The amount of change in visitation of vertices we would tolerate before we need
     * to recompute the graph.
     * TODO (pavlo): Saurya says: Should this be in MarkovGraph?
     */
    private static final double RECOMPUTE_TOLERANCE = (double) 0.5;

    public static TypedObjectPool<MarkovPathEstimator> POOL_ESTIMATORS;
    
    public static TypedObjectPool<MarkovEstimatorState> POOL_STATES;
    
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    private final CatalogContext catalogContext;
    private final MarkovGraphsContainer markovs;
    
    /**
     * We can maintain a cache of the last successful MarkovPathEstimator per MarkovGraph
     */
    private final Map<MarkovGraph, MarkovPathEstimator> cached_estimators = new HashMap<MarkovGraph, MarkovPathEstimator>();
    
    private transient boolean enable_recomputes = false;
    
    /**
     * If we're using the TransactionEstimator, then we need to convert all 
     * primitive array ProcParameters into object arrays...
     */
    private final Map<Procedure, ParameterMangler> manglers;
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * @param p_estimator
     * @param mappings
     * @param markovs
     */
    public MarkovEstimator(CatalogContext catalogContext, PartitionEstimator p_estimator, MarkovGraphsContainer markovs) {
        super(p_estimator);
        this.catalogContext = catalogContext;
        this.markovs = markovs;
        if (this.markovs != null && this.markovs.getHasher() == null) 
            this.markovs.setHasher(this.hasher);
        
        // Create all of our parameter manglers
        this.manglers = new IdentityHashMap<Procedure, ParameterMangler>();
        for (Procedure catalog_proc : this.catalogContext.database.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.manglers.put(catalog_proc, new ParameterMangler(catalog_proc));
        } // FOR
        if (debug.get()) LOG.debug(String.format("Created ParameterManglers for %d procedures",
                                   this.manglers.size()));
        
        // HACK: Initialize the STATE_POOL
        synchronized (LOG) {
            if (POOL_STATES == null) {
                if (d) LOG.debug("Creating TransactionEstimator.State Object Pool");
                TypedPoolableObjectFactory<MarkovEstimatorState> s_factory = new MarkovEstimatorState.Factory(this.catalogContext); 
                POOL_STATES = new TypedObjectPool<MarkovEstimatorState>(s_factory,
                        HStoreConf.singleton().site.pool_estimatorstates_idle);
                
                if (d) LOG.debug("Creating MarkovPathEstimator Object Pool");
                TypedPoolableObjectFactory<MarkovPathEstimator> m_factory = new MarkovPathEstimator.Factory(this.catalogContext, this.p_estimator);
                POOL_ESTIMATORS = new TypedObjectPool<MarkovPathEstimator>(m_factory,
                        HStoreConf.singleton().site.pool_pathestimators_idle);
            }
        } // SYNC
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    public void enableGraphRecomputes() {
       this.enable_recomputes = true;
    }
    public MarkovGraphsContainer getMarkovs() {
        return (this.markovs);
    }
    
    /**
     * Return the initial path estimation for the given transaction id
     * @param txn_id
     * @return
     */
//    protected List<MarkovVertex> getInitialPath(long txn_id) {
//        State s = this.txn_states.get(txn_id);
//        assert(s != null) : "Unexpected Transaction #" + txn_id;
//        return (s.getInitialPath());
//    }
//    protected double getConfidence(long txn_id) {
//        State s = this.txn_states.get(txn_id);
//        assert(s != null) : "Unexpected Transaction #" + txn_id;
//        return (s.getInitialPathConfidence());
//    }
    
    // ----------------------------------------------------------------------------
    // RUNTIME METHODS
    // ----------------------------------------------------------------------------

    @Override
    public EstimatorState startTransactionImpl(Long txn_id, int base_partition, Procedure catalog_proc, Object[] args) {
        assert (catalog_proc != null);
        long start_time = EstTime.currentTimeMillis();
        if (d) LOG.debug(String.format("%s - Starting transaction estimation [partition=%d]",
                         AbstractTransaction.formatTxnName(catalog_proc, txn_id), base_partition));

        // If we don't have a graph for this procedure, we should probably just return null
        // This will be the case for all sysprocs
        if (this.markovs == null) return (null);
        MarkovGraph markov = this.markovs.getFromParams(txn_id, base_partition, args, catalog_proc);
        if (markov == null) {
            if (d) LOG.debug(String.format("%s - No MarkovGraph is available for transaction",
                             AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
            return (null);
        }
        
        if (t) LOG.trace(String.format("%s - Creating new MarkovEstimatorState",
                         AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
        MarkovEstimatorState state = null;
        try {
            state = (MarkovEstimatorState)POOL_STATES.borrowObject();
            state.init(txn_id, base_partition, markov, start_time);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        MarkovVertex start = markov.getStartVertex();
        assert(start != null) : "The start vertex is null. This should never happen!";
        MarkovEstimate initialEst = state.createNextEstimate(start);
        MarkovPathEstimator pathEstimator = null;
        
        // We'll reuse the last MarkovPathEstimator (and it's path) if the graph has been accurate for
        // other previous transactions. This prevents us from having to recompute the path every single time,
        // especially for single-partition transactions where the clustered MarkovGraphs are accurate
        if (hstore_conf.site.markov_path_caching && markov.getAccuracyRatio() >= hstore_conf.site.markov_path_caching_threshold) {
            pathEstimator = this.cached_estimators.get(markov);
        }
            
        // Otherwise we have to recalculate everything from scratch again
        if (pathEstimator == null) {
            ParameterMangler mangler = this.manglers.get(catalog_proc);
            Object mangledArgs[] = args;
            if (mangler != null) mangledArgs = mangler.convert(args);
            
            if (d) LOG.debug(String.format("%s - Recalculating initial path estimate",
                             AbstractTransaction.formatTxnName(catalog_proc, txn_id))); 
            try {
                pathEstimator = (MarkovPathEstimator)POOL_ESTIMATORS.borrowObject();
                pathEstimator.init(markov, initialEst, base_partition, mangledArgs);
                pathEstimator.enableForceTraversal(true);
            } catch (Throwable ex) {
                String msg = "Failed to intitialize new MarkovPathEstimator for " + AbstractTransaction.formatTxnName(catalog_proc, txn_id); 
                LOG.error(msg, ex);
                throw new RuntimeException(msg, ex);
            }
            
            // Calculate initial path estimate
            if (t) LOG.trace(String.format("%s - Estimating initial execution path",
                             AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
            start.addInstanceTime(txn_id, start_time);
            synchronized (markov) {
                try {
                    pathEstimator.traverse(start);
                    // if (catalog_proc.getName().equalsIgnoreCase("NewBid")) throw new Exception ("Fake!");
                } catch (Throwable ex) {
                    try {
                        GraphvizExport<MarkovVertex, MarkovEdge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(pathEstimator.getVisitPath()));
                        LOG.error("GRAPH #" + markov.getGraphId() + " DUMP: " + gv.writeToTempFile(catalog_proc));
                    } catch (Exception ex2) {
                        throw new RuntimeException(ex2);
                    }
                    String msg = "Failed to estimate path for " + AbstractTransaction.formatTxnName(catalog_proc, txn_id);
                    LOG.error(msg, ex);
                    throw new RuntimeException(msg, ex);
                }
            } // SYNCH
        }
//        else {
//            if (d) LOG.info(String.format("Using cached MarkovPathEstimator for %s [hashCode=%d, ratio=%.02f]",
//                            AbstractTransaction.formatTxnName(catalog_proc, txn_id),
//                            pathEstimator.hashCode(), markov.getAccuracyRatio()));
//            assert(pathEstimator.isCached()) :
//                String.format("The cached MarkovPathEstimator used by %s does not have its cached flag set [hashCode=%d]",
//                              AbstractTransaction.formatTxnName(catalog_proc, txn_id), pathEstimator.hashCode());
//            assert(pathEstimator.getEstimate().isValid()) :
//                String.format("Invalid MarkovEstimate for cache Estimator used by %s [hashCode=%d]",
//                              AbstractTransaction.formatTxnName(catalog_proc, txn_id), pathEstimator.hashCode());
//            pathEstimator.getEstimate().incrementReusedCounter();
//        }
        assert(pathEstimator != null);
        if (t) {
            String txnName = AbstractTransaction.formatTxnName(catalog_proc, txn_id);
            List<MarkovVertex> path = pathEstimator.getVisitPath();
            LOG.trace(String.format("%s - Estimated Path [length=%d]\n%s",
                      txnName, path.size(),
                      StringUtil.join("\n----------------------\n", path, "debug")));
            LOG.trace(String.format("%s - MarkovEstimate\n%s",
                      txnName, initialEst));
        }
        
        // Update EstimatorState.prefetch any time we transition to a MarkovVertex where the
        // underlying Statement catalog object was marked as prefetchable
        // Do we want to put this traversal above?
        for (MarkovVertex vertex : pathEstimator.getVisitPath()) {
            Statement statement = (Statement) vertex.getCatalogItem();
            if (statement.getPrefetchable()) {
                state.addPrefetchableStatement(statement, vertex.getQueryCounter());
            }
        }
        
        return (state);
    }

//    public final AtomicInteger batch_cache_attempts = new AtomicInteger(0);
//    public final AtomicInteger batch_cache_success = new AtomicInteger(0);
//    public final ProfileMeasurement CACHE = new ProfileMeasurement("CACHE");
//    public final ProfileMeasurement CONSUME = new ProfileMeasurement("CONSUME");
    
    @Override
    public TransactionEstimate executeQueries(EstimatorState s, Statement catalog_stmts[], PartitionSet partitions[], boolean allow_cache_lookup) {
        MarkovEstimatorState state = (MarkovEstimatorState)s; 
        if (d) LOG.debug(String.format("Processing %d queries for txn #%d", catalog_stmts.length, state.txn_id));
        int batch_size = catalog_stmts.length;
        MarkovGraph markov = state.getMarkovGraph();
            
        MarkovVertex current = state.current;
        MarkovVertex next_v = null;
        MarkovEdge next_e = null;
        Statement last_stmt = null;
        int stmt_idxs[] = null;
        boolean attempt_cache_lookup = false;

        if (attempt_cache_lookup && batch_size >= hstore_conf.site.markov_batch_caching_min) {
            assert(current != null);
            if (d) LOG.debug("Attempting cache look-up for last statement in batch: " + Arrays.toString(catalog_stmts));
            attempt_cache_lookup = true;
            
            state.cache_last_partitions.clear();
            state.cache_past_partitions.clear();
            
            PartitionSet last_partitions;
            stmt_idxs = new int[batch_size];
            for (int i = 0; i < batch_size; i++) {
                last_stmt = catalog_stmts[i];
                last_partitions = partitions[batch_size - 1];
                stmt_idxs[i] = state.updateQueryInstanceCount(last_stmt);
                if (i+1 != batch_size) state.cache_past_partitions.addAll(last_partitions);
                else state.cache_last_partitions.addAll(last_partitions);
            } // FOR
            
            Pair<MarkovEdge, MarkovVertex> pair = markov.getCachedBatchEnd(current,
                                                                           last_stmt,
                                                                           stmt_idxs[batch_size-1],
                                                                           state.cache_last_partitions,
                                                                           state.cache_past_partitions);
            if (pair != null) {
                next_e = pair.getFirst();
                assert(next_e != null);
                next_v = pair.getSecond();
                assert(next_v != null);
                if (d) LOG.debug(String.format("Got cached batch end for %s: %s -> %s", markov, current, next_v));
                
                // Update the counters and other info for the next vertex and edge
                next_v.addInstanceTime(state.txn_id, state.getExecutionTimeOffset());
                
                // Update the state information
                state.setCurrent(next_v, next_e);
                state.touched_partitions.addAll(state.cache_last_partitions);
                state.touched_partitions.addAll(state.cache_past_partitions);
            }
        }
        
        // Roll through the Statements in this batch and move the current vertex
        // for the txn's State handle along the path in the MarkovGraph
        if (next_v == null) {
            for (int i = 0; i < batch_size; i++) {
                int idx = (attempt_cache_lookup ? stmt_idxs[i] : -1);
                this.consume(state, markov, catalog_stmts[i], partitions[i], idx);
                if (attempt_cache_lookup == false) state.touched_partitions.addAll(partitions[i]);
            } // FOR
            
            // Update our cache if we tried and failed before
            if (attempt_cache_lookup) {
                if (d) LOG.debug(String.format("Updating cache batch end for %s: %s -> %s", markov, current, state.current));
                markov.addCachedBatchEnd(current,
                                         CollectionUtil.last(state.actual_path_edges),
                                         state.current,
                                         last_stmt,
                                         stmt_idxs[batch_size-1],
                                         state.cache_past_partitions,
                                         state.cache_last_partitions);
            }
        }
        
        MarkovEstimate estimate = state.createNextEstimate(state.current);
        assert(estimate != null);
        if (d) LOG.debug(String.format("Next MarkovEstimate for txn #%d\n%s", state.txn_id, estimate));
        assert(estimate.isValid()) :
            String.format("Invalid MarkovEstimate for txn #%d\n%s", state.txn_id, estimate);
        
        // Once the workload shifts we detect it and trigger this method. Recomputes
        // the graph with the data we collected with the current workload method.
        if (this.enable_recomputes && markov.shouldRecompute(this.txn_count.get(), RECOMPUTE_TOLERANCE)) {
            markov.calculateProbabilities();
        }
        return (estimate);
    }

    @Override
    protected void completeTransaction(EstimatorState s, Status status) {
        MarkovEstimatorState state = (MarkovEstimatorState)s;

        // The transaction for the given txn_id is in limbo, so we just want to remove it
        if (status != Status.OK && status != Status.ABORT_USER) {
            if (state != null && status == Status.ABORT_MISPREDICT) 
                state.markov.incrementMispredictionCount();
            return;
        }

        Long txn_id = state.getTransactionId();
        long timestamp = EstTime.currentTimeMillis();
        if (d) LOG.debug(String.format("Cleaning up state info for txn #%d [status=%s]",
                         txn_id, status));
        
        // We need to update the counter information in our MarkovGraph so that we know
        // that the procedure may transition to the ABORT vertex from where ever it was before 
        MarkovGraph g = state.getMarkovGraph();
        MarkovVertex current = state.getCurrent();
        MarkovVertex next_v = g.getFinishVertex(status);
        assert(next_v != null) : "Missing " + status;
        
        // If no edge exists to the next vertex, then we need to create one
        MarkovEdge next_e = g.findEdge(current, next_v);
        if (next_e == null) {
            synchronized (g) {
                next_e = g.findEdge(current, next_v);
                if (next_e == null) {
                    next_e = g.addToEdge(current, next_v);
                }
            } // SYNCH
        }
        state.setCurrent(next_v, next_e); // For post-txn processing...

        // Update counters
        // We want to update the counters for the entire path right here so that
        // nobody gets incomplete numbers if they recompute probabilities
        for (MarkovVertex v : state.actual_path) v.incrementInstanceHits();
        for (MarkovEdge e : state.actual_path_edges) e.incrementInstanceHits();
        next_v.addInstanceTime(txn_id, state.getExecutionTimeOffset(timestamp));
        
        // Store this as the last accurate MarkovPathEstimator for this graph
//        if (hstore_conf.site.markov_path_caching &&
//                this.cached_estimators.containsKey(state.markov) == false &&
//                state.getInitialEstimate().isValid()) {
//            synchronized (this.cached_estimators) {
//                if (this.cached_estimators.containsKey(state.markov) == false) {
//                    state.initial_estimator.setCached(true);
//                    if (d) LOG.debug(String.format("Storing cached MarkovPathEstimator for %s used by txn #%d [cached=%s, hashCode=%d]",
//                                                   state.markov, txn_id, state.initial_estimator.isCached(), state.initial_estimator.hashCode()));
//                    this.cached_estimators.put(state.markov, state.initial_estimator);
//                }
//            } // SYNCH
//        }
        return;
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
    private void consume(MarkovEstimatorState state, MarkovGraph markov, Statement catalog_stmt, Collection<Integer> partitions, int queryInstanceIndex) {
//        CONSUME.start();
        // Update the number of times that we have executed this query in the txn
        if (queryInstanceIndex < 0) queryInstanceIndex = state.updateQueryInstanceCount(catalog_stmt);
        assert(markov != null);
        
        // Examine all of the vertices that are adjacent to our current vertex
        // and see which vertex we are going to move to next
        MarkovVertex current = state.getCurrent();
        assert(current != null);
        MarkovVertex next_v = null;
        MarkovEdge next_e = null;
        
        // Synchronize on the single vertex so that it's more fine-grained than the entire graph
        synchronized (current) {
            Collection<MarkovEdge> edges = markov.getOutEdges(current); 
            if (t) LOG.trace("Examining " + edges.size() + " edges from " + current + " for Txn #" + state.txn_id);
            for (MarkovEdge e : edges) {
                MarkovVertex v = markov.getDest(e);
                if (v.isEqual(catalog_stmt, partitions, state.touched_partitions, queryInstanceIndex)) {
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
                next_v = new MarkovVertex(catalog_stmt,
                                          MarkovVertex.Type.QUERY,
                                          queryInstanceIndex,
                                          partitions,
                                          state.touched_partitions);
                markov.addVertex(next_v);
                next_e = markov.addToEdge(current, next_v);
                if (t) LOG.trace(String.format("Created new edge from %s to new %s for txn #%d", 
                                 state.getCurrent(), next_v, state.txn_id));
                assert(state.getCurrent().getPartitions().size() <= state.touched_partitions.size());
            }
        } // SYNCH

        // Update the counters and other info for the next vertex and edge
        next_v.addInstanceTime(state.txn_id, state.getExecutionTimeOffset());
        
        // Update the state information
        state.setCurrent(next_v, next_e);
        if (t) LOG.trace("Updated State Information for Txn #" + state.txn_id + ":\n" + state);
//        CONSUME.stop();
    }

    // ----------------------------------------------------------------------------
    // HELPER METHODS
    // ----------------------------------------------------------------------------
    
    public MarkovEstimatorState processTransactionTrace(TransactionTrace txn_trace) throws Exception {
        Long txn_id = txn_trace.getTransactionId();
        if (d) {
            LOG.debug("Processing TransactionTrace #" + txn_id);
            if (t) LOG.trace(txn_trace.debug(this.catalogContext.database));
        }
        MarkovEstimatorState s = (MarkovEstimatorState)this.startTransaction(txn_id,
                                               txn_trace.getCatalogItem(this.catalogContext.database),
                                               txn_trace.getParams());
        assert(s != null) : "Null EstimatorState for txn #" + txn_id;
        
        for (Entry<Integer, List<QueryTrace>> e : txn_trace.getBatches().entrySet()) {
            int batch_size = e.getValue().size();
            if (t) LOG.trace(String.format("Batch #%d: %d traces", e.getKey(), batch_size));
            
            // Generate the data structures we will need to give to the TransactionEstimator
            Statement catalog_stmts[] = new Statement[batch_size];
            PartitionSet partitions[] = new PartitionSet[batch_size];
            this.populateQueryBatch(e.getValue(), s.getBasePartition(), catalog_stmts, partitions);
        
            synchronized (s.getMarkovGraph()) {
                this.executeQueries(s, catalog_stmts, partitions, false);
            } // SYNCH
        } // FOR (batches)
        if (txn_trace.isAborted()) {
            this.abort(s, Status.ABORT_USER);
        } else {
            this.commit(s);
        }
        
        assert(s.getEstimateCount()-1 == txn_trace.getBatchCount()) :
            String.format("EstimateCount[%d] != BatchCount[%d]",
                          s.getEstimateCount(), txn_trace.getBatchCount());
        assert(s.actual_path.size() == (txn_trace.getQueryCount() + 2)) :
            String.format("Path[%d] != QueryCount[%d]",
                          s.actual_path.size(), txn_trace.getQueryCount());
        return (s);
    }
    
    private boolean populateQueryBatch(List<QueryTrace> queries, int base_partition, Statement catalog_stmts[], PartitionSet partitions[]) throws Exception {
        int i = 0;
        boolean readOnly = true;
        for (QueryTrace query_trace : queries) {
            assert(query_trace != null);
            catalog_stmts[i] = query_trace.getCatalogItem(catalogContext.database);
            partitions[i] = new PartitionSet();
            this.p_estimator.getAllPartitions(partitions[i], query_trace, base_partition);
            assert(partitions[i].isEmpty() == false) : "No partitions for " + query_trace;
            readOnly = readOnly && catalog_stmts[i].getReadonly();
            i++;
        } // FOR
        return (readOnly);
    }

}