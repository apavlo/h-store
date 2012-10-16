package edu.brown.markov;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.MarkovEstimator;
import edu.brown.interfaces.Loggable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;

/**
 * Path Estimator for TransactionEstimator
 * @author pavlo
 */
public class MarkovPathEstimator extends VertexTreeWalker<MarkovVertex, MarkovEdge> implements Loggable {
    private static final Logger LOG = Logger.getLogger(MarkovPathEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    private static boolean t = trace.get();
    
    /**
     * 
     * @author pavlo
     */
    public static class Factory extends TypedPoolableObjectFactory<MarkovPathEstimator> {
        private final CatalogContext catalogContext;
        private final PartitionEstimator p_estimator;
        
        public Factory(CatalogContext catalogContext, PartitionEstimator p_estimator) {
            super(HStoreConf.singleton().site.pool_profiling);
            this.catalogContext = catalogContext;
            this.p_estimator = p_estimator;
        }
        @Override
        public MarkovPathEstimator makeObjectImpl() throws Exception {
            return (new MarkovPathEstimator(this.catalogContext, this.p_estimator));
        }
    };

    // ----------------------------------------------------------------------------
    // INVOCATION MEMBERS
    // ----------------------------------------------------------------------------
    
    private int base_partition;
    private Object args[];
    private float greatest_abort = MarkovUtil.NULL_MARKER;

    private final int num_partitions;
    private final ParameterMappingsSet allMappings;
    private final PartitionEstimator p_estimator;
    private final PartitionSet all_partitions;
    private final PartitionSet touched_partitions = new PartitionSet();
    private final PartitionSet read_partitions = new PartitionSet();
    private final PartitionSet write_partitions = new PartitionSet();
    
    /**
     * If this flag is set to true, then we will always try to go to the end
     * This means that if we don't have an edge to the vertex that we're pretty sure we want to take, we'll 
     * just pick the edge from the one that is available that has the highest probability
     */
    private boolean force_traversal = false;

    /**
     * These are the vertices that we weren't sure about.
     * This only gets populated when force_traversal is set to true 
     */
    private final Set<MarkovVertex> forced_vertices = new HashSet<MarkovVertex>();
    
    /**
     * This is how confident we are 
     */
    private float confidence = MarkovUtil.NULL_MARKER;

    /**
     * 
     */
    private MarkovEstimate estimate;
    
    /**
     * If this flag is true, then this MarkovPathEstimator is being cached by the TransactionEstimator and should not be returned to
     * the object pool when its transaction finishes
     */
    private transient boolean cached = false;

    // ----------------------------------------------------------------------------
    // TEMPORARY TRAVERSAL MEMBERS
    // ----------------------------------------------------------------------------
    
    private final transient PartitionSet past_partitions = new PartitionSet();
    
    private final transient SortedSet<MarkovEdge> candidates = new TreeSet<MarkovEdge>();
    
    private transient MarkovEdge candidate_edge = null;
    
    private final transient Set<Pair<Statement, Integer>> next_statements = new HashSet<Pair<Statement, Integer>>();
    
    private final transient PartitionSet stmt_partitions = new PartitionSet();
    
    private final transient Map<Statement, StmtParameter[]> stmt_params = new HashMap<Statement, StmtParameter[]>();
    
    private final transient Map<Statement, Object[]> stmt_param_arrays = new HashMap<Statement, Object[]>();
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param markov
     * @param t_estimator
     * @param base_partition
     * @param args
     */
    public MarkovPathEstimator(CatalogContext catalogContext, PartitionEstimator p_estimator) {
        super();
        this.num_partitions = catalogContext.numberOfPartitions;
        this.all_partitions = catalogContext.getAllPartitionIds();
        this.p_estimator = p_estimator;
        this.allMappings = catalogContext.paramMappings;
        assert(this.allMappings != null);
    }
    
    /**
     * 
     * @param markov
     * @param t_estimator
     * @param base_partition
     * @param args
     * @return
     */
    public MarkovPathEstimator init(MarkovGraph markov, MarkovEstimate estimate, int base_partition, Object args[]) {
        this.init(markov, TraverseOrder.DEPTH, Direction.FORWARD);
        this.estimate = estimate;
        this.confidence = 1.0f;
        this.base_partition = base_partition;
        this.args = args;
        assert(this.base_partition >= 0);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Procedure:       " + markov.getProcedure().getName());
            LOG.trace("Base Partition:  " + this.base_partition);
            LOG.trace("# of Partitions: " + this.num_partitions);
//            LOG.trace("Arguments:       " + Arrays.toString(args));
        }
        return (this);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.confidence != MarkovUtil.NULL_MARKER && super.isInitialized()); 
    }
    
    @Override
    public void finish() {
        if (d) LOG.debug(String.format("Cleaning up MarkovPathEstimator [cached=%s, hashCode=%d]", this.cached, this.hashCode()));
        super.finish();
        this.confidence = MarkovUtil.NULL_MARKER;
        this.greatest_abort = MarkovUtil.NULL_MARKER;
        this.cached = false;
        this.estimate = null;
        this.touched_partitions.clear();
        this.read_partitions.clear();
        this.write_partitions.clear();
        this.past_partitions.clear();
        this.forced_vertices.clear();
    }
    
    public void setCached(boolean val) {
        this.cached = val;
    }
    
    public boolean isCached() {
        return this.cached;
    }
    
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    /**
     * Forcing traversal means that we will keep estimating the
     * path even if the next MarkovVertex that the txn needs to transition
     * to is not in the MarkovGraph.
     * @param flag
     */
    public void enableForceTraversal(boolean flag) {
        this.force_traversal = flag;
    }
    
    public PartitionSet getTouchedPartitions() {
        return this.touched_partitions;
    }
    public PartitionSet getReadPartitions() {
        return this.read_partitions;
    }
    public PartitionSet getWritePartitions() {
        return this.write_partitions;
    }
    
    /**
     * 
     * @return
     */
    public Set<MarkovVertex> getForcedVertices() {
        return this.forced_vertices;
    }
    
    /**
     * Return the confidence factor that of our estimated path
     * @return
     */
    public float getConfidence() {
        return this.confidence;
    }

    private StmtParameter[] getStatementParams(Statement catalog_stmt) {
        StmtParameter arr[] = this.stmt_params.get(catalog_stmt);
        if (arr == null) {
            arr = new StmtParameter[catalog_stmt.getParameters().size()];
            for (StmtParameter catalog_param : catalog_stmt.getParameters()) {
                arr[catalog_param.getIndex()] = catalog_param;
            }
            this.stmt_param_arrays.put(catalog_stmt, arr);
        }
        return (arr);
    }
    
//    private Object[] getStatementParamsArray(Statement catalog_stmt) {
//        Object arr[] = this.stmt_param_arrays.get(catalog_stmt);
//        int size = catalog_stmt.getParameters().size();
//        if (arr == null) {
//            arr = new Object[size];
//            this.stmt_param_arrays.put(catalog_stmt, arr);
//        } else {
//            for (int i = 0; i < size; i++) arr[i] = null;
//        }
//        return (arr);
//    }
    
    /**
     * This is the main part of where we figure out the path that this transaction will take
     */
    protected void populate_children(Children<MarkovVertex> children, MarkovVertex element) {
//        if (element.isAbortVertex() || element.isCommitVertex()) {
//            return;
//        }
        
        // Initialize temporary data
        this.candidates.clear();
        this.next_statements.clear();
        this.past_partitions.addAll(element.getPartitions());
        
        if (t) LOG.trace("Current Vertex: " + element);
        Statement cur_catalog_stmt = element.getCatalogItem();
        int cur_catalog_stmt_index = element.getQueryCounter();
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        
        // At our current vertex we need to gather all of our neighbors
        // and get unique Statements that we could be executing next
        Collection<MarkovVertex> next_vertices = markov.getSuccessors(element);
        if (t) LOG.trace("Successors: " + next_vertices);
        if (next_vertices == null) {
            this.stop();
            return;
        }

        // Step #1
        // Get all of the unique Statement+StatementInstanceIndex pairs for the vertices
        // that are adjacent to our current vertex
        // XXX: Why do we use the pairs rather than just look at the vertices?
        for (MarkovVertex next : next_vertices) {
            Statement next_catalog_stmt = next.getCatalogItem();
            int next_catalog_stmt_index = next.getQueryCounter();
            
            // Sanity Check: If this vertex is the same Statement as the current vertex,
            // then its instance counter must be greater than the current vertex's counter
            if (next_catalog_stmt.equals(cur_catalog_stmt)) {
                if (next_catalog_stmt_index <= cur_catalog_stmt_index) {
                    LOG.error("CURRENT: " + element + "  [commit=" + element.isCommitVertex() + "]");
                    LOG.error("NEXT:    " + next + "  [commit=" + next.isCommitVertex() + "]");
                }
                assert(next_catalog_stmt_index > cur_catalog_stmt_index) :
                    String.format("%s[#%d] > %s[#%d]",
                                  next_catalog_stmt.fullName(), next_catalog_stmt_index,
                                  cur_catalog_stmt.fullName(), cur_catalog_stmt_index);
            }
            
            // Check whether it's COMMIT/ABORT
            if (next.isCommitVertex() || next.isAbortVertex()) {
                MarkovEdge candidate = markov.findEdge(element, next);
                assert(candidate != null);
                this.candidates.add(candidate);
            } else {
                this.next_statements.add(Pair.of(next_catalog_stmt, next_catalog_stmt_index));
            }
        } // FOR
        
        // Now for the unique set of Statement+StatementIndex pairs, figure out which partitions
        // the queries will go to.
        for (Pair<Statement, Integer> pair : this.next_statements) {
            Statement catalog_stmt = pair.getFirst();
            Integer catalog_stmt_index = pair.getSecond();
            if (t) LOG.trace("Examining " + pair);
            
            // Get the mapping objects (if any) for next
            // This is the only way we can predict what partitions we will touch
            Map<StmtParameter, SortedSet<ParameterMapping>> stmtMappings = this.allMappings.get(catalog_stmt, catalog_stmt_index);
            if (stmtMappings == null) {
                if (d) {
                    LOG.warn("No parameter mappings for " + pair);
                    LOG.trace(this.allMappings.debug(catalog_stmt));
                }
                continue;
            }
            
            // Go through the StmtParameters and map values from ProcParameters
            StmtParameter stmt_params[] = this.getStatementParams(catalog_stmt);
            Object stmt_args[] = new Object[stmt_params.length]; // this.getStatementParamsArray(catalog_stmt);
            boolean stmt_args_set = false;
            for (int i = 0; i < stmt_args.length; i++) {
                StmtParameter catalog_stmt_param = stmt_params[i];
                assert(catalog_stmt_param != null);
                if (t) LOG.trace("Examining " + CatalogUtil.getDisplayName(catalog_stmt_param, true));
                
                SortedSet<ParameterMapping> mappings = stmtMappings.get(catalog_stmt_param);
                if (mappings == null || mappings.isEmpty()) {
                    if (t) LOG.trace("No parameter mappings for " + CatalogUtil.getDisplayName(catalog_stmt_param, true) + " from " + pair);
                    continue;
                }
                if (t) LOG.trace("Found " + mappings.size() + " mapping(s) for " + CatalogUtil.getDisplayName(catalog_stmt_param, true));
        
                // Special Case:
                // If the number of possible Statements we could execute next is greater than one,
                // then we need to prune our list by removing those Statements who have a StmtParameter
                // that are correlated to a ProcParameter that doesn't exist (such as referencing an
                // array element that is greater than the size of that current array)
                // TODO: For now we are just going always pick the first mapping 
                // that comes back. Is there any choice that we would need to make in order
                // to have a better prediction about what the transaction might do?
                if (mappings.size() > 1) {
                    if (d) LOG.warn("Multiple parameter mappings for " + CatalogUtil.getDisplayName(catalog_stmt_param, true));
                    if (t) {
                        int ctr = 0;
                        for (ParameterMapping m : mappings) {
                            LOG.trace("[" + (ctr++) + "] Mapping: " + m);
                        } // FOR
                    }
                }
                for (ParameterMapping m : mappings) {
                    if (t) LOG.trace("Mapping: " + m);
                    ProcParameter catalog_proc_param = m.getProcParameter();
                    if (catalog_proc_param.getIsarray()) {
                        Object proc_inner_args[] = (Object[])args[m.getProcParameter().getIndex()];
                        if (t) LOG.trace(CatalogUtil.getDisplayName(m.getProcParameter(), true) + " is an array: " + Arrays.toString(proc_inner_args));
                        
                        // TODO: If this Mapping references an array element that is not available for this
                        // current transaction, should we just skip this mapping or skip the entire query?
                        if (proc_inner_args.length <= m.getProcParameterIndex()) {
                            if (t) LOG.trace("Unable to map parameters: " +
                                                 "proc_inner_args.length[" + proc_inner_args.length + "] <= " +
                                                 "c.getProcParameterIndex[" + m.getProcParameterIndex() + "]"); 
                            continue;
                        }
                        stmt_args[i] = proc_inner_args[m.getProcParameterIndex()];
                        stmt_args_set = true;
                        if (t) LOG.trace("Mapped " + CatalogUtil.getDisplayName(m.getProcParameter()) + "[" + m.getProcParameterIndex() + "] to " +
                                         CatalogUtil.getDisplayName(catalog_stmt_param) + " [value=" + stmt_args[i] + "]");
                    } else {
                        stmt_args[i] = args[m.getProcParameter().getIndex()];
                        stmt_args_set = true;
                        if (t) LOG.trace("Mapped " + CatalogUtil.getDisplayName(m.getProcParameter()) + " to " +
                                             CatalogUtil.getDisplayName(catalog_stmt_param) + " [value=" + stmt_args[i] + "]"); 
                    }
                    break;
                } // FOR (Mapping)
            } // FOR (StmtParameter)
                
            // If we set any of the stmt_args in the previous step, then we can throw it
            // to our good old friend the PartitionEstimator and see whether we can figure
            // things out for this Statement
            if (stmt_args_set) {
                if (t) LOG.trace("Mapped StmtParameters: " + Arrays.toString(stmt_args));
                this.stmt_partitions.clear();
                try {
                    this.p_estimator.getAllPartitions(this.stmt_partitions, catalog_stmt, stmt_args, this.base_partition);
                } catch (Exception ex) {
                    String msg = "Failed to calculate partitions for " + catalog_stmt + " using parameters " + Arrays.toString(stmt_args);
                    LOG.error(msg, ex);
                    this.stop();
                    return;
                }
                if (t) LOG.trace("Estimated Partitions for " + catalog_stmt + ": " + this.stmt_partitions);
                
                // Now for this given list of partitions, find a Vertex in our next set
                // that has the same partitions
                if (this.stmt_partitions.isEmpty() == false) {
                    this.candidate_edge = null;
                    for (MarkovVertex next : next_vertices) {
                        if (next.isEqual(catalog_stmt, this.stmt_partitions, this.past_partitions, catalog_stmt_index)) {
                            // BINGO!!!
                            assert(this.candidate_edge == null);
                            try {
                                this.candidate_edge = markov.findEdge(element, next);
                            } catch (NullPointerException ex) {
                                continue;
                            }
                            assert(this.candidate_edge != null);

                            this.candidates.add(this.candidate_edge);
                            if (t) LOG.trace("Found candidate edge to " + next + " [" + this.candidate_edge + "]");
                            break; // ???
                        }
                    } // FOR (Vertex
                    if (candidate_edge == null && t) LOG.trace("Failed to find candidate edge from " + element + " to " + pair);
                }
            // Without any stmt_args, there's nothing we can do here...
            } else {
                if (t) LOG.trace("No stmt_args for " + pair + ". Skipping...");
            } // IF
        } // FOR
        
        // If we don't have any candidate edges and the FORCE TRAVERSAL flag is set, then we'll just
        // grab all of the edges from our currect vertex
        int num_candidates = this.candidates.size();
        boolean was_forced = false;
        if (num_candidates == 0 && this.force_traversal) {
            if (t) LOG.trace("No candidate edges were found. Force travesal flag is set, so taking all");
//            if (this.next_statements.size() == 1) {
//                Pair<Statement, Integer> p = CollectionUtil.getFirst(this.next_statements);
//                Vertex v = new Vertex(p.getFirst(), Vertex.Type.QUERY, p.getSecond(), this.stmt_partitions, this.past_partitions);
//                markov.addVertex(v);
//                this.candidate_edge = new Edge(markov);
//                markov.addEdge(this.candidate_edge, element, v, EdgeType.DIRECTED);
//                this.candidates.add(candidate_edge);
//                LOG.info("Created a new vertex " + v);
//            } else {
            Collection<MarkovEdge> out_edges = markov.getOutEdges(element);
            if (out_edges != null) this.candidates.addAll(out_edges);
//            }
            num_candidates = this.candidates.size();
            was_forced = true;
        }
        
        // So now we have our list of candidate edges. We can pick the first one
        // since they will be sorted by their probability
        if (t) LOG.trace("Candidate Edges: " + this.candidates);
        if (num_candidates > 0) {
            MarkovEdge next_edge = CollectionUtil.first(this.candidates);
            MarkovVertex next_vertex = markov.getOpposite(element, next_edge);
            children.addAfter(next_vertex);
            if (was_forced) this.forced_vertices.add(next_vertex);
            
            // Our confidence is based on the total sum of the probabilities for all of the
            // edges that we could have taken in comparison to the one that we did take
            double total_probability = 0.0;
            if (d) LOG.debug("CANDIDATES:");
            int i = 0;
            for (MarkovEdge e : this.candidates) {
                MarkovVertex v = markov.getOpposite(element, e);
                total_probability += e.getProbability();
                if (d) {
                    LOG.debug(String.format("  [%d] %s  --[%s]--> %s%s",
                                            i++, element, e, v, (next_vertex.equals(v) ? " <== SELECTED" : "")));
                    if (this.candidates.size() > 1) LOG.debug(StringUtil.addSpacers(v.debug()));
                }
            } // FOR
            this.confidence *= next_edge.getProbability() / total_probability;
            
            // Update our list of partitions touched by this transaction
            PartitionSet next_partitions = next_vertex.getPartitions();
            // String orig = (debug.get() ? next_partitions.toString() : null);
            float inverse_prob = 1.0f - this.confidence;
            Statement catalog_stmt = next_vertex.getCatalogItem();
            
            // READ
            if (catalog_stmt.getQuerytype() == QueryType.SELECT.getValue()) {
                for (Integer p : next_partitions) {
                    if (this.read_partitions.contains(p) == false) {
                        if (t) LOG.trace(String.format("First time partition %d is read from! Setting read-only probability to %.03f", p, this.confidence));
                        try {
                            this.estimate.setReadOnlyProbability(p.intValue(), this.confidence);
                        } catch (AssertionError ex) {
                            if (debug.get()) {
                                LOG.debug("BUSTED: " + next_vertex);
                                LOG.debug("NEXT PARTITIONS: " + next_partitions);
                                // ??? LOG.debug("ORIG PARTITIONS: " + orig);
                            }
                            throw ex;
                        }
                        if (this.touched_partitions.contains(p) == false) {
                            this.estimate.setFinishProbability(p.intValue(), inverse_prob);
                        }
                        this.read_partitions.add(p);
                    }
                    this.estimate.incrementTouchedCounter(p.intValue());
                } // FOR
            // WRITE
            } else {
                for (Integer p : next_partitions) {
                    if (this.write_partitions.contains(p) == false) {
                        if (t) LOG.trace(String.format("First time partition %d is written to! Setting write probability to %.03f", p, this.confidence));
                        this.estimate.setReadOnlyProbability(p.intValue(), inverse_prob);
                        this.estimate.setWriteProbability(p.intValue(), this.confidence);
                        if (this.touched_partitions.contains(p) == false) {
                            this.estimate.setFinishProbability(p.intValue(), inverse_prob);
                        }
                        this.write_partitions.add(p);
                    }
                    this.estimate.incrementTouchedCounter(p.intValue());
                } // FOR
            }
            this.touched_partitions.addAll(next_partitions);
            
            // If this is the first time that the path touched more than one partition, then we need to set the single-partition
            // probability to be the confidence coefficient thus far
            if (this.touched_partitions.size() > 1 && this.estimate.isSinglePartitionProbabilitySet() == false) {
                if (t) LOG.trace("Setting the single-partition probability to current confidence [" + this.confidence + "]");
                this.estimate.setSinglePartitionProbability(inverse_prob);
            }
            
            // Keep track of the highest abort probability that we've seen thus far
            if (next_vertex.isQueryVertex() && next_vertex.getAbortProbability() > this.greatest_abort) {
                this.greatest_abort = next_vertex.getAbortProbability();
            }
            
            if (d) {
                LOG.debug("TOTAL:    " + total_probability);
                LOG.debug("SELECTED: " + next_vertex + " [confidence=" + this.confidence + "]");
                LOG.debug(StringUtil.repeat("-", 150));
            }
        } else {
            if (t) LOG.trace("No matching children found. We have to stop...");
        }
    }
    
    @Override
    protected void callback(MarkovVertex element) {
        if (element.isQueryVertex() == false) {
            if (element.isCommitVertex()) {
                if (t) LOG.trace("Reached COMMIT. Stopping...");
                this.stop();
            } else if (element.isAbortVertex()) {
                if (t) LOG.trace("Reached ABORT. Stopping...");
                this.stop();
            }
        }
    }
    
    @Override
    protected void callback_finish() {
//        if (d)
//            LOG.info("Callback Last! Last Element = " + last_v);
        List<MarkovVertex> path = this.getVisitPath();
        this.estimate.copyMarkovPath(path);
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        MarkovVertex first_v = markov.getStartVertex();
        
        // Confidence
        this.estimate.setConfidenceProbability(this.confidence);
        float inverse_prob = 1.0f - this.confidence;
        
        // Partition Probabilities
        boolean is_singlepartition = this.touched_partitions.size() == 1;
        float untouched_finish = 1.0f;
        for (int p : this.all_partitions) {
            if (this.touched_partitions.contains(p) == false) {
                this.estimate.setReadOnlyProbability(p, first_v.getReadOnlyProbability(p));
                this.estimate.setWriteProbability(p, first_v.getWriteProbability(p));
                
                float finished_prob = first_v.getFinishProbability(p);
                this.estimate.setFinishProbability(p, finished_prob);
                if (is_singlepartition) untouched_finish = Math.min(untouched_finish, finished_prob);
            } else if (this.estimate.isWriteProbabilitySet(p) == false) {
                this.estimate.setWriteProbability(p, inverse_prob);
            }
        } // FOR
        
        // Single-Partition Probability
        if (is_singlepartition) {
            if (t) LOG.trace(String.format("Only one partition was touched %s. Setting single-partition probability to ???", this.touched_partitions)); 
            this.estimate.setSinglePartitionProbability(untouched_finish);
        }
        
        // Abort Probability
        // Only use the abort probability if we have seen at least ABORT_MIN_TXNS
        if (markov.getStartVertex().getTotalHits() >= MarkovGraph.MIN_HITS_FOR_NO_ABORT) {
            this.estimate.setAbortProbability(this.greatest_abort);
        } else {
            this.estimate.setAbortProbability(1.0f);
        }
    }
    
    
    /**
     * Convenience method that returns the traversal path predicted for this instance
     * @param markov
     * @param t_estimator
     * @param args
     * @return
     */
    public static MarkovEstimate predictPath(MarkovGraph markov, MarkovEstimator t_estimator, Object args[]) {
        CatalogContext catalogContext = t_estimator.getCatalogContext();
        PartitionEstimator p_estimator = t_estimator.getPartitionEstimator();
        
        int base_partition = HStoreConstants.NULL_PARTITION_ID;
        try {
            base_partition = p_estimator.getBasePartition(markov.getProcedure(), args);
        } catch (Exception ex) {
            String msg = String.format("Failed to calculate base partition for <%s, %s>",
                                       markov.getProcedure().getName(), Arrays.toString(args)); 
            LOG.fatal(msg, ex);
            throw new RuntimeException(msg, ex);
        }
        assert(base_partition != HStoreConstants.NULL_PARTITION_ID);
        
        
        MarkovEstimate est = new MarkovEstimate(t_estimator.getCatalogContext());
        MarkovPathEstimator estimator = new MarkovPathEstimator(catalogContext, p_estimator);
        estimator.init(markov, est, base_partition, args);
        estimator.updateLogging();
        estimator.traverse(markov.getStartVertex());
        return (est);
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_MAPPINGS,
            ArgumentsParser.PARAM_MARKOV
        );
        
        // Word up
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalogContext);
        
        // Create MarkovGraphsContainer
        String input_path = args.getParam(ArgumentsParser.PARAM_MARKOV);
        Map<Integer, MarkovGraphsContainer> m = MarkovUtil.load(args.catalog_db, input_path);
        
        // Blah blah blah...
        Map<Integer, MarkovEstimator> t_estimators = new HashMap<Integer, MarkovEstimator>();
        for (Integer id : m.keySet()) {
            t_estimators.put(id, new MarkovEstimator(args.catalogContext, p_estimator, m.get(id)));
        } // FOR
        
        final Set<String> skip = new HashSet<String>();
        
        Map<Procedure, AtomicInteger> totals = new TreeMap<Procedure, AtomicInteger>();
        Map<Procedure, AtomicInteger> correct_partitions_txns = new HashMap<Procedure, AtomicInteger>();
        Map<Procedure, AtomicInteger> correct_path_txns = new HashMap<Procedure, AtomicInteger>();
        Map<Procedure, AtomicInteger> multip_txns = new HashMap<Procedure, AtomicInteger>();
        for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
            if (!catalog_proc.getSystemproc()) {
                totals.put(catalog_proc, new AtomicInteger(0));
                correct_partitions_txns.put(catalog_proc, new AtomicInteger(0));
                correct_path_txns.put(catalog_proc, new AtomicInteger(0));
                multip_txns.put(catalog_proc, new AtomicInteger(0));
            }
        } // FOR
        
        // Loop through each of the procedures and measure how accurate we are in our predictions
        for (TransactionTrace xact : args.workload.getTransactions()) {
            LOG.info(xact.debug(args.catalog_db));
            
            Procedure catalog_proc = xact.getCatalogItem(args.catalog_db);
            if (skip.contains(catalog_proc.getName())) continue;
            
            int partition = HStoreConstants.NULL_PARTITION_ID;
            try {
                partition = p_estimator.getBasePartition(catalog_proc, xact.getParams(), true);
            } catch (Exception ex) {
                ex.printStackTrace();
                assert(false);
            }
            assert(partition >= 0);
            totals.get(catalog_proc).incrementAndGet();
            
            MarkovGraph markov = m.get(partition).getFromParams(xact.getTransactionId(), partition, xact.getParams(), catalog_proc);
            if (markov == null) {
                LOG.warn(String.format("No MarkovGraph for %s at partition %d", catalog_proc.getName(), partition));
                continue;
            }
            
            // Check whether we predict the same path
            List<MarkovVertex> actual_path = markov.processTransaction(xact, p_estimator);
            MarkovEstimate est = MarkovPathEstimator.predictPath(markov, t_estimators.get(partition), xact.getParams());
            assert(est != null);
            List<MarkovVertex> predicted_path = est.getMarkovPath();
            if (actual_path.equals(predicted_path)) correct_path_txns.get(catalog_proc).incrementAndGet();
            
            LOG.info("MarkovEstimate:\n" + est);
            
            // Check whether we predict the same partitions
            PartitionSet actual_partitions = MarkovUtil.getTouchedPartitions(actual_path); 
            PartitionSet predicted_partitions = MarkovUtil.getTouchedPartitions(predicted_path);
            if (actual_partitions.equals(predicted_partitions)) correct_partitions_txns.get(catalog_proc).incrementAndGet();
            if (actual_partitions.size() > 1) multip_txns.get(catalog_proc).incrementAndGet();
            
                
//                System.err.println(xact.debug(args.catalog_db));
//                System.err.println(StringUtil.repeat("=", 120));
//                System.err.println(GraphUtil.comparePaths(markov, actual_path, predicted_path));
//                
//                String dotfile = "/home/pavlo/" + catalog_proc.getName() + ".dot";
//                GraphvizExport<Vertex, Edge> graphviz = MarkovUtil.exportGraphviz(markov, actual_path); 
//                FileUtil.writeStringToFile(dotfile, graphviz.export(catalog_proc.getName()));
////                skip.add(catalog_proc.getName());
//                System.err.println("\n\n");
        } // FOR
        
//        if (args.hasParam(ArgumentsParser.PARAM_MARKOV_OUTPUT)) {
//            markovs.save(args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
//        }
        
        System.err.println("Procedure\t\tTotal\tSingleP\tPartitions\tPaths");
        for (Entry<Procedure, AtomicInteger> entry : totals.entrySet()) {
            Procedure catalog_proc = entry.getKey();
            int total = entry.getValue().get();
            if (total == 0) continue;
            
            int valid_partitions = correct_partitions_txns.get(catalog_proc).get();
            double valid_partitions_p = (valid_partitions / (double)total) * 100;
            
            int valid_paths = correct_path_txns.get(catalog_proc).get();
            double valid_paths_p = (valid_paths / (double)total) * 100;            
            
            int singlep = total - multip_txns.get(catalog_proc).get();
            double singlep_p = (singlep / (double)total) * 100;
            
            System.err.println(String.format("%-25s %d\t%.02f\t%.02f\t%.02f", catalog_proc.getName(),
                    total,
                    singlep_p,
                    valid_partitions_p,
                    valid_paths_p
            ));
        } // FOR
        
    }
}