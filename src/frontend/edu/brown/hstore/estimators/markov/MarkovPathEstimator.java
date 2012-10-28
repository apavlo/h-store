package edu.brown.hstore.estimators.markov;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.CountedStatement;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.interfaces.Loggable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;
import edu.uci.ics.jung.graph.util.EdgeType;

/**
 * Path Estimator for MarkovEstimator
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
    
    private MarkovEstimate estimate;
    private int base_partition;
    private Object args[];

    private final int num_partitions;
    private final ParameterMappingsSet allMappings;
    private final PartitionEstimator p_estimator;
    
    /**
     * If this flag is set to true, then we will always try to go to the end
     * This means that if we don't have an edge to the vertex that we're pretty sure we
     * want to take, we'll just pick the edge from the one that is available that has 
     * the highest probability.
     */
    private boolean force_traversal = false;
    
    /**
     * These are the vertices that we weren't sure about.
     * This only gets populated when force_traversal is set to true.
     * This is primarily used for debugging.
     */
    private final Set<MarkovVertex> forced_vertices = new HashSet<MarkovVertex>();
    
    /**
     * If this flag is set to true, then the estimator will be allowed to create vertices
     * that it knows that it needs to transition to but do not exist.
     */
    private boolean create_missing = false;
    
    /**
     * These are the vertices that we weren't sure about.
     * This only gets populated when force_traversal is set to true.
     * This is primarily used for debugging.
     */
    private final Set<MarkovVertex> created_vertices = new HashSet<MarkovVertex>();

    // ----------------------------------------------------------------------------
    // TEMPORARY TRAVERSAL MEMBERS
    // ----------------------------------------------------------------------------
    
    private final PartitionSet stmt_partitions = new PartitionSet();
    private final PartitionSet past_partitions = new PartitionSet();
    private final SortedSet<MarkovEdge> candidate_edges = new TreeSet<MarkovEdge>();
    private final Collection<CountedStatement> next_statements = new HashSet<CountedStatement>();
    
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
        return (this.estimate != null);
    }
    
    @Override
    public void finish() {
        if (d) LOG.debug(String.format("Cleaning up MarkovPathEstimator [hashCode=%d]",
                         this.hashCode()));
        super.finish();
        this.estimate = null;
        this.forced_vertices.clear();
        this.created_vertices.clear();
        this.past_partitions.clear();
        this.stmt_partitions.clear();
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
    public void setForceTraversal(boolean flag) {
        this.force_traversal = flag;
    }
    protected Collection<MarkovVertex> getForcedVertices() {
        return this.forced_vertices;
    }
    
    /**
     * Setting this flag to true means that the MarkovPathEstimator is allowed
     * to create vertices that it knows that it needs to transition to but do not
     * exist yet in the graph. Note that this is done without any synchronization, so
     * it is up to whomever is using use to make sure that what we're doing is thread safe.
     * @param flag
     */
    public void setCreateMissing(boolean flag) {
        this.create_missing = flag;
    }
    protected Collection<MarkovVertex> getCreatedVertices() {
        return this.created_vertices;
    }

    // ----------------------------------------------------------------------------
    // TRAVERSAL METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * This is the main part of where we figure out the path that this transaction will take
     */
    protected void populate_children(Children<MarkovVertex> children, MarkovVertex element) {
//        if (element.isAbortVertex() || element.isCommitVertex()) {
//            return;
//        }
        
        // Initialize temporary data
        this.candidate_edges.clear();
        this.next_statements.clear();
        this.past_partitions.addAll(element.getPartitions());
        
        if (t) LOG.trace("Current Vertex: " + element);
        Statement cur_catalog_stmt = element.getCatalogItem();
        int cur_catalog_stmt_index = element.getQueryCounter();
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        
        // At our current vertex we need to gather all of our neighbors
        // and get unique Statements that we could be executing next
        Collection<MarkovVertex> next_vertices = markov.getSuccessors(element);
        if (next_vertices == null || next_vertices.isEmpty()) {
            if (t) LOG.trace("No succesors were found for " + element + ". Halting traversal");
            this.stop();
            return;
        }
        if (t) LOG.trace("Successors: " + next_vertices);
        
        // Step #1
        // Get all of the unique Statement+StatementInstanceIndex pairs for the vertices
        // that are adjacent to our current vertex
        for (MarkovVertex next : next_vertices) {
            Statement next_catalog_stmt = next.getCatalogItem();
            int next_catalog_stmt_index = next.getQueryCounter();
            
            // Sanity Check: If this vertex is the same Statement as the current vertex,
            // then its instance counter must be greater than the current vertex's counter
            if (next_catalog_stmt.equals(cur_catalog_stmt)) {
                if (next_catalog_stmt_index <= cur_catalog_stmt_index) {
                    LOG.error("CURRENT: " + element + " [commit=" + element.isCommitVertex() + "]");
                    LOG.error("NEXT: " + next + " [commit=" + next.isCommitVertex() + "]");
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
                this.candidate_edges.add(candidate);
            } else {
                this.next_statements.add(next.getCountedStatement());
            }
        } // FOR

        // Now for the unique set of Statement+StatementIndex pairs, figure out which partitions
        // the queries will go to.
        MarkovEdge candidate_edge;
        for (CountedStatement cstmt : this.next_statements) {
            Statement catalog_stmt = cstmt.statement;
            Integer catalog_stmt_index = cstmt.counter;
            if (t) LOG.trace("Examining " + cstmt);
            
            // Get the mapping objects (if any) for next
            // This is the only way we can predict what partitions we will touch
            Map<StmtParameter, SortedSet<ParameterMapping>> stmtMappings = this.allMappings.get(catalog_stmt, catalog_stmt_index);
            if (stmtMappings == null) {
                if (d) {
                    LOG.warn("No parameter mappings for " + catalog_stmt);
                    LOG.trace(this.allMappings.debug(catalog_stmt));
                }
                continue;
            }
            
            // Go through the StmtParameters and map values from ProcParameters
            StmtParameter stmt_params[] = catalog_stmt.getParameters().values();
            Object stmt_args[] = new Object[stmt_params.length]; // this.getStatementParamsArray(catalog_stmt);
            boolean stmt_args_set = false;
            for (int i = 0; i < stmt_args.length; i++) {
                StmtParameter catalog_stmt_param = stmt_params[i];
                assert(catalog_stmt_param != null);
                if (t) LOG.trace("Retrieving ParameterMappings for " + catalog_stmt_param.fullName());
                
                SortedSet<ParameterMapping> mappings = stmtMappings.get(catalog_stmt_param);
                if (mappings == null || mappings.isEmpty()) {
                    if (t) LOG.trace("No parameter mappings exists for " + catalog_stmt_param.fullName());
                    continue;
                }
                if (t) LOG.trace("Found " + mappings.size() + " mapping(s) for " + catalog_stmt_param.fullName());
        
                // Special Case:
                // If the number of possible Statements we could execute next is greater than one,
                // then we need to prune our list by removing those Statements who have a StmtParameter
                // that are correlated to a ProcParameter that doesn't exist (such as referencing an
                // array element that is greater than the size of that current array)
                // TODO: For now we are just going always pick the first mapping 
                // that comes back. Is there any choice that we would need to make in order
                // to have a better prediction about what the transaction might do?
                if (mappings.size() > 1) {
                    if (d) LOG.warn("Multiple parameter mappings for " + catalog_stmt_param.fullName());
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
                    candidate_edge = null;
                    if (t) LOG.trace("Partitions:" + this.stmt_partitions + " / Past:" + this.past_partitions);
                    for (MarkovVertex next_v : next_vertices) {
                        if (t) LOG.trace("Checking whether " + next_v + " is the correct transition");
                        if (next_v.isEqual(catalog_stmt, this.stmt_partitions, this.past_partitions, catalog_stmt_index, true)) {
                            // BINGO!!!
                            assert(candidate_edge == null);
                            try {
                                candidate_edge = markov.findEdge(element, next_v);
                            } catch (NullPointerException ex) {
                                continue;
                            }
                            assert(candidate_edge != null);
                            this.candidate_edges.add(candidate_edge);
                            if (t) LOG.trace("Found candidate edge to " + next_v + " [" + candidate_edge + "]");
                            break;
                        } else if (t) { 
                            Map<String, Object> m = new LinkedHashMap<String, Object>();
                            m.put("stmt", next_v.getCatalogItem().equals(catalog_stmt));
                            m.put("stmtCtr", next_v.getQueryCounter() == catalog_stmt_index);
                            m.put("partitions", next_v.getPartitions().equals(this.stmt_partitions));
                            m.put("past", next_v.getPastPartitions().equals(this.past_partitions));
                            LOG.trace("Invalid candidate transition:\n" + StringUtil.formatMaps(m));
                        }
                    } // FOR (Vertex
                    if (t && candidate_edge == null)
                        LOG.trace(String.format("Failed to find candidate edge from %s to %s [partitions=%s]",
                                  element, catalog_stmt.fullName(), this.stmt_partitions)); 
                }
            }
            // Without any stmt_args, there's nothing we can do here...
            else if (t) { 
                LOG.trace("No stmt_args for " + catalog_stmt + ". Skipping...");
            } // IF
        } // FOR
        
        // If we don't have any candidate edges and the FORCE TRAVERSAL flag is set, then we'll just
        // grab all of the edges from our currect vertex
        int num_candidates = this.candidate_edges.size();
        boolean was_forced = false;
        if (num_candidates == 0 && this.force_traversal) {
            if (t) LOG.trace(String.format("No candidate edges were found. " +
            		                       "Checking whether we can create our own. [nextStatements=%s]",
            		                       this.next_statements));
            
            // We're allow to create the vertices that we know are missing
            if (this.create_missing && this.next_statements.size() == 1) {
                CountedStatement cntStmt = CollectionUtil.first(this.next_statements);
                MarkovVertex v = new MarkovVertex(cntStmt.statement,
                                                  MarkovVertex.Type.QUERY,
                                                  cntStmt.counter,
                                                  this.stmt_partitions,
                                                  this.past_partitions);
                markov.addVertex(v);
                
                // For now we'll set the new edge's probability to 1.0 to just
                // make the calculations down below work. This will get updated
                // overtime when we recompute the probabilities in the entire graph.
                candidate_edge = new MarkovEdge(markov, 1, 1.0f);
                markov.addEdge(candidate_edge, element, v, EdgeType.DIRECTED);
                this.candidate_edges.add(candidate_edge);
                this.created_vertices.add(v);
                if (t) LOG.trace(String.format("Created new vertex %s and connected it to %s", v, element));
                
                // 2012-10-21
                // The problem with allow the estimator to create a new vertex is that 
                // we don't know what it's children are going to be. That means that when
                // we invoke this method again at the next vertex (the one we just made above)
                // then it's not going to have any children, so we don't know what it's
                // going to do. We are actually better off with just grabbing the next best
                // vertex from the existing edges and then updating the graph after 
                // the txn has finished, since now we know exactly what it did.
            }
            // Otherwise we'll just make all of the out bound edges from the
            // current vertex be our candidates
            else {
                if (t) LOG.trace("No candidate edges were found. Force travesal flag is set to true, so taking all");
                Collection<MarkovEdge> out_edges = markov.getOutEdges(element);
                if (out_edges != null) this.candidate_edges.addAll(out_edges);
            }
            num_candidates = this.candidate_edges.size();
            was_forced = true;
        }
        
        // So now we have our list of candidate edges. We can pick the first one
        // since they will be sorted by their probability
        if (t) LOG.trace("Candidate Edges: " + this.candidate_edges);
        if (num_candidates > 0) {
            MarkovEdge next_edge = CollectionUtil.first(this.candidate_edges);
            MarkovVertex next_vertex = markov.getOpposite(element, next_edge);
            children.addAfter(next_vertex);
            if (was_forced) this.forced_vertices.add(next_vertex);
            
            // Our confidence is based on the total sum of the probabilities for all of the
            // edges that we could have taken in comparison to the one that we did take
            double total_probability = 0.0;
            if (d) LOG.debug(String.format("#%02d CANDIDATES:", this.getDepth()));
            int i = 0;
            for (MarkovEdge e : this.candidate_edges) {
                MarkovVertex v = markov.getOpposite(element, e);
                total_probability += e.getProbability();
                if (d) {
                    LOG.debug(String.format("  [%d] %s  --[%s]--> %s%s%s",
                              i++, element, e, v,
                              (next_vertex.equals(v) ? " <== SELECTED" : ""),
                              (t && this.candidate_edges.size() > 1 ? "\n"+StringUtil.addSpacers(v.debug()) : "")));
                }
            } // FOR
            this.estimate.confidence *= next_edge.getProbability() / total_probability;
            
            // Update our list of partitions touched by this transaction
            MarkovPathEstimator.populateProbabilities(this.estimate, next_vertex);
            
            if (d) {
                LOG.debug("TOTAL:    " + total_probability);
                LOG.debug("SELECTED: " + next_vertex + " [confidence=" + this.estimate.confidence + "]");
                LOG.debug(StringUtil.repeat("-", 150));
            }
        } else {
            if (t) LOG.trace("No matching children found. We have to stop...");
        }
    }
    
    @Override
    protected void callback(MarkovVertex v) {
        if (v.isQueryVertex() == false) {
            if (v.isCommitVertex()) {
                if (t) LOG.trace("Reached COMMIT. Stopping...");
                this.stop();
            } else if (v.isAbortVertex()) {
                if (t) LOG.trace("Reached ABORT. Stopping...");
                this.stop();
            }
        }
        this.estimate.path.add(v);
    }
    
    
    @Override
    protected void callback_finish() {
        MarkovPathEstimator.populateMarkovEstimate(this.estimate, this.estimate.getVertex());
    }
    
    // ----------------------------------------------------------------------------
    // PROBABILITY CALCULATION METHODS
    // ----------------------------------------------------------------------------
    
    protected static void populateProbabilities(MarkovEstimate estimate, MarkovVertex next_vertex) {
        PartitionSet next_partitions = next_vertex.getPartitions();
        // String orig = (debug.get() ? next_partitions.toString() : null);
        float inverse_prob = 1.0f - estimate.confidence;
        Statement catalog_stmt = next_vertex.getCatalogItem();
        
        // READ
        if (catalog_stmt.getQuerytype() == QueryType.SELECT.getValue()) {
            for (int p : next_partitions) {
                if (estimate.read_partitions.contains(p) == false) {
                    if (t) LOG.trace(String.format("First time partition %d is read from! Setting read-only probability to %.03f", p, estimate.confidence));
                    estimate.setReadOnlyProbability(p, estimate.confidence);
                    if (estimate.isFinishProbabilitySet(p) == false) {
                        estimate.setFinishProbability(p, inverse_prob);
                    }
                    estimate.read_partitions.add(p);
                }
                estimate.incrementTouchedCounter(p);
            } // FOR
        }
        // WRITE
        else {
            for (int p : next_partitions) {
                if (estimate.write_partitions.contains(p) == false) {
                    if (t) LOG.trace(String.format("First time partition %d is written to! Setting write probability to %.03f", p, estimate.confidence));
                    estimate.setReadOnlyProbability(p, inverse_prob);
                    estimate.setWriteProbability(p, estimate.confidence);
                    if (estimate.isFinishProbabilitySet(p) == false) {
                        estimate.setFinishProbability(p, inverse_prob);
                    }
                    estimate.write_partitions.add(p);
                }
                estimate.incrementTouchedCounter(p);
            } // FOR
        }
        estimate.touched_partitions.addAll(next_vertex.getPartitions());
        
        // If this is the first time that the path touched more than one partition, then we need to set the single-partition
        // probability to be the confidence coefficient thus far
        if (estimate.touched_partitions.size() > 1 && estimate.isSinglePartitionProbabilitySet() == false) {
            if (t) LOG.trace("Setting the single-partition probability to current confidence [" + estimate.confidence + "]");
            estimate.setSinglePartitionProbability(inverse_prob);
        }
        
        // Keep track of the highest abort probability that we've seen thus far
        if (next_vertex.isQueryVertex() && next_vertex.getAbortProbability() > estimate.greatest_abort) {
            estimate.greatest_abort = next_vertex.getAbortProbability();
        }
    }
    
    protected static void populateMarkovEstimate(MarkovEstimate estimate, MarkovVertex vertex) {
        assert(vertex != null);
        if (d) LOG.debug("Populating internal properties based on current vertex\n" + vertex.debug());
        
        boolean is_singlepartition = (estimate.touched_partitions.size() == 1);
        float untouched_finish = 1.0f;
        float inverse_prob = 1.0f - estimate.confidence;
        for (int p = 0, cnt = estimate.getCatalogContext().numberOfPartitions; p < cnt; p++) {
            float finished_prob = vertex.getFinishProbability(p);
            if (estimate.touched_partitions.contains(p) == false) {
                estimate.setReadOnlyProbability(p, vertex.getReadOnlyProbability(p));
                estimate.setWriteProbability(p, vertex.getWriteProbability(p));
                if (is_singlepartition) untouched_finish = Math.min(untouched_finish, finished_prob);
            }
            if (estimate.isReadOnlyProbabilitySet(p) == false) {
                estimate.setReadOnlyProbability(p, vertex.getReadOnlyProbability(p));
            }
            if (estimate.isWriteProbabilitySet(p) == false) {
                estimate.setWriteProbability(p, vertex.getWriteProbability(p));
                // estimate.setWriteProbability(p, inverse_prob);
            }
            if (estimate.isFinishProbabilitySet(p) == false) {
                estimate.setFinishProbability(p, finished_prob);
            }
        } // FOR
        
        // Single-Partition Probability
        if (is_singlepartition) {
            if (t) LOG.trace(String.format("Only one partition was touched %s. Setting single-partition probability to ???",
                             estimate.touched_partitions)); 
            estimate.setSinglePartitionProbability(untouched_finish);
        } else {
            estimate.setSinglePartitionProbability(1.0f - untouched_finish);
        }
        
        // Abort Probability
        // Only use the abort probability if we have seen at least ABORT_MIN_TXNS
        if (vertex.getTotalHits() >= MarkovGraph.MIN_HITS_FOR_NO_ABORT) {
            if (estimate.greatest_abort == EstimatorUtil.NULL_MARKER) estimate.greatest_abort = 0.0f;
            estimate.setAbortProbability(estimate.greatest_abort);
        } else {
            estimate.setAbortProbability(1.0f);
        }
    }
    
    // ----------------------------------------------------------------------------
    // CONVENIENCE METHODS
    // ----------------------------------------------------------------------------
    
    public static void fastEstimation(MarkovEstimate estimate, List<MarkovVertex> initialPath, MarkovVertex current) {
        boolean add = false;
        for (MarkovVertex v : initialPath) {
            if (add || current.equals(v)) {
                MarkovPathEstimator.populateProbabilities(estimate, v);
                estimate.path.add(v);
                add = true;
            }
        } // FOR
        MarkovPathEstimator.populateMarkovEstimate(estimate, estimate.getVertex());
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
        File input_path = args.getFileParam(ArgumentsParser.PARAM_MARKOV);
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