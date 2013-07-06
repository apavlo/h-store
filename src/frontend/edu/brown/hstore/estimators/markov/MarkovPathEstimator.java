package edu.brown.hstore.estimators.markov;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.CountedStatement;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovVertex;
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.uci.ics.jung.graph.util.EdgeType;

/**
 * Path Estimator for MarkovEstimator
 * @author pavlo
 */
public class MarkovPathEstimator extends VertexTreeWalker<MarkovVertex, MarkovEdge> {
    private static final Logger LOG = Logger.getLogger(MarkovPathEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
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
    private Object procParams[];

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
    private Set<MarkovVertex> forced_vertices;
    
    /**
     * If this flag is set to true, then the estimator will be allowed to create vertices
     * that it knows that it needs to transition to but do not exist.
     */
    private boolean learning_enabled = false;
    
    /**
     * These are the vertices that we weren't sure about.
     * This only gets populated when force_traversal is set to true.
     * This is primarily used for debugging.
     */
    private Set<MarkovVertex> created_vertices;

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
     * Constructor
     * @param catalogContext
     * @param p_estimator
     */
    public MarkovPathEstimator(CatalogContext catalogContext, PartitionEstimator p_estimator) {
        super();
        this.num_partitions = catalogContext.numberOfPartitions;
        this.p_estimator = p_estimator;
        this.allMappings = catalogContext.paramMappings;
        assert(this.allMappings != null);
    }

    /**
     * Initialize this MarkovPathEstimator for a new traversal run.
     * @param markov The MarkovGraph to use for this txn.
     * @param estimate The MarkovEstimate to populate while we traverse the model.
     * @param procParams The txn's Procedure input parameters
     * @param base_partition The txn's bases partition.
     * @return
     */
    public MarkovPathEstimator init(MarkovGraph markov, MarkovEstimate estimate, Object procParams[], int base_partition) {
        this.init(markov, TraverseOrder.DEPTH, Direction.FORWARD);
        this.estimate = estimate;
        this.base_partition = base_partition;
        this.procParams = procParams;
        assert(this.base_partition >= 0);

        if (trace.val) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Procedure", markov.getProcedure().getName());
            m.put("Base Partition", this.base_partition);
            m.put("# of Partitions", this.num_partitions);
            m.put("Parameters", StringUtil.toString(this.procParams, true, true));
            m.put("Force Traversal", this.force_traversal);
            m.put("Auto Learning", this.learning_enabled);
            
            LOG.trace(String.format("Initialized %s [hashCode=%d]\n%s",
                      this.getClass().getSimpleName(), this.hashCode(),
                      StringUtil.formatMaps(m)));
        }
        return (this);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.estimate != null);
    }
    
    @Override
    public void finish() {
        if (debug.val)
            LOG.debug(String.format("Cleaning up MarkovPathEstimator [hashCode=%d]",
                      this.hashCode()));
        super.finish();
        this.estimate = null;
        this.past_partitions.clear();
        this.stmt_partitions.clear();
        if (this.forced_vertices != null) this.forced_vertices.clear();
        if (this.created_vertices != null) this.created_vertices.clear();
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
    /**
     * Can be null.
     * @return
     */
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
    public void setLearningEnabled(boolean flag) {
        this.learning_enabled = flag;
    }
    /**
     * Can be null
     * @return
     */
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
        
        if (trace.val) LOG.trace("Current Vertex: " + element);
        Statement cur_catalog_stmt = element.getCatalogItem();
        int cur_catalog_stmt_index = element.getQueryCounter();
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        
        // At our current vertex we need to gather all of our neighbors
        // and get unique Statements that we could be executing next
        Collection<MarkovVertex> next_vertices = markov.getSuccessors(element);
        if (next_vertices == null || next_vertices.isEmpty()) {
            if (debug.val) LOG.debug("No succesors were found for " + element + ". Halting traversal");
            return;
        }
        if (trace.val) LOG.trace("Successors: " + next_vertices);
        
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
            if (debug.val) LOG.debug("Examining " + cstmt);
            
            // Get the mapping objects (if any) for next
            // This is the only way we can predict what partitions we will touch
            Map<StmtParameter, SortedSet<ParameterMapping>> stmtMappings = this.allMappings.get(catalog_stmt, catalog_stmt_index);
            if (stmtMappings == null) {
                if (debug.val) {
                    LOG.warn("No parameter mappings for " + catalog_stmt);
                    if (trace.val) LOG.trace(this.allMappings.debug(catalog_stmt));
                }
                continue;
            }
            
            // Go through the StmtParameters and map values from ProcParameters
            StmtParameter stmt_params[] = catalog_stmt.getParameters().values();
            Object stmt_args[] = new Object[stmt_params.length]; // this.getStatementParamsArray(catalog_stmt);
            boolean stmt_args_set = false;
            
            // XXX: This method may return null because it's being used for other
            // purposes in the BatchPlanner.
            int stmt_args_offsets[] = this.p_estimator.getStatementEstimationParameters(catalog_stmt);
            if (stmt_args_offsets == null) {
                stmt_args_offsets = new int[stmt_args.length];
                for (int i = 0; i < stmt_args.length; i++)
                    stmt_args_offsets[i] = i;
            }
            assert(stmt_args_offsets != null) :
                "Unexpected null StmtParameter offsets for " + catalog_stmt.fullName();
            for (int offset : stmt_args_offsets) {
                StmtParameter catalog_stmt_param = stmt_params[offset];
                assert(catalog_stmt_param != null);
                if (trace.val)
                    LOG.trace("Retrieving ParameterMappings for " + catalog_stmt_param.fullName());
                
                Collection<ParameterMapping> mappings = stmtMappings.get(catalog_stmt_param);
                if (mappings == null || mappings.isEmpty()) {
                    if (trace.val)
                        LOG.trace("No parameter mappings exists for " + catalog_stmt_param.fullName());
                    continue;
                }
                if (debug.val)
                    LOG.debug("Found " + mappings.size() + " mapping(s) for " + catalog_stmt_param.fullName());
        
                // Special Case:
                // If the number of possible Statements we could execute next is greater than one,
                // then we need to prune our list by removing those Statements who have a StmtParameter
                // that are correlated to a ProcParameter that doesn't exist (such as referencing an
                // array element that is greater than the size of that current array)
                // TODO: For now we are just going always pick the first mapping 
                // that comes back. Is there any choice that we would need to make in order
                // to have a better prediction about what the transaction might do?
                if (debug.val && mappings.size() > 1) {
                    LOG.warn("Multiple parameter mappings for " + catalog_stmt_param.fullName());
                    if (trace.val) {
                        int ctr = 0;
                        for (ParameterMapping m : mappings) {
                            LOG.trace("[" + (ctr++) + "] Mapping: " + m);
                        } // FOR
                    }
                }
                for (ParameterMapping m : mappings) {
                    if (trace.val) LOG.trace("Mapping: " + m);
                    ProcParameter catalog_proc_param = m.getProcParameter();
                    if (catalog_proc_param.getIsarray()) {
                        Object proc_inner_args[] = (Object[])procParams[m.getProcParameter().getIndex()];
                        if (trace.val)
                            LOG.trace(CatalogUtil.getDisplayName(m.getProcParameter(), true) + " is an array: " + 
                                      Arrays.toString(proc_inner_args));
                        
                        // TODO: If this Mapping references an array element that is not available for this
                        // current transaction, should we just skip this mapping or skip the entire query?
                        if (proc_inner_args.length <= m.getProcParameterIndex()) {
                            if (trace.val)
                                LOG.trace("Unable to map parameters: " +
                                          "proc_inner_args.length[" + proc_inner_args.length + "] <= " +
                                          "c.getProcParameterIndex[" + m.getProcParameterIndex() + "]"); 
                            continue;
                        }
                        stmt_args[offset] = proc_inner_args[m.getProcParameterIndex()];
                        stmt_args_set = true;
                        if (trace.val)
                            LOG.trace("Mapped " + CatalogUtil.getDisplayName(m.getProcParameter()) + "[" + m.getProcParameterIndex() + "] to " +
                                      CatalogUtil.getDisplayName(catalog_stmt_param) + " [value=" + stmt_args[offset] + "]");
                    } else {
                        stmt_args[offset] = procParams[m.getProcParameter().getIndex()];
                        stmt_args_set = true;
                        if (trace.val)
                            LOG.trace("Mapped " + CatalogUtil.getDisplayName(m.getProcParameter()) + " to " +
                                      CatalogUtil.getDisplayName(catalog_stmt_param) + " [value=" + stmt_args[offset] + "]"); 
                    }
                    break;
                } // FOR (Mapping)
            } // FOR (StmtParameter)
                
            // If we set any of the stmt_args in the previous step, then we can throw it
            // to our good old friend the PartitionEstimator and see whether we can figure
            // things out for this Statement
            if (stmt_args_set) {
                if (trace.val)
                    LOG.trace("Mapped StmtParameters: " + Arrays.toString(stmt_args));
                this.stmt_partitions.clear();
                try {
                    this.p_estimator.getAllPartitions(this.stmt_partitions, catalog_stmt, stmt_args, this.base_partition);
                } catch (Exception ex) {
                    String msg = "Failed to calculate partitions for " + catalog_stmt + " using parameters " + Arrays.toString(stmt_args);
                    LOG.error(msg, ex);
                    this.stop();
                    return;
                }
                if (trace.val)
                    LOG.trace("Estimated Partitions for " + catalog_stmt + ": " + this.stmt_partitions);
                
                // Now for this given list of partitions, find a Vertex in our next set
                // that has the same partitions
                if (this.stmt_partitions.isEmpty() == false) {
                    candidate_edge = null;
                    if (trace.val)
                        LOG.trace("Partitions:" + this.stmt_partitions + " / Past:" + this.past_partitions);
                    for (MarkovVertex next_v : next_vertices) {
                        if (trace.val) LOG.trace("Checking whether " + next_v + " is the correct transition");
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
                            if (trace.val)
                                LOG.trace("Found candidate edge to " + next_v + " [" + candidate_edge + "]");
                            break;
                        } else if (trace.val) { 
                            Map<String, Object> m = new LinkedHashMap<String, Object>();
                            m.put("stmt", next_v.getCatalogItem().equals(catalog_stmt));
                            m.put("stmtCtr", next_v.getQueryCounter() == catalog_stmt_index);
                            m.put("partitions", next_v.getPartitions().equals(this.stmt_partitions));
                            m.put("past", next_v.getPastPartitions().equals(this.past_partitions));
                            LOG.trace("Invalid candidate transition:\n" + StringUtil.formatMaps(m));
                        }
                    } // FOR (Vertex
                    if (trace.val && candidate_edge == null)
                        LOG.trace(String.format("Failed to find candidate edge from %s to %s [partitions=%s]",
                                  element, catalog_stmt.fullName(), this.stmt_partitions)); 
                }
            }
            // Without any stmt_args, there's nothing we can do here...
            else if (trace.val) { 
                LOG.trace("No stmt_args for " + catalog_stmt + ". Skipping...");
            } // IF
        } // FOR
        
        // If we don't have any candidate edges and the FORCE TRAVERSAL flag is set, then we'll just
        // grab all of the edges from our current vertex
        int num_candidates = this.candidate_edges.size();
        boolean was_forced = false;
        if (num_candidates == 0 && this.force_traversal) {
            if (debug.val)
                LOG.debug(String.format("No candidate edges were found. " +
            		      "Checking whether we can create our own. [nextStatements=%s]",
            		      this.next_statements));
            
            // We're allow to create the vertices that we know are missing
            if (this.learning_enabled && this.next_statements.size() == 1) {
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
                
                if (this.created_vertices == null) this.created_vertices = new HashSet<MarkovVertex>();
                this.created_vertices.add(v);
                if (trace.val)
                    LOG.trace(String.format("Created new vertex %s and connected it to %s", v, element));
                
                // 2012-10-21
                // The problem with allowing the estimator to create a new vertex is that 
                // we don't know what it's children are going to be. That means that when
                // we invoke this method again at the next vertex (the one we just made above)
                // then it's not going to have any children, so we don't know what it's
                // going to do. We are actually better off with just grabbing the next best
                // vertex from the existing edges and then updating the graph after 
                // the txn has finished, since now we know exactly what it did.
                
            }
            // Otherwise we'll just make all of the outbound edges from the
            // current vertex be our candidates
            else {
                if (trace.val)
                    LOG.trace("No candidate edges were found. Force travesal flag is set to true, so taking all");
                Collection<MarkovEdge> out_edges = markov.getOutEdges(element);
                if (out_edges != null) this.candidate_edges.addAll(out_edges);
            }
            num_candidates = this.candidate_edges.size();
            was_forced = true;
        }
        
        // So now we have our list of candidate edges. We can pick the first one
        // since they will be sorted by their probability
        if (trace.val) LOG.trace("Candidate Edges: " + this.candidate_edges);
        if (num_candidates > 0) {
            MarkovEdge next_edge = CollectionUtil.first(this.candidate_edges);
            assert(next_edge != null) : "Unexpected null edge " + this.candidate_edges;
            MarkovVertex next_vertex = markov.getOpposite(element, next_edge);
            children.addAfter(next_vertex);
            if (was_forced) {
                if (this.forced_vertices == null) this.forced_vertices = new HashSet<MarkovVertex>();
                this.forced_vertices.add(next_vertex);
            }

            if (debug.val) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("#%02d CANDIDATES:\n", this.getDepth()));
                int i = 0;
                for (MarkovEdge e : this.candidate_edges) {
                    MarkovVertex v = markov.getOpposite(element, e);
                    sb.append(String.format("  [%d] %s  --[%s]--> %s%s%s",
                              i++, element, e, v,
                              (next_vertex.equals(v) ? " <== SELECTED" : ""),
                              (trace.val && this.candidate_edges.size() > 1 ? "\n"+StringUtil.addSpacers(v.debug()) : "")));
                } // FOR
                LOG.debug(sb.toString());
            } // DEBUG
            
            // If there was only one next Statement that we could possibly execute here,
            // and if our ParameterMappings allowed us to know exactly what path we took,
            // then we don't need to compute the confidence based on the candidate edges.
            // We know that our confidence here is one!
            if (was_forced == false && this.next_statements.size() == 1 && num_candidates == 1) {
                // Nothing to do!
            }
            // Otherwise, our confidence is based on the total sum of the probabilities for all of the
            // edges that we could have taken in comparison to the one that we did take.
            else {
                double total_probability = 0.0;
                for (MarkovEdge e : this.candidate_edges) {
                    total_probability += e.getProbability();
                } // FOR
                this.estimate.confidence *= next_edge.getProbability() / total_probability;
                if (debug.val) LOG.debug("TOTAL:    " + total_probability);
            }
            
            // Update our list of partitions touched by this transaction
            MarkovPathEstimator.populateProbabilities(this.estimate, next_vertex);
            
            if (debug.val) {
                LOG.debug("SELECTED: " + next_vertex + " [confidence=" + this.estimate.confidence + "]");
                LOG.debug(StringUtil.repeat("-", 150));
            }
        } else {
            if (trace.val) LOG.trace("No matching children found. We have to stop...");
        }
    }
    
    @Override
    protected void callback(MarkovVertex v) {
        this.estimate.path.add(v);
        if (v.isQueryVertex() == false) {
            if (v.isCommitVertex()) {
                if (trace.val) LOG.trace("Reached COMMIT. Stopping...");
                this.stop();
            } else if (v.isAbortVertex()) {
                if (trace.val) LOG.trace("Reached ABORT. Stopping...");
                this.stop();
            }
        }
    }
    
    
    @Override
    protected void callback_finish() {
        MarkovPathEstimator.populateMarkovEstimate(this.estimate, this.estimate.getVertex());
    }
    
    // ----------------------------------------------------------------------------
    // PROBABILITY CALCULATION METHODS
    // ----------------------------------------------------------------------------
    
    protected static void populateProbabilities(MarkovEstimate estimate, MarkovVertex vertex) {
        if (debug.val)
            LOG.debug(String.format("Populating %s probabilities based on %s " +
                      "[touchedPartitions=%s, confidence=%.03f, hashCode=%d]%s",
                      estimate.getClass().getSimpleName(), vertex.getClass().getSimpleName(),
                      estimate.touched_partitions, estimate.confidence, estimate.hashCode(),
                      (trace.val ? "\n"+vertex.debug() : "")));
        
        Statement catalog_stmt = vertex.getCatalogItem();
        PartitionSet partitions = vertex.getPartitions();
        boolean readQuery = (catalog_stmt.getQuerytype() == QueryType.SELECT.getValue());
        for (int partition : partitions.values()) {
            if (estimate.isDoneProbabilitySet(partition) == false) {
                estimate.setDoneProbability(partition, vertex.getDoneProbability(partition));
            }
            if (estimate.isWriteProbabilitySet(partition) == false) {
                estimate.setWriteProbability(partition, vertex.getWriteProbability(partition));
            }
            (readQuery ? estimate.read_partitions : estimate.write_partitions).add(partition);
            estimate.incrementTouchedCounter(partition);
            estimate.touched_partitions.add(partition);
        } // FOR
        // Make sure that we update our probabilities for any partition that we've touched
        // in the past but are not touching for this query
        for (int partition : vertex.getPastPartitions()) {
            if (partitions.contains(partition) == false) {
                if (estimate.isDoneProbabilitySet(partition) == false) {
                    estimate.setDoneProbability(partition, vertex.getDoneProbability(partition));
                }
                if (estimate.isWriteProbabilitySet(partition) == false) {
                    estimate.setWriteProbability(partition, vertex.getWriteProbability(partition));
                }   
            }
        } // FOR
        
     // float inverse_prob = 1.0f - estimate.confidence;
//        // READ QUERY
//        if (catalog_stmt.getQuerytype() == QueryType.SELECT.getValue()) {
//            for (int partition : next_partitions.values()) {
//                // This is the first time we've read from this partition
//                if (estimate.read_partitions.contains(partition) == false) {
//                    if (trace.val)
//                        LOG.trace(String.format("First time partition %d is read from! " +
//                        		  "Setting read-only probability to %.03f",
//                                  partition, estimate.confidence));
////                    estimate.setReadOnlyProbability(p, estimate.confidence);
//                    if (estimate.isDoneProbabilitySet(partition) == false) {
//                        estimate.setDoneProbability(partition, inverse_prob);
//                    }
//                    estimate.read_partitions.add(partition);
//                }
//                estimate.incrementTouchedCounter(partition);
//            } // FOR
//        }
//        // WRITE QUERY
//        else {
//            for (int partition : next_partitions.values()) {
//                // This is the first time we've written to this partition
//                if (estimate.write_partitions.contains(partition) == false) {
//                    if (trace.val)
//                        LOG.trace(String.format("First time partition %d is written to! " +
//                        		  "Setting write probability to %.03f",
//                                  partition, estimate.confidence));
////                    estimate.setReadOnlyProbability(p, inverse_prob);
//                    estimate.setWriteProbability(partition, estimate.confidence);
//                    if (estimate.isDoneProbabilitySet(partition) == false) {
//                        estimate.setDoneProbability(partition, inverse_prob);
//                    }
//                    estimate.write_partitions.add(partition);
//                }
//                estimate.incrementTouchedCounter(partition);
//            } // FOR
//        }
        
        // Keep track of the highest abort probability that we've seen thus far
        if (vertex.isQueryVertex() && vertex.getAbortProbability() > estimate.greatest_abort) {
            estimate.greatest_abort = vertex.getAbortProbability();
        }
    }
    
    /**
     * Copy the MarkovVertex probabilities into the given MarkovEstimate
     * @param estimate
     * @param vertex
     */
    protected static void populateMarkovEstimate(MarkovEstimate estimate, MarkovVertex vertex) {
        assert(vertex != null);
        if (debug.val)
            LOG.debug(String.format("Populating %s internal properties based on current %s " +
            		  "[touchedPartitions=%s, confidence=%f]",
            		  estimate.getClass().getSimpleName(), vertex.getClass().getSimpleName(),
            		  estimate.touched_partitions, estimate.confidence));
        
//        float untouched_finish = 1.0f;
        // float inverse_prob = 1.0f - estimate.confidence;
        
        // We need to loop through all possible partitions and make sure
        // that they all have a probability here.
        for (int partition : estimate.getCatalogContext().getAllPartitionIds().values()) {
            if (estimate.isDoneProbabilitySet(partition) == false) {
                estimate.setDoneProbability(partition, vertex.getDoneProbability(partition));
            }
            if (estimate.isWriteProbabilitySet(partition) == false) {
                estimate.setWriteProbability(partition, vertex.getWriteProbability(partition));
            }
//            estimate.setReadOnlyProbability(partition, vertex.getReadOnlyProbability(partition));
        } // FOR
        
        // If our single-partition probability hasn't been set and we can set it now
//        if (estimate.isSinglePartitionProbabilitySet() == false) {
//            if (estimate.touched_partitions.size() == 1) {
//                estimate.setSinglePartitionProbability(estimate.confidence);
//            } else {
//                estimate.setSinglePartitionProbability(1f - estimate.confidence);
//            }
//            if (debug.val)
//                LOG.debug(String.format("Setting single-partition probability to %s [touchedPartitions=%s]",
//                          estimate.getSinglePartitionProbability(), estimate.touched_partitions));
//        }
        
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
        if (debug.val)
            LOG.debug(String.format("Fast Estimation for %s [hashCode=%d]\n%s",
                      estimate.getClass().getSimpleName(), estimate.hashCode(), 
                      estimate.toString()));
        
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
        estimator.init(markov, est, args, base_partition);
        estimator.traverse(markov.getStartVertex());
        return (est);
    }
}