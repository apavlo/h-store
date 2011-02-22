package edu.brown.markov;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.voltdb.catalog.*;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.*;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.TransactionTrace;

/**
 * Path Estimator for TransactionEstimator
 * @author pavlo
 */
public class MarkovPathEstimator extends VertexTreeWalker<Vertex> {
    private static final Logger LOG = Logger.getLogger(MarkovPathEstimator.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * 
     * @author pavlo
     */
    public static class Factory extends BasePoolableObjectFactory {
        @Override
        public Object makeObject() throws Exception {
            MarkovPathEstimator path_estimator = new MarkovPathEstimator();
            return path_estimator;
        }
        public void passivateObject(Object obj) throws Exception {
            MarkovPathEstimator path_estimator = (MarkovPathEstimator)obj;
            path_estimator.finish();
        };
    };

    // ----------------------------------------------------------------------------
    // INVOCATION MEMBERS
    // ----------------------------------------------------------------------------
    
    private TransactionEstimator t_estimator;
    private ParameterCorrelations correlations;
    private PartitionEstimator p_estimator;
    private int base_partition;
    private Object args[];

    private final Set<Integer> all_partitions = new HashSet<Integer>();
    private final Set<Integer> read_partitions = new HashSet<Integer>();
    private final Set<Integer> write_partitions = new HashSet<Integer>();
    
    
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
    private final Set<Vertex> forced_vertices = new HashSet<Vertex>();
    
    /**
     * This is how confident we are 
     */
    private double confidence = 1.00;


    // ----------------------------------------------------------------------------
    // TEMPORARY TRAVERSAL MEMBERS
    // ----------------------------------------------------------------------------
    
    private final transient Set<Integer> past_partitions = new HashSet<Integer>();
    
    private final transient SortedSet<Edge> candidates = new TreeSet<Edge>();
    
    private final transient Set<Pair<Statement, Integer>> next_statements = new HashSet<Pair<Statement, Integer>>();
    
    private final transient Set<Integer> stmt_partitions = new HashSet<Integer>();
    
    private final transient Map<Statement, StmtParameter[]> stmt_params = new HashMap<Statement, StmtParameter[]>();
    
    private final transient Map<Statement, Object[]> stmt_param_arrays = new HashMap<Statement, Object[]>();
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------
    
    /**
     * Base Constructor
     */
    public MarkovPathEstimator() {
        super();
    }
    
    /**
     * 
     * @param markov
     * @param t_estimator
     * @param base_partition
     * @param args
     */
    public MarkovPathEstimator(MarkovGraph markov, TransactionEstimator t_estimator, int base_partition, Object args[]) {
        super(markov);
        this.init(markov, t_estimator, base_partition, args);
    }
    
    /**
     * 
     * @param markov
     * @param t_estimator
     * @param base_partition
     * @param args
     * @return
     */
    public MarkovPathEstimator init(MarkovGraph markov, TransactionEstimator t_estimator, int base_partition, Object args[]) {
        this.init(markov, TraverseOrder.DEPTH, Direction.FORWARD);
        this.t_estimator = t_estimator;
        this.p_estimator = this.t_estimator.getPartitionEstimator();
        this.correlations = this.t_estimator.getCorrelations();
        this.base_partition = base_partition;
        this.args = args;
        
        assert(this.t_estimator.getCorrelations() != null);
        assert(this.base_partition >= 0);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Procedure:       " + markov.getProcedure().getName());
            LOG.trace("Base Partition:  " + this.base_partition);
            LOG.trace("# of Partitions: " + CatalogUtil.getNumberOfPartitions(this.p_estimator.getDatabase()));
//            LOG.trace("Arguments:       " + Arrays.toString(args));
        }
        return (this);
    }
    
    @Override
    public void finish() {
        super.finish();
        this.all_partitions.clear();
        this.read_partitions.clear();
        this.write_partitions.clear();
        this.past_partitions.clear();
        this.forced_vertices.clear();
    }
    
    /**
     * 
     * @param flag
     */
    public void enableForceTraversal(boolean flag) {
        this.force_traversal = flag;
    }
    
    public Set<Integer> getAllPartitions() {
        return this.all_partitions;
    }
    public Set<Integer> getReadPartitions() {
        return this.read_partitions;
    }
    public Set<Integer> getWritePartitions() {
        return this.write_partitions;
    }
    
    /**
     * 
     * @return
     */
    public Set<Vertex> getForcedVertices() {
        return this.forced_vertices;
    }
    
    /**
     * Return the confidence factor that of our estimated path
     * @return
     */
    public double getConfidence() {
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
    
    private Object[] getStatementParamsArray(Statement catalog_stmt) {
        Object arr[] = this.stmt_param_arrays.get(catalog_stmt);
        int size = catalog_stmt.getParameters().size();
        if (arr == null) {
            arr = new Object[size];
            this.stmt_param_arrays.put(catalog_stmt, arr);
        } else {
            for (int i = 0; i < size; i++) arr[i] = null;
        }
        return (arr);
    }
    
    /**
     * This is the main part of where we figure out the path that this transaction will take
     */
    protected void populate_children(Children children, Vertex element) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        if (element.isAbortVertex() || element.isCommitVertex()) {
            this.stop();
            return;
        }
        
        // Initialize temporary data
        this.candidates.clear();
        this.next_statements.clear();
        this.past_partitions.addAll(element.getPartitions());
        
        if (trace) LOG.trace("Current Vertex: " + element);
        Statement cur_catalog_stmt = element.getCatalogItem();
        int cur_catalog_stmt_index = element.getQueryInstanceIndex();
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        
        // At our current vertex we need to gather all of our neighbors
        // and get unique Statements that we could be executing next
        Collection<Vertex> next_vertices = markov.getSuccessors(element);
        if (trace) LOG.trace("Successors: " + next_vertices);
        if (next_vertices == null) {
            this.stop();
            return;
        }

        // Step #1
        // Get all of the unique Statement+StatementInstanceIndex pairs for the vertices
        // that are adjacent to our current vertex
        // XXX: Why do we use the pairs rather than just look at the vertices?
        for (Vertex next : next_vertices) {
            Statement next_catalog_stmt = next.getCatalogItem();
            int next_catalog_stmt_index = next.getQueryInstanceIndex();
            
            // Sanity Check: If this vertex is the same Statement as the current vertex,
            // then its instance counter must be greater than the current vertex's counter
            if (next_catalog_stmt.equals(cur_catalog_stmt)) {
                if (next_catalog_stmt_index <= cur_catalog_stmt_index) {
                    LOG.error("CURRENT: " + element + "  [commit=" + element.isCommitVertex() + "]");
                    LOG.error("NEXT:    " + next + "  [commit=" + next.isCommitVertex() + "]");
                }
                assert(next_catalog_stmt_index > cur_catalog_stmt_index) : String.format("%d > %d", next_catalog_stmt_index, cur_catalog_stmt_index);
            }
            
            // Check whether it's COMMIT/ABORT
            if (next.isCommitVertex() || next.isAbortVertex()) {
                Edge candidate = markov.findEdge(element, next);
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
            if (trace) LOG.trace("Examining " + pair);
            
            // Get the correlation objects (if any) for next
            // This is the only way we can predict what partitions we will touch
            SortedMap<StmtParameter, SortedSet<Correlation>> param_correlations = this.correlations.get(catalog_stmt, catalog_stmt_index);
            if (param_correlations == null) {
                if (trace) {
                    LOG.warn("No parameter correlations for " + pair);
                    LOG.trace(this.correlations.debug(catalog_stmt));
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
                if (trace) LOG.trace("Examining " + CatalogUtil.getDisplayName(catalog_stmt_param, true));
                
                SortedSet<Correlation> correlations = param_correlations.get(catalog_stmt_param);
                if (correlations == null || correlations.isEmpty()) {
                    if (trace) LOG.trace("No parameter correlations for " + CatalogUtil.getDisplayName(catalog_stmt_param, true) + " from " + pair);
                    continue;
                }
                if (trace) LOG.trace("Found " + correlations.size() + " correlation(s) for " + CatalogUtil.getDisplayName(catalog_stmt_param, true));
        
                // Special Case:
                // If the number of possible Statements we could execute next is greater than one,
                // then we need to prune our list by removing those Statements who have a StmtParameter
                // that are correlated to a ProcParameter that doesn't exist (such as referencing an
                // array element that is greater than the size of that current array)
                // TODO: For now we are just going always pick the first Correlation 
                // that comes back. Is there any choice that we would need to make in order
                // to have a better prediction about what the transaction might do?
                if (correlations.size() > 1) {
                    if (debug) LOG.warn("Multiple parameter correlations for " + CatalogUtil.getDisplayName(catalog_stmt_param, true));
                    if (trace) {
                        int ctr = 0;
                        for (Correlation c : correlations) {
                            LOG.trace("[" + (ctr++) + "] Correlation: " + c);
                        } // FOR
                    }
                }
                for (Correlation c : correlations) {
                    if (trace) LOG.trace("Correlation: " + c);
                    ProcParameter catalog_proc_param = c.getProcParameter();
                    if (catalog_proc_param.getIsarray()) {
                        Object proc_inner_args[] = (Object[])args[c.getProcParameter().getIndex()];
                        if (trace) LOG.trace(CatalogUtil.getDisplayName(c.getProcParameter(), true) + " is an array: " + Arrays.toString(proc_inner_args));
                        
                        // TODO: If this Correlation references an array element that is not available for this
                        // current transaction, should we just skip this correlation or skip the entire query?
                        if (proc_inner_args.length <= c.getProcParameterIndex()) {
                            if (trace) LOG.trace("Unable to map parameters: " +
                                                 "proc_inner_args.length[" + proc_inner_args.length + "] <= " +
                                                 "c.getProcParameterIndex[" + c.getProcParameterIndex() + "]"); 
                            continue;
                        }
                        stmt_args[i] = proc_inner_args[c.getProcParameterIndex()];
                        stmt_args_set = true;
                        if (trace) LOG.trace("Mapped " + CatalogUtil.getDisplayName(c.getProcParameter()) + "[" + c.getProcParameterIndex() + "] to " +
                                             CatalogUtil.getDisplayName(catalog_stmt_param) + " [value=" + stmt_args[i] + "]");
                    } else {
                        stmt_args[i] = args[c.getProcParameter().getIndex()];
                        stmt_args_set = true;
                        if (trace) LOG.trace("Mapped " + CatalogUtil.getDisplayName(c.getProcParameter()) + " to " +
                                             CatalogUtil.getDisplayName(catalog_stmt_param) + " [value=" + stmt_args[i] + "]"); 
                    }
                    break;
                } // FOR (Correlation)
            } // FOR (StmtParameter)
                
            // If we set any of the stmt_args in the previous step, then we can throw it
            // to our good old friend the PartitionEstimator and see whether we can figure
            // things out for this Statement
            if (stmt_args_set) {
                if (trace) LOG.trace("Mapped StmtParameters: " + Arrays.toString(stmt_args));
                this.stmt_partitions.clear();
                try {
                    this.p_estimator.getAllPartitions(this.stmt_partitions, catalog_stmt, stmt_args, this.base_partition);
                } catch (Exception ex) {
                    String msg = "Failed to calculate partitions for " + catalog_stmt + " using parameters " + Arrays.toString(stmt_args);
                    LOG.error(msg, ex);
                    this.stop();
                    return;
                }
                if (trace) LOG.trace("Estimated Partitions for " + catalog_stmt + ": " + this.stmt_partitions);
                
                // Now for this given list of partitions, find a Vertex in our next set
                // that has the same partitions
                if (this.stmt_partitions != null && !this.stmt_partitions.isEmpty()) {
                    Edge candidate = null;
                    for (Vertex next : next_vertices) {
                        if (next.isEqual(catalog_stmt, this.stmt_partitions, this.past_partitions, catalog_stmt_index)) {
                            // BINGO!!!
                            assert(candidate == null);
                            candidate = markov.findEdge(element, next);
                            assert(candidate != null);

                            this.candidates.add(candidate);
                            if (trace) LOG.trace("Found candidate edge to " + next + " [" + candidate + "]");
                            break; // ???
                        }
                    } // FOR (Vertex
                    if (candidate == null && trace) LOG.trace("Failed to find candidate edge from " + element + " to " + pair);
                }
            // Without any stmt_args, there's nothing we can do here...
            } else {
                if (trace) LOG.trace("No stmt_args for " + pair + ". Skipping...");
            } // IF
        } // FOR
        
        // If we don't have any candidate edges and the FORCE TRAVERSAL flag is set, then we'll just
        // grab all of the edges from our currect vertex
        int num_candidates = this.candidates.size();
        boolean was_forced = false;
        if (num_candidates == 0 && this.force_traversal) {
            if (trace) LOG.trace("No candidate edges were found. Force travesal flag is set, so taking all");
            this.candidates.addAll(markov.getOutEdges(element));
            num_candidates = this.candidates.size();
            was_forced = true;
        }
        
        // So now we have our list of candidate edges. We can pick the first one
        // since they will be sorted by their probability
        if (trace) LOG.trace("Candidate Edges: " + this.candidates);
        if (num_candidates > 0) {
            Edge next_edge = CollectionUtil.getFirst(this.candidates);
            Vertex next_vertex = markov.getOpposite(element, next_edge);
            children.addAfter(next_vertex);
            if (was_forced) this.forced_vertices.add(next_vertex);
 
            // Update our list of partitions touched by this transaction
            Statement catalog_stmt = next_vertex.getCatalogItem();
            if (catalog_stmt.getQuerytype() == QueryType.SELECT.getValue()) {
                this.read_partitions.addAll(next_vertex.getPartitions());
            } else {
                this.write_partitions.addAll(next_vertex.getPartitions());
            }
            this.all_partitions.addAll(next_vertex.getPartitions());
            
            // Our confidence is based on the total sum of the probabilities for all of the
            // edges that we could have taken in comparison to the one that we did take
            double total_probability = 0.0;
            if (debug) LOG.debug("CANDIDATES:");
            for (Edge e : this.candidates) {
                Vertex v = markov.getOpposite(element, e);
                total_probability += e.getProbability();
                if (debug) {
                    LOG.debug("  " + element + " --[" + e + "]--> " + v + (next_vertex.equals(v) ? " *******" : ""));
                    if (this.candidates.size() > 1) LOG.debug(StringUtil.addSpacers(v.debug()));
                }
            } // FOR
            this.confidence *= next_edge.getProbability() / total_probability;
            
            if (debug) {
                LOG.debug("TOTAL:    " + total_probability);
                LOG.debug("SELECTED: " + next_vertex + " [confidence=" + this.confidence + "]");
            }
        } else {
            if (trace) LOG.trace("No matching children found. We have to stop...");
        }
        if (debug) LOG.debug(StringUtil.repeat("-", 100));
    }
    
    @Override
    protected void callback(Vertex element) {
        final boolean trace = LOG.isTraceEnabled();
        
        if (element.isCommitVertex()) {
            if (trace) LOG.trace("Reached COMMIT. Stopping...");
            this.stop();
        } else if (element.isAbortVertex()) {
            if (trace) LOG.trace("Reached ABORT. Stopping...");
            this.stop();
        }
    }
    
    
    /**
     * Convenience method that returns the traversal path predicted for this instance
     * @param markov
     * @param t_estimator
     * @param args
     * @return
     */
    public static List<Vertex> predictPath(MarkovGraph markov, TransactionEstimator t_estimator, Object args[]) {
        Integer base_partition = null; 
        try {
            base_partition = t_estimator.getPartitionEstimator().getBasePartition(markov.getProcedure(), args);
        } catch (Exception ex) {
            LOG.fatal(String.format("Failed to calculate base partition for <%s, %s>", markov.getProcedure().getName(), Arrays.toString(args)), ex);
            System.exit(1);
        }
        assert(base_partition != null);
        
        MarkovPathEstimator estimator = new MarkovPathEstimator(markov, t_estimator, base_partition, args);
        estimator.traverse(markov.getStartVertex());
        return (new Vector<Vertex>(estimator.getVisitPath()));
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_CORRELATIONS
        );
        
        // Word up
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db);
        
        // Create MarkovGraphsContainer
        MarkovGraphsContainer markovs = null;
        
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
            markovs = new MarkovGraphsContainer();
            markovs.load(args.getParam(ArgumentsParser.PARAM_MARKOV), args.catalog_db);
        } else {
            markovs = MarkovUtil.createBasePartitionGraphs(args.catalog_db, args.workload, p_estimator);
        }
        
        // Blah blah blah...
        TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, args.param_correlations, markovs);
        
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
            Procedure catalog_proc = xact.getCatalogItem(args.catalog_db);
            if (skip.contains(catalog_proc.getName())) continue;
            
            int partition = -1;
            try {
                partition = p_estimator.getBasePartition(catalog_proc, xact.getParams(), true);
            } catch (Exception ex) {
                ex.printStackTrace();
                assert(false);
            }
            assert(partition >= 0);
            totals.get(catalog_proc).incrementAndGet();
            
            MarkovGraph markov = markovs.get(partition, catalog_proc);
            if (markov == null) continue;
            
            // Check whether we predict the same path
            List<Vertex> actual_path = markov.processTransaction(xact, p_estimator);
            List<Vertex> predicted_path = MarkovPathEstimator.predictPath(markov, t_estimator, xact.getParams());
            if (actual_path.equals(predicted_path)) correct_path_txns.get(catalog_proc).incrementAndGet();
            
            // Check whether we predict the same partitions
            Set<Integer> actual_partitions = MarkovUtil.getTouchedPartitions(actual_path); 
            Set<Integer> predicted_partitions = MarkovUtil.getTouchedPartitions(predicted_path);
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
        
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV_OUTPUT)) {
            markovs.save(args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
        }
        
        System.err.println("Procedure\t\tTotal\tSingleP\tPartitions\tPaths");
        for (Procedure catalog_proc : totals.keySet()) {
            int total = totals.get(catalog_proc).get();
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