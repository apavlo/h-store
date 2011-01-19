package edu.brown.markov;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.*;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;

/**
 * Path Estimator for TransactionEstimator
 * @author pavlo
 */
public class MarkovPathEstimator extends VertexTreeWalker<Vertex> {
    protected static final Logger LOG = Logger.getLogger(MarkovPathEstimator.class);

    private final TransactionEstimator t_estimator;
    private final PartitionEstimator p_estimator;
    private final int base_partition;
    private final Object args[];
    
    /**
     * If this flag is set to true, then we will always try to go to the end
     * This means that if we don't have an edge to the vertex that we're pretty sure we want to take, we'll 
     * just pick the edge from the one that is available that has the highest probability
     */
    private boolean force_traversal = false;
    
    /**
     * This is how confident we are 
     */
    private double confidence = 1.00;
    
    public MarkovPathEstimator(MarkovGraph markov, TransactionEstimator t_estimator, int base_partition, Object args[]) {
        super(markov);
        this.t_estimator = t_estimator;
        this.args = args;
        this.p_estimator = this.t_estimator.getPartitionEstimator();
        this.base_partition = base_partition;
        
        assert(this.t_estimator.getCorrelations() != null);
        assert(this.base_partition >= 0);
        
        if (LOG.isTraceEnabled()) {
            LOG.trace("Procedure:       " + markov.getProcedure().getName());
            LOG.trace("Base Partition:  " + this.base_partition);
            LOG.trace("# of Partitions: " + CatalogUtil.getNumberOfPartitions(this.p_estimator.getDatabase()));
//            LOG.trace("Arguments:       " + Arrays.toString(args));
        }
    }
    
    /**
     * 
     * @param flag
     */
    public void enableForceTraversal(boolean flag) {
        this.force_traversal = flag;
    }
    
    /**
     * Return the confidence factor that of our estimated path
     * @return
     */
    public double getConfidence() {
        return this.confidence;
    }
    
    /**
     * Conveinence method that returns the traversal path predicted for this instance
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
    
    /**
     * 
     */
    protected void populate_children(Children children, Vertex element) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        if (trace) LOG.trace("Current Vertex: " + element);
        Statement cur_catalog_stmt = element.getCatalogItem();
        int cur_catalog_stmt_index = element.getQueryInstanceIndex();
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        final SortedSet<Edge> candidates = new TreeSet<Edge>();
        
        // At our current vertex we need to gather all of our neighbors
        // and get unique Statements that we could be executing next
        Collection<Vertex> next_vertices = markov.getSuccessors(element);
        
        if (trace) LOG.trace("Successors: " + next_vertices);

        // Step #1
        // Get all of the unique Statement+StatementInstanceIndex pairs for the vertices
        // that are adjacent to our current vertex
        // XXX: Why do we use the pairs rather than just look at the vertices?
        Set<Pair<Statement, Integer>> next_statements = new HashSet<Pair<Statement, Integer>>();
        for (Vertex next : next_vertices) {
            Statement next_catalog_stmt = next.getCatalogItem();
            int next_catalog_stmt_index = next.getQueryInstanceIndex();
            
            // Sanity Check: If this vertex is the same Statement as the current vertex,
            // then its instance counter must be greater than the current vertex's counter
            if (next_catalog_stmt.equals(cur_catalog_stmt)) {
                assert(next_catalog_stmt_index > cur_catalog_stmt_index);
            }
            
            // Check whether it's COMMIT/ABORT
            if (next.equals(markov.getCommitVertex()) || next.equals(markov.getAbortVertex())) {
                Edge candidate = markov.findEdge(element, next);
                assert(candidate != null);
                candidates.add(candidate);
            } else {
                next_statements.add(Pair.of(next_catalog_stmt, next_catalog_stmt_index));
            }
        } // FOR
        
        // Now for the unique set of Statement+StatementIndex pairs, figure out which partitions
        // the queries will go to.
        for (Pair<Statement, Integer> pair : next_statements) {
            Statement catalog_stmt = pair.getFirst();
            Integer catalog_stmt_index = pair.getSecond();
            if (trace) LOG.trace("Examining " + pair);
            
            // Get the correlation objects (if any) for next
            // This is the only way we can predict what partitions we will touch
            SortedMap<StmtParameter, SortedSet<Correlation>> param_correlations = this.t_estimator.getCorrelations().get(catalog_stmt, catalog_stmt_index);
            if (param_correlations == null) {
                if (trace) LOG.trace("No parameter correlations for " + pair);
                System.err.println(this.t_estimator.getCorrelations().debug(catalog_stmt));
                continue;
            }
            
            // Go through the StmtParameters and map values from ProcParameters
            Object stmt_args[] = new Object[catalog_stmt.getParameters().size()];
            boolean stmt_args_set = false;
            for (int i = 0; i < stmt_args.length; i++) {
                StmtParameter catalog_stmt_param = catalog_stmt.getParameters().get(i);
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
                Set<Integer> partitions = null;
                try {
                    partitions = this.p_estimator.getPartitions(catalog_stmt, stmt_args, this.base_partition);
                } catch (Exception ex) {
                    String msg = "Failed to calculate partitions for " + catalog_stmt + " using parameters " + Arrays.toString(stmt_args);
                    LOG.error(msg, ex);
                    this.stop();
                    return;
                }
                if (trace) LOG.trace("Estimated Partitions for " + catalog_stmt + ": " + partitions);
                
                // Now for this given list of partitions, find a Vertex in our next set
                // that has the same partitions
                if (partitions != null && !partitions.isEmpty()) {
                    Edge candidate = null;
                    for (Vertex next : next_vertices) {
                        if (next.getCatalogItem().equals(catalog_stmt) &&
                            next.getQueryInstanceIndex() == catalog_stmt_index &&
                            next.getPartitions().equals(partitions)) {
                            // BINGO!!!
                            assert(candidate == null);
                            candidate = markov.findEdge(element, next);
                            assert(candidate != null);

                            candidates.add(candidate);
                            LOG.trace("Found candidate edge to " + next + " [" + candidate + "]");
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
        int num_candidates = candidates.size();
        if (num_candidates == 0 && this.force_traversal) {
            if (trace) LOG.trace("No candidate edges were found. Force travesal flag is set, so taking all");
            candidates.addAll(markov.getOutEdges(element));
            num_candidates = candidates.size();
        }
        
        // So now we have our list of candidate edges. We can pick the first one
        // since they will be sorted by their probability
        if (trace) LOG.trace("Candidate Edges: " + candidates);
        if (num_candidates > 0) {
            Edge next_edge = CollectionUtil.getFirst(candidates);
            Vertex next_vertex = markov.getOpposite(element, next_edge);
            children.addAfter(next_vertex);
            
            // Our confidence is based on the total sum of the probabilities for all of the
            // edges that we could have taken in comparison to the one that we did take
            float total_probability = 0.0f;
            if (debug) LOG.debug("CANDIDATES:");
            for (Edge e : candidates) {
                Vertex v = markov.getOpposite(element, e);
                total_probability += e.getProbability();
                if (debug) LOG.debug("  " + element + " --[" + e + "]--> " + v + (next_vertex.equals(v) ? " *******" : ""));
                if (debug && candidates.size() > 1) LOG.debug(StringUtil.addSpacers(v.debug()));
            } // FOR
            double next_probability = next_edge.getProbability();
            this.confidence *= next_probability / total_probability;
            
            if (debug) {
                LOG.debug("TOTAL:    " + total_probability);
                LOG.debug("SELECTED: " + next_vertex + " [confidence=" + this.confidence + "]");
            }
        } else {
            if (trace) LOG.trace("No matching children found. We have to stop...");
        }
        LOG.debug(StringUtil.repeat("-", 100));
    }
    
    @Override
    protected void callback(Vertex element) {
        final boolean trace = LOG.isTraceEnabled();
        
        // Anything??
        
        final MarkovGraph markov = (MarkovGraph)this.getGraph();
        if (markov.getCommitVertex().equals(element)) {
            if (trace) LOG.trace("Reached COMMIT. Stopping...");
            this.stop();
        } else if (markov.getAbortVertex().equals(element)) {
            if (trace) LOG.trace("Reached ABORT. Stopping...");
            this.stop();
        }
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
            markovs = MarkovUtil.load(args.catalog_db, args.getParam(ArgumentsParser.PARAM_MARKOV));
        } else {
            markovs = MarkovUtil.createBasePartitionGraphs(args.catalog_db, args.workload, p_estimator);
        }
        
        // Blah blah blah...
        Map<Integer, TransactionEstimator> t_estimators = new HashMap<Integer, TransactionEstimator>();
        for (Integer partition : CatalogUtil.getAllPartitionIds(args.catalog_db)) {
            t_estimators.put(partition, new TransactionEstimator(p_estimator, args.param_correlations));
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
            TransactionEstimator t_estimator = t_estimators.get(partition);
            
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
            MarkovUtil.save(markovs, args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
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