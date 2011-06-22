package edu.brown.markov;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.graphs.VertexTreeWalker.Direction;
import edu.brown.graphs.VertexTreeWalker.TraverseOrder;
import edu.brown.markov.Vertex.Type;
import edu.brown.markov.benchmarks.TPCCMarkovGraphsContainer;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * Markov Model Graph
 * @author svelagap
 * @author pavlo
 */
public class MarkovGraph extends AbstractDirectedGraph<Vertex, Edge> implements Comparable<MarkovGraph> {
    private static final long serialVersionUID = 3548405718926801012L;
    private static final Logger LOG = Logger.getLogger(MarkovGraph.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * If this global parameter is set to true, then all of the MarkovGraphs will evaluate the 
     * past partitions set at each vertex. If this is parameter is false, then all of the calculations
     * to determine the uniqueness of vertices will not include past partitions. 
     */
    public static final boolean USE_PAST_PARTITIONS = true;
    
    protected final Procedure catalog_proc;

    /**
     * 
     */
    private transient int xact_count = 0;
    private transient int xact_mispredict = 0;
    private transient double xact_accuracy = 1.0;

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param catalog_proc
     * @param basePartition
     */
//    public MarkovGraph(Procedure catalog_proc, int xact_count) {
//        super((Database) catalog_proc.getParent());
//        this.catalog_proc = catalog_proc;
//        this.xact_count = xact_count;
//    }
    public MarkovGraph(Procedure catalog_proc){
        super((Database) catalog_proc.getParent());
        this.catalog_proc = catalog_proc;
    }
    
    /**
     * Add the START, COMMIT, and ABORT vertices to the current graph
     */
    public MarkovGraph initialize() {
        for (Vertex.Type type : Vertex.Type.values()) {
            switch (type) {
                case START:
                case COMMIT:
                case ABORT:
                    Vertex v = MarkovUtil.getSpecialVertex(this.getDatabase(), type);
                    assert(v != null);
                    this.addVertex(v);
                    break;
                default:
                    // IGNORE
            } // SWITCH
        }
        return (this);
    }
    
    /**
     * Returns true if this graph has been initialized
     * @return
     */
    public boolean isInitialized() {
        boolean ret;
        if (cache_isInitialized == null) {
            ret = (this.vertices.isEmpty() == false);
            if (ret) this.cache_isInitialized = ret;
        } else {
            ret = this.cache_isInitialized.booleanValue();
        }
        return (ret);
    }
    private Boolean cache_isInitialized = null;
    
    @Override
    public String toString() {
//        return (this.getClass().getSimpleName() + "<" + this.getProcedure().getName() + ", " + this.getBasePartition() + ">");
        return (this.getClass().getSimpleName() + "<" + this.getProcedure().getName() + ">");
    }
    
    @Override
    public int compareTo(MarkovGraph o) {
        assert(o != null);
        return (this.catalog_proc.compareTo(o.catalog_proc));
    }

    // ----------------------------------------------------------------------------
    // CACHED METHODS
    // ----------------------------------------------------------------------------

    /**
     * Cached references to the special marker vertices
     */
    private transient final Vertex cache_specialVertices[] = new Vertex[Vertex.Type.values().length];

    private transient final Map<Statement, Set<Vertex>> cache_stmtVertices = new HashMap<Statement, Set<Vertex>>();
    private transient final Map<Vertex, Collection<Vertex>> cache_getSuccessors = new ConcurrentHashMap<Vertex, Collection<Vertex>>();
    
    @Override
    public Collection<Vertex> getSuccessors(Vertex vertex) {
        Collection<Vertex> successors = this.cache_getSuccessors.get(vertex);
        if (successors == null) {
            successors = super.getSuccessors(vertex);
            this.cache_getSuccessors.put(vertex, successors);
        }
        return (successors);
    }
    
    protected void buildCache() {
        for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
            if (this.cache_stmtVertices.containsKey(catalog_stmt) == false)
                this.cache_stmtVertices.put(catalog_stmt, new HashSet<Vertex>());
        } // FOR
        for (Vertex v : this.getVertices()) {
            if (v.isQueryVertex()) {
                Statement catalog_stmt = v.getCatalogItem();
                this.cache_stmtVertices.get(catalog_stmt).add(v);
            }
        } // FOR
        
    }
    
    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the Procedure catalog object that this Markov graph represents
     * @return
     */
    public Procedure getProcedure() {
        return catalog_proc;
    }

    /**
     * Increases the weight between two vertices. Creates an edge if one does
     * not exist, then increments the source vertex's count and the edge's count
     * 
     * @param source the source vertex
     * @param dest the destination vertex
     */
    public Edge addToEdge(Vertex source, Vertex dest) {
        assert(source != null);
        assert(dest != null);
        
        Edge e = null;
        synchronized (source) {
            e = this.findEdge(source, dest);
            if (e == null) {
                e = new Edge(this);
                this.addEdge(e, source, dest);
            }
            source.increment();
            e.increment();
        } // SYNCH
        return (e);
    }
    
    /**
     * 
     */
    @Override
    public boolean addVertex(Vertex v) {
        boolean ret;
        synchronized (v) {
            ret = super.addVertex(v);
            if (ret) {
                if (v.isQueryVertex()) {
                    Set<Vertex> stmt_vertices = this.cache_stmtVertices.get(v.getCatalogItem());
                    if (stmt_vertices == null) {
                        stmt_vertices = new HashSet<Vertex>();
                        this.cache_stmtVertices.put((Statement)v.getCatalogItem(), stmt_vertices);
                    }
                    stmt_vertices.add(v);
                } else {
                    Vertex.Type vtype = v.getType();
                    int idx = vtype.ordinal();
                    assert(this.cache_specialVertices[idx] == null) : "Trying add duplicate " + vtype + " vertex";
                    this.cache_specialVertices[idx] = v;
                }
            }
        } // SYNCH
        return (ret);
    }

    /**
     * For the given Vertex type, return the special vertex
     * @param vtype - the Vertex type (cannot be a regular query)
     * @return the single vertex for that type  
     */
    protected Vertex getSpecialVertex(Vertex.Type vtype) {
        assert(vtype != Vertex.Type.QUERY);
        Vertex v = null;
        switch (vtype) {
            case START:
            case COMMIT:
            case ABORT: {
                v = this.cache_specialVertices[vtype.ordinal()];
                assert(v != null) : "The special vertex for type " + vtype + " is null";
                break;
            }
            default:
                // Ignore others
        } // SWITCH
        return (v);
    }

    /**
     * Get the start vertex for this MarkovGraph
     * @return
     */
    public final Vertex getStartVertex() {
        return (this.getSpecialVertex(Vertex.Type.START));
    }
    /**
     * Get the stop vertex for this MarkovGraph
     * @return
     */
    public final Vertex getCommitVertex() {
        return (this.getSpecialVertex(Vertex.Type.COMMIT));
    }
    /**
     * Get the abort vertex for this MarkovGraph
     * @return
     */
    public final Vertex getAbortVertex() {
        return (this.getSpecialVertex(Vertex.Type.ABORT));
    }
    
    /**
     * Get the vertex based on it's unique identifier. This is a combination of
     * the query, the partitions the query is touching and where the query is in
     * the transaction
     * 
     * @param a
     * @param partitions
     *            set of partitions this query is touching
     * @param queryInstanceIndex
     *            query's location in transactiontrace
     * @return
     */
    protected Vertex getVertex(Statement a, Set<Integer> partitions, Set<Integer> past_partitions, int queryInstanceIndex) {
        Set<Vertex> stmt_vertices = this.cache_stmtVertices.get(a);
        if (stmt_vertices == null) {
            this.buildCache();
            stmt_vertices = this.cache_stmtVertices.get(a);
        }
        for (Vertex v : stmt_vertices) {
            if (v.isEqual(a, partitions, past_partitions, queryInstanceIndex)) {
                return v;
            }
        }
        return null;
    }
    
    /**
     * Return an immutable list of all the partition ids in our catalog
     * @return
     */
    protected List<Integer> getAllPartitions() {
        return (CatalogUtil.getAllPartitionIds(this.getDatabase()));
    }
    
    /**
     * Gives all edges to the set of vertices vs in the MarkovGraph g
     * @param vs
     * @param g
     * @return
     */
    public Set<Edge> getEdgesTo(Set<Vertex> vs) {
        Set<Edge> edges = new HashSet<Edge>();
        for (Vertex v : vs) {
            if (v != null) edges.addAll(this.getInEdges(v));
        }
        return edges;
    }
    
    // ----------------------------------------------------------------------------
    // STATISTICAL MODEL METHODS
    // ----------------------------------------------------------------------------

    /**
     * Calculate the probabilities for this graph This invokes the static
     * methods in Vertex to calculate each probability
     */
    public synchronized void calculateProbabilities() {
        // Reset all probabilities
//        for (Vertex v : this.getVertices()) {
//            v.resetAllProbabilities();
//        } // FOR
        
        // We first need to calculate the edge probabilities because the probabilities
        // at each vertex are going to be derived from these
        this.calculateEdgeProbabilities();
        
        // Then traverse the graph and calculate the vertex probability tables
        this.calculateVertexProbabilities();
    }
    
    /**
     * Update the instance hits for the graph's elements and recalculate probabilities
     */
    public synchronized void recalculateProbabilities() {
        this.normalizeTimes();
        for (Vertex v : this.getVertices()) {
            v.incrementTotalhits(v.getInstancehits());
        }
        for (Edge e : this.getEdges()) {
            e.incrementHits(e.getInstancehits());
        }
        this.calculateProbabilities();
    }

    /**
     * Calculate vertex probabilities
     */
    private void calculateVertexProbabilities() {
        if (trace.get()) LOG.trace("Calculating Vertex probabilities for " + this);
        
        final Set<Edge> visited_edges = new HashSet<Edge>();
        final List<Integer> all_partitions = this.getAllPartitions();
        
        // This is tricky. We need to sort of multiplex the traversal from either the commit
        // or abort vertices. We'll always start from the commit but then force the abort 
        // vertex to be the first node visited after it
        final Vertex commit = this.getCommitVertex();
        
        new VertexTreeWalker<Vertex>(this, TraverseOrder.LONGEST_PATH, Direction.REVERSE) {
            {
                this.getChildren(commit).addAfter(MarkovGraph.this.getAbortVertex());
            }
            
            @Override
            protected void callback(Vertex element) {
                if (trace.get()) LOG.trace("BEFORE: " + element + " => " + element.getSingleSitedProbability());
//                    if (element.isSingleSitedProbablitySet() == false) element.setSingleSitedProbability(0.0);
                Type vtype = element.getType(); 
                
                // COMMIT/ABORT is always single-partitioned!
                if (vtype == Vertex.Type.COMMIT || vtype == Vertex.Type.ABORT) {
                    if (trace.get()) LOG.trace(element + " is single-partitioned!");
                    element.setSingleSitedProbability(1.0f);
                    
                    // And DONE at all partitions!
                    // And will not Read/Write Probability
                    for (Integer partition : all_partitions) {
                        element.setDoneProbability(partition, 1.0f);
                        element.setReadOnlyProbability(partition, 1.0f);
                        element.setWriteProbability(partition, 0.0f);
                    } // FOR
                    
                    // Abort Probability
                    if (vtype == Vertex.Type.ABORT) {
                        element.setAbortProbability(1.0f);
                    } else {
                        element.setAbortProbability(0.0f);
                    }

                } else {
                    
                    // If the current vertex is not single-partitioned, then we know right away
                    // that the probability should be zero and we don't need to check our successors
                    // We define a single-partition vertex to be a query that accesses only one partition
                    // that is the same partition as the base/local partition. So even if the query accesses
                    // only one partition, if that partition is not the same as where the java is executing,
                    // then we're going to say that it is multi-partitioned
                    boolean element_islocalonly = element.isLocalPartitionOnly(); 
                    if (element_islocalonly == false) {
                        if (trace.get()) LOG.trace(element + " NOT is single-partitioned!");
                        element.setSingleSitedProbability(0.0f);
                    }

                    Statement catalog_stmt = element.getCatalogItem();
                    QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
                    
                    Collection<Edge> edges = MarkovGraph.this.getOutEdges(element);
                    for (Edge e : edges) {
                        if (visited_edges.contains(e)) continue;
                        Vertex successor = MarkovGraph.this.getDest(e);
                        assert(successor != null);
                        assert(successor.isSingleSitedProbabilitySet()) : "Setting " + element + " BEFORE " + successor;

                        // Single-Partition Probability
                        // If our vertex only touches the base partition, then we need to calculate the 
                        // single-partition probability as the sum of the the edge weights times our
                        // successors' single-partition probability
                        if (element_islocalonly) {
                            float prob = (float)(e.getProbability() * successor.getSingleSitedProbability());
                            element.addSingleSitedProbability(prob);
                            if (trace.get()) LOG.trace(element + " --" + e + "--> " + successor + String.format(" [%f * %f = %f]", e.getProbability(), successor.getSingleSitedProbability(), prob) + "\nprob = " + prob);
                        }
                        
                        // Abort Probability
                        element.addAbortProbability((float)(e.getProbability() * successor.getAbortProbability()));
                        
                        // Done/Read/Write At Partition Probability
                        for (Integer partition : all_partitions) {
                            assert(successor.isDoneProbabilitySet(partition)) : "Setting " + element + " BEFORE " + successor;
                            assert(successor.isReadOnlyProbabilitySet(partition)) : "Setting " + element + " BEFORE " + successor;
                            assert(successor.isWriteProbabilitySet(partition)) : "Setting " + element + " BEFORE " + successor;
                            
                            // This vertex accesses this partition
                            if (element.getPartitions().contains(partition)) {
                                element.setDoneProbability(partition, 0.0f);
                                
                                // Figure out whether it is a read or a write
                                if (catalog_stmt.getReadonly()) {
                                    if (trace.get()) LOG.trace(String.format("%s does not modify partition %d. Setting writing probability based on children [%s]", element, partition, qtype));
                                    element.addWriteProbability(partition, (float)(e.getProbability() * successor.getWriteProbability(partition)));
                                    element.addReadOnlyProbability(partition, (float)(e.getProbability() * successor.getReadOnlyProbability(partition)));
                                } else {
                                    if (trace.get()) LOG.trace(String.format("%s modifies partition %d. Setting writing probability to 1.0 [%s]", element, partition, qtype));
                                    element.setWriteProbability(partition, 1.0f);
                                    element.setReadOnlyProbability(partition, 0.0f);
                                }
                                
                            // This vertex doesn't access the partition, but successor vertices might so
                            // the probability is based on the edge probabilities 
                            } else {
                                element.addDoneProbability(partition, (float)(e.getProbability() * successor.getDoneProbability(partition)));
                                element.addWriteProbability(partition, (float)(e.getProbability() * successor.getWriteProbability(partition)));
                                element.addReadOnlyProbability(partition, (float)(e.getProbability() * successor.getReadOnlyProbability(partition)));
                            }
                        } // FOR (PartitionId)
                    } // FOR (Edge)
                }
                if (trace.get()) LOG.trace("AFTER: " + element + " => " + element.getSingleSitedProbability());
                if (trace.get()) LOG.trace(StringUtil.repeat("-", 40));
            }
        }.traverse(commit);
    }

    /**
     * Calculates the probabilities for each edge to be traversed
     */
    private void calculateEdgeProbabilities() {
        Collection<Vertex> vertices = this.getVertices();
        for (Vertex v : vertices) {
            for (Edge e : getOutEdges(v)) {
                e.setProbability(v.getTotalHits());
            }
        }
    }

    /**
     * Normalizes the times kept during online tallying of execution times.
     * TODO (svelagap): What about aborted transactions? Should they be counted in the normalization?
     */
    protected void normalizeTimes() {
        Map<Long, Long> stoptimes = this.getCommitVertex().getInstanceTimes();
        List<Long> to_remove = new ArrayList<Long>();
        for (Vertex v : this.getVertices()) {
            v.normalizeInstanceTimes(stoptimes, to_remove);
            to_remove.clear();
        } // FOR
    }
    
    /**
     * Checks to make sure the graph doesn't contain nonsense. We make sure
     * execution times and probabilities all add up correctly.
     * 
     * @return whether graph contains sane data
     */
    public boolean isValid() {
        double EPSILON = 0.00001;
        for (Vertex v : this.getVertices()) {
            double sum = 0.0;
            
            if (v.isValid() == false) {
                LOG.warn("The vertex " + v + " is not valid!");
                return (false);
            }
            
            Set<Vertex> seen_vertices = new HashSet<Vertex>(); 
            for (Edge e : this.getOutEdges(v)) {
                Vertex v1 = this.getOpposite(v, e);
                
                // Make sure that each vertex only has one edge to another vertex
                assert(!seen_vertices.contains(v1)) : "Vertex " + v + " has more than one edge to vertex " + v1;
                seen_vertices.add(v1);
                
                // Calculate total edge probabilities
                double edge_probability = e.getProbability(); 
                assert(edge_probability >= 0.0 && edge_probability <= 1.0) :
                    String.format("Edge %s->%s probability is %.4f", v.getCatalogItemName(), v1.getCatalogItemName(), edge_probability);
                sum += e.getProbability();
            } // FOR
            
            if (sum - 1.0 > EPSILON && getInEdges(v).size() != 0) {
                return false;
            }
            sum = 0.0;
            // Andy asks: Should this be getInEdges()?
            // Saurya replies: No, the probability of leaving this query should be 1.0, coming
            //                 in could be more. There could be two vertices each with .75 probability
            //                 of getting into this vertex, 1.5 altogether
            for (Edge e : getOutEdges(v)) { 
                sum += e.getProbability();
            }
            if (sum - 1.0 > EPSILON && getOutEdges(v).size() != 0) {
                return false;
            }
        }
        return true;
    }

    public boolean shouldRecompute(int instance_count, double recomputeTolerance){
        double VERTEX_PROPORTION = 0.5f; //If VERTEX_PROPORTION of 
        int count = 0;
        for(Vertex v: this.getVertices()){
            if(v.shouldRecompute(instance_count, recomputeTolerance, xact_count)){
                count++;
            }
        }
        return (count*1.0/getVertices().size()) >= VERTEX_PROPORTION;
    }
    // ----------------------------------------------------------------------------
    // XACT PROCESSING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * For a given TransactionTrace object, process its contents and update our
     * graph
     * 
     * @param txn_trace - The TransactionTrace to process and update the graph with
     * @param pest - The PartitionEstimator to use for estimating where things go
     */
    public List<Vertex> processTransaction(TransactionTrace txn_trace, PartitionEstimator pest) throws Exception {
        Procedure catalog_proc = txn_trace.getCatalogItem(this.getDatabase());
        Vertex previous = this.getStartVertex();
        previous.addExecutionTime(txn_trace.getStopTimestamp() - txn_trace.getStartTimestamp());

        final List<Vertex> path = new ArrayList<Vertex>();
        path.add(previous);
        
        Map<Statement, AtomicInteger> query_instance_counters = new HashMap<Statement, AtomicInteger>();
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            query_instance_counters.put(catalog_stmt, new AtomicInteger(0));
        } // FOR
        
        final int base_partition = pest.getBasePartition(txn_trace);
        
        // The past partitions is all of the partitions that this txn has touched,
        // included the base partition where the java executes
        Set<Integer> past_partitions = new HashSet<Integer>();
        // XXX past_partitions.add(this.getBasePartition());
        
        // -----------QUERY TRACE-VERTEX CREATION--------------
        for (QueryTrace query_trace : txn_trace.getQueries()) {
            Set<Integer> partitions = pest.getAllPartitions(query_trace, base_partition);
            assert(partitions != null);
            assert(!partitions.isEmpty());
            Statement catalog_stmnt = query_trace.getCatalogItem(this.getDatabase());

            int queryInstanceIndex = query_instance_counters.get(catalog_stmnt).getAndIncrement(); 
            Vertex v = null;
            synchronized (previous) {
                v = this.getVertex(catalog_stmnt, partitions, past_partitions, queryInstanceIndex);
                if (v == null) {
                    // If no such vertex exists we simply create one
                    v = new Vertex(catalog_stmnt, Vertex.Type.QUERY, queryInstanceIndex, partitions, new HashSet<Integer>(past_partitions));
                    this.addVertex(v);
                }
                assert(v.isQueryVertex());
                // Add to the edge between the previous vertex and the current one
                this.addToEdge(previous, v);
    
                // Annotate the vertex with remaining execution time
                v.addExecutionTime(txn_trace.getStopTimestamp() - query_trace.getStartTimestamp());
            } // SYNCH
            previous = v;
            path.add(v);
            past_partitions.addAll(partitions);
        } // FOR
        
        synchronized (previous) {
            if (txn_trace.isAborted()) {
                this.addToEdge(previous, this.getAbortVertex());
                path.add(this.getAbortVertex());
            } else {
                this.addToEdge(previous, this.getCommitVertex());
                path.add(this.getCommitVertex());
            }
        } // SYNCH
        // -----------END QUERY TRACE-VERTEX CREATION--------------
        this.xact_count++;
        return (path);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Reset the instance hit counters
     * XXX: This assumes that this will not be manipulated concurrently, no
     * other transaction running at the same time
     */
    public synchronized void resetCounters() {
        for (Vertex v : this.getVertices()) {
            v.setInstancehits(0);
        }
        for (Edge e : this.getEdges()) {
            e.setInstancehits(0);
        }
    }
    
    /**
     * 
     * @return The number of xacts used to make this MarkovGraph
     */
    public int getTransactionCount() {
        return xact_count;
    }    
    public void setTransactionCount(int xact_count){
        this.xact_count = xact_count;
    }
    /**
     * Increase the transaction count for this MarkovGraph by one
     */
    public void incrementTransasctionCount() {
       this.xact_count++; 
    }
    
    public void incrementMispredictionCount() {
        this.xact_mispredict++;
        if (this.xact_count > 0) this.xact_accuracy = (this.xact_count - this.xact_mispredict) / (double)this.xact_count;
    }
    
    public double getAccuracyRatio() {
        return (this.xact_accuracy);
    }
    
    // ----------------------------------------------------------------------------
    // YE OLDE MAIN METHOD
    // ----------------------------------------------------------------------------

    /**
     * To load in the workloads and see their properties, we use this method.
     * There are still many workloads that have problems running.
     * 
     * @param args
     * @throws InterruptedException
     * @throws InvocationTargetException
     */
    public static void main(String vargs[]) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD);
        final PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        
        MarkovGraphsContainer markovs = null;
        
        // Check whether we are generating the global graphs or the clustered versions
        Boolean global = args.getBooleanParam(ArgumentsParser.PARAM_MARKOV_GLOBAL); 
        if (global != null && global == true) {
            markovs = MarkovUtil.createProcedureGraphs(args.catalog_db, args.workload, p_estimator);

        // HACK
        } else if (args.catalog_type == ProjectType.TPCC) {
            TPCCMarkovGraphsContainer.create(args);
            return;
        // Or whether we should divide the transactions by their base partition 
        } else {
            markovs = MarkovUtil.createBasePartitionGraphs(args.catalog_db, args.workload, p_estimator);
        }
        
        // Save the graphs
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV_OUTPUT)) {
            LOG.info("Writing graphs out to " + args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
            
            // HACK: Split the graphs into separate MarkovGraphsContainers based on partition ids
            Map<Integer, MarkovGraphsContainer> inner = new HashMap<Integer, MarkovGraphsContainer>();
            for (Entry<Integer, Map<Procedure, MarkovGraph>> e : markovs.entrySet()) {
                MarkovGraphsContainer m = new MarkovGraphsContainer(markovs.isGlobal());
                Integer p = e.getKey();
                for (MarkovGraph markov : e.getValue().values()) {
                    m.put(p, markov);
                } // FOR
                inner.put(p, m);
            } // FOR
            MarkovUtil.save(inner, args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
        }

        
//
//        Map<Procedure, Pair<Integer, Integer>> counts = new HashMap<Procedure, Pair<Integer, Integer>>();
//        for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
//            int vertexcount = 0;
//            int edgecount = 0;
//            
//            for (int i : partitions) {
//                Pair<Procedure, Integer> current = new Pair<Procedure, Integer>(catalog_proc, i);
//                vertexcount += partitionGraphs.get(current).getVertexCount();
//                edgecount += partitionGraphs.get(current).getEdgeCount();
//            } // FOR
//            counts.put(catalog_proc, new Pair<Integer, Integer>(vertexcount, edgecount));
//        } // FOR
//        for (Procedure pr : counts.keySet()) {
//            System.out.println(pr + "," + args.workload + "," + args.workload_xact_limit + ","
//                    + counts.get(pr).getFirst() + ","
//                    + counts.get(pr).getSecond());
//        } // FOR
        
            
            
//            for (Integer partition : graphs_per_partition.keySet()) {
//                for (MarkovGraph g : graphs_per_partition.get(partition)) {
//                    String name = g.getProcedure() + "_" + partition;
//                    String contents = GraphvizExport.export(g, name);
//                    FileUtil.writeStringToFile("./graphs/" + name + ".dot", contents);
//                }
//            }
    }
}