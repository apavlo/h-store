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
import org.json.JSONException;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.AbstractGraphElement;
import edu.brown.graphs.GraphUtil;
import edu.brown.graphs.exceptions.InvalidGraphElementException;
import edu.brown.markov.benchmarks.TPCCMarkovGraphsContainer;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
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
    
    /**
     * The precision we care about for vertex probabilities
     */
    public static final float PROBABILITY_EPSILON = 0.00001f;
    
    /**
     * TODO
     */
    public static final int MIN_HITS_FOR_NO_ABORT = 20;
    
    // ----------------------------------------------------------------------------
    // INSTANCE DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    protected final Procedure catalog_proc;

    /** Keep track of how many times we have used this graph for transactions */
    private transient int xact_count = 0;
    /** Keep track of how many times we have used this graph but the transaction mispredicted */
    private transient int xact_mispredict = 0;
    /** Percentage of how accurate this graph has been */
    private transient double xact_accuracy = 1.0;
    /** How many times have we recomputed the probabilities for this graph */
    private transient int recompute_count = 0;

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
     * Calculate the probabilities for this graph.
     * First we will reset all of the existing probabilities and then apply the instancehits to 
     * the totalhits for each graph element
     */
    public synchronized void calculateProbabilities() {
        // Reset all probabilities
        for (Vertex v : this.getVertices()) {
            v.resetAllProbabilities();
        } // FOR
        
        this.normalizeTimes();
        for (Vertex v : this.getVertices()) {
            v.applyInstanceHitsToTotalHits();
        }
        for (Edge e : this.getEdges()) {
            e.applyInstanceHitsToTotalHits();
        }
        
        // We first need to calculate the edge probabilities because the probabilities
        // at each vertex are going to be derived from these
        this.calculateEdgeProbabilities();
        
        // Then traverse the graph and calculate the vertex probability tables
        this.calculateVertexProbabilities();
        
        this.recompute_count++;
    }

    /**
     * Calculate vertex probabilities
     */
    private void calculateVertexProbabilities() {
        if (trace.get()) LOG.trace("Calculating Vertex probabilities for " + this);
        new MarkovProbabilityCalculator(this).calculate();
    }

    /**
     * Calculates the probabilities for each edge to be traversed
     */
    private void calculateEdgeProbabilities() {
        Collection<Vertex> vertices = this.getVertices();
        for (Vertex v : vertices) {
            if (v.isQueryVertex() && v.getTotalHits() == 0) continue;
            for (Edge e : this.getOutEdges(v)) {
                try {
                    e.calculateProbability(v.getTotalHits());
                } catch (Throwable ex) {
                    throw new RuntimeException(String.format("Failed to calculate probabilities for edge %s -> %s", v, this.getDest(e)), ex);  
                }
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
    
    public Set<Vertex> getInvalidVertices() {
        Set<Vertex> ret = new HashSet<Vertex>();
        for (Vertex v : this.getVertices()) {
            if (v.isValid(this) == false) ret.add(v);
        } // FOR
        return (ret);
    }
    
    /**
     * Checks to make sure the graph doesn't contain nonsense. We make sure
     * execution times and probabilities all add up correctly.
     * 
     * @return whether graph contains sane data
     */
    public boolean isValid() {
        try {
            this.validate();
        } catch (InvalidGraphElementException ex) {
            return (false);
        }
        return (true);
    }
    
    public void validate() throws InvalidGraphElementException {
        if (debug.get()) LOG.warn("Calling MarkovGraph.isValid(). This should never be done at runtime!");
        Map<Long, AbstractGraphElement> seen_ids = new HashMap<Long, AbstractGraphElement>();
        
        // Validate Edges
        for (Edge e : this.getEdges()) {
            
            // Make sure the edge thinks it's valid
            e.validate(this);
            
            // Make sure that nobody else has the same element id
            if (seen_ids.containsKey(e.getElementId())) {
                throw new InvalidGraphElementException(this, e, String.format("Duplicate element id #%d: Edge[%s] <-> %s",
                                                                               e.getElementId(), e.toString(true), seen_ids.get(e.getElementId())));
            }
            seen_ids.put(e.getElementId(), e);
        } // FOR
        
        // Validate Vertices
        Set<Vertex> seen_vertices = new HashSet<Vertex>();
        Set<Integer> all_partitions = new HashSet<Integer>();
        for (Vertex v0 : this.getVertices()) {
            float total_prob = 0.0f;
            long total_edgehits = 0;
            
            // Make sure the vertex thinks it's valid
            v0.validate(this);
            
            Collection<Edge> outbound = this.getOutEdges(v0);
            Collection<Edge> inbound = this.getInEdges(v0);
            
            // Make sure that nobody else has the same element id
            if (seen_ids.containsKey(v0.getElementId())) {
                throw new InvalidGraphElementException(this, v0, String.format("Duplicate element id #%d: Vertex[%s] <-> %s",
                                                                 v0.getElementId(), v0, seen_ids.get(v0.getElementId())));
            }
            seen_ids.put(v0.getElementId(), v0);
            
            all_partitions.addAll(v0.partitions);
            all_partitions.addAll(v0.past_partitions);
            
            for (Edge e : outbound) {
                Vertex v1 = this.getOpposite(v0, e);
                
                // Make sure that each vertex only has one edge to another vertex
                if (seen_vertices.contains(v1)) {
                    throw new InvalidGraphElementException(this, v0, "Vertex has more than one edge to vertex " + v1);
                }
                seen_vertices.add(v1);
                
                // The totalhits for this vertex should always be greater than or equal to the total hints for 
                // all of its edges and equal to the sum
                if (v0.getTotalHits() < e.getTotalHits()) {
                    throw new InvalidGraphElementException(this, v0, String.format("Vertex has %d totalHits but edge from %s to %s has %d",
                                                                                   v0.getTotalHits(), v0, v1, e.getTotalHits()));
                }
                
                // Make sure that this the destination vertex has the same partitions as the past+current partitions
                // for the destination vertex
                if (v0.isQueryVertex() && v1.isQueryVertex() && v1.past_partitions.equals(all_partitions) == false) {
                    throw new InvalidGraphElementException(this, e, "Destination vertex in edge does not have the correct past partitions");
                }
                
                // Make sure that if the two vertices are for the same Statement, that
                // the source vertex's instance counter is one less than the destination
                if (v0.getCatalogKey().equals(v1.getCatalogKey())) {
                    if (v0.counter+1 != v1.counter) {
                        throw new InvalidGraphElementException(this, e, String.format("Invalid edge in from multiple invocations of %s: %d+1 != %d",
                                                                                       this.catalog_proc.getName(), v0.counter, v1.counter));
                    }
                }
                
                // Calculate total edge probabilities and hits
                total_prob += e.getProbability();
                total_edgehits += e.getTotalHits();
            } // FOR
            if (v0.isQueryVertex()) {
                if (MathUtil.equals(total_prob, 1.0f, MarkovGraph.PROBABILITY_EPSILON) == false && outbound.size() > 0) {
                    throw new InvalidGraphElementException(this, v0, "Total outbound edge probability is not equal to one: " + total_prob);
                }
                if (total_edgehits != v0.getTotalHits()) {
                    throw new InvalidGraphElementException(this, v0, String.format("Vertex has %d totalHits but the sum of all its edges has %d",
                                                                                   v0.getTotalHits(), total_edgehits));
                }
            }
            
            total_prob = 0.0f;
            total_edgehits = 0;
            for (Edge e : inbound) { 
                total_prob += e.getProbability();
                total_edgehits += e.getTotalHits();
            } // FOR
            if (MathUtil.greaterThan(total_prob, 0.0f, MarkovGraph.PROBABILITY_EPSILON) == false && inbound.size() > 0 && total_edgehits > 0) {
                throw new InvalidGraphElementException(this, v0, "Total inbound edge probability is not greater than zero: " + total_prob);
            }
            
            seen_vertices.clear();
            all_partitions.clear();
        } // FOR
    }

    public boolean shouldRecompute(int instance_count, double recomputeTolerance) {
        double VERTEX_PROPORTION = 0.5f; // If VERTEX_PROPORTION of
        int count = 0;
        for (Vertex v : this.getVertices()) {
            if (v.shouldRecompute(instance_count, recomputeTolerance, xact_count)) {
                count++;
            }
        }
        return (count * 1.0 / getVertices().size()) >= VERTEX_PROPORTION;
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
        previous.incrementTotalHits();

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
                Edge e = this.addToEdge(previous, v);
                assert(e != null);
                v.incrementTotalHits();
                e.incrementTotalHits();
    
                // Annotate the vertex with remaining execution time
                v.addExecutionTime(txn_trace.getStopTimestamp() - query_trace.getStartTimestamp());
            } // SYNCH
            previous = v;
            path.add(v);
            past_partitions.addAll(partitions);
        } // FOR
        
        synchronized (previous) {
            Vertex v = (txn_trace.isAborted() ? this.getAbortVertex() : this.getCommitVertex());
            assert(v != null);
            Edge e = this.addToEdge(previous, v);
            assert(e != null);
            path.add(v);
            v.incrementTotalHits();
            e.incrementTotalHits();
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
            v.setInstanceHits(0);
        }
        for (Edge e : this.getEdges()) {
            e.setInstanceHits(0);
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
    
    public int getRecomputeCount() {
        return (this.recompute_count);
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        // Ignore any vertices with no totalhits
        Set<Vertex> ignore = new HashSet<Vertex>();
        for (Vertex v : this.getVertices()) {
            if (v.isQueryVertex() && (v.instancehits == 0 && v.totalhits == 0)) ignore.add(v);
        }
        GraphUtil.serialize(this, ignore, null, stringer);
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
                    try {
                        markov.validate();
                    } catch (InvalidGraphElementException ex) {
                        throw new Exception(String.format("Invalid %s MarkovGraph at partition %d", markov.getProcedure().getName(), p), ex);
                    }
                    m.put(p, markov);
                } // FOR
                inner.put(p, m);
            } // FOR
            MarkovUtil.save(inner, args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
        }

    }
}