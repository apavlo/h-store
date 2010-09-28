package edu.brown.markov;

import java.util.*;

import org.apache.log4j.Logger;

import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.*;
import edu.brown.utils.*;

/**
 * 
 * @author pavlo
 * 
 */
public class TransactionEstimator {
    private static final Logger LOG = Logger.getLogger(TransactionEstimator.class.getName());

    /**
     * The amount of change in visitation of vertices we would tolerate before we need to recompute the graph.
     * TODO (pavlo): Saurya says: Should this be in MarkovGraph?
     */
    private static final double RECOMPUTE_TOLERANCE = (double) 0.5;

    private transient Database catalog_db;
    private transient int num_partitions;
    private transient PartitionEstimator p_estimator;
    private transient ParameterCorrelations correlations;
    
    private final int base_partition;
    private final HashMap<Long, MarkovGraph> xact_graphs = new HashMap<Long, MarkovGraph>();
    private final HashMap<Procedure, MarkovGraph> procedure_graphs = new HashMap<Procedure, MarkovGraph>();
    private final transient Map<Long, State> xact_states = new HashMap<Long, State>();
    private int xact_count;

    /**
     * The current state of a transaction
     */
    private final class State {
        private Vertex current;
        private Estimate last_estimate;
        private final long start_time;
        private long stop_time;
        private final List<Vertex> initial_path = new ArrayList<Vertex>();
        private final List<Vertex> taken_path = new ArrayList<Vertex>();

        public State(MarkovGraph markov) {
            this.start_time = System.currentTimeMillis();
            this.current = markov.getStartVertex();
        }

        public List<Vertex> getInitialPath() {
            return (this.initial_path);
        }

        public List<Vertex> getTakenPath() {
            return (this.taken_path);
        }

        public Vertex getCurrent() {
            return (this.current);
        }

        public void setCurrent(Vertex current) {
            this.current = current;
        }

        public long getStartTime() {
            return this.start_time;
        }

        public long getStopTime() {
            return this.stop_time;
        }

        public Estimate getLastEstimate() {
            return this.last_estimate;
        }

        public void setLastEstimate(Estimate last_estimate) {
            this.last_estimate = last_estimate;
        }
    } // END CLASS

    /**
     * An estimation for a transaction given its current state
     */
    public final class Estimate {
        // Global
        public double singlepartition;
        public double userabort;

        // Partition-specific
        public final double finished[];
        public final double read[];
        public final double write[];
        public Long time;

        public Estimate() {
            this.finished = new double[TransactionEstimator.this.num_partitions];
            this.read = new double[TransactionEstimator.this.num_partitions];
            this.write = new double[TransactionEstimator.this.num_partitions];
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

        private Set<Integer> getPartitions(double values[], double limit, boolean inverse) {
            Set<Integer> ret = new HashSet<Integer>();
            for (int i = 0; i < values.length; i++) {
                if (inverse) {
                    if ((1 - values[i]) >= limit) ret.add(i);
                } else {
                    if (values[i] >= limit) ret.add(i);
                }
            } // FOR
            return (ret);
        }

        public Set<Integer> getReadOnlyPartitions(EstimationThresholds t) {
            return (this.getPartitions(this.read, t.getReadThreshold(), false));
        }

        public Set<Integer> getWritePartitions(EstimationThresholds t) {
            return (this.getPartitions(this.write, t.getWriteThreshold(), false));
        }

        public Set<Integer> getFinishedPartitions(EstimationThresholds t) {
            return (this.getPartitions(this.finished, t.getDoneThreshold(), false));
        }
        
        public Set<Integer> getTargetPartitions(EstimationThresholds t) {
            return (this.getPartitions(this.finished, t.getDoneThreshold(), true));
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            final String f = "%.02f"; 
            
            sb.append("Single-Partition: ").append(String.format(f, this.singlepartition)).append("\n");
            sb.append("User Abort:       ").append(String.format(f, this.userabort)).append("\n");
            sb.append("\n");
            
            sb.append("              \tRead\tWrite\tFinish\n");
            for (int i = 0; i < TransactionEstimator.this.num_partitions; i++) {
                sb.append(String.format("Partition %02d:\t", i));
                sb.append(String.format(f, this.read[i])).append("\t");
                sb.append(String.format(f, this.write[i])).append("\t");
                sb.append(String.format(f, this.finished[i])).append("\n");
            } // FOR
            return (sb.toString());
        }
    } // END CLASS

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * 
     * @param base_partition
     * @param p_estimator
     * @param correlations
     */
    public TransactionEstimator(int base_partition, PartitionEstimator p_estimator, ParameterCorrelations correlations) {
        this.base_partition = base_partition;
        this.p_estimator = p_estimator;
        this.catalog_db = this.p_estimator.getDatabase();
        this.num_partitions = CatalogUtil.getNumberOfPartitions(this.catalog_db);
        this.correlations = (correlations == null ? new ParameterCorrelations() : correlations);
    }

    /**
     * Constructor
     * 
     * @param catalog_db
     */
    public TransactionEstimator(int base_partition, PartitionEstimator p_estimator) {
        this(base_partition, p_estimator, null);
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------

    public ParameterCorrelations getCorrelations() {
        return this.correlations;
    }

    public int getBasePartition() {
        return this.base_partition;
    }

    public PartitionEstimator getPartitionEstimator() {
        return this.p_estimator;
    }

    public void addMarkovGraphs(Set<MarkovGraph> markovs) {
        for (MarkovGraph m : markovs) {
            this.addMarkovGraph(m.getProcedure(), m);
        } // FOR
    }
    
    public void addMarkovGraphs(Map<Procedure, MarkovGraph> markovs) {
        this.procedure_graphs.putAll(markovs);
    }

    public void addMarkovGraph(Procedure catalog_proc, MarkovGraph graph) {
        this.procedure_graphs.put(catalog_proc, graph);
    }

    public MarkovGraph getMarkovGraph(Procedure catalog_proc) {
        return (this.procedure_graphs.get(catalog_proc));
    }

    public MarkovGraph getMarkovGraph(String catalog_key) {
        return (this.getMarkovGraph(CatalogKey.getFromKey(this.catalog_db, catalog_key, Procedure.class)));
    }

    // ----------------------------------------------------------------------------
    // RUNTIME METHODS
    // ----------------------------------------------------------------------------

    
    /**
     * Returns true if we will be able to calculate estimations for the given Procedure
     * 
     * @param catalog_proc
     * @return
     */
    public boolean canEstimate(Procedure catalog_proc) {
        return (catalog_proc.getSystemproc() == false && procedure_graphs.containsKey(catalog_proc));
    }
    
    /**
     * Sets up the beginning of a transaction. Returns an estimate of where this
     * transaction will go.
     * 
     * @param xact_id
     * @param catalog_proc
     * @param BASE_PARTITION
     * @return an estimate for the transaction's future
     */
    public Estimate startTransaction(long xact_id, Procedure catalog_proc, Object args[]) {
        assert (catalog_proc != null);
        Estimate estimate = new Estimate();

        // If we don't have a graph for this procedure, we should probably just return null
        // This will be the case for all sysprocs
        if (!procedure_graphs.containsKey(catalog_proc)) {
            LOG.debug("No MarkovGraph exists for '" + catalog_proc + "' on partition #" + this.base_partition);
            return (null);
            // fillIn(estimate,xact_states.get(xact_id));
            // return estimate;
        }

        MarkovGraph graph = this.procedure_graphs.get(catalog_proc);
        
        xact_graphs.put(xact_id, graph);
        assert (graph != null);
        graph.resetCounters();

        Vertex start = graph.getStartVertex();
        start.addInstanceTime(xact_id, System.currentTimeMillis());
        
        State state = new State(graph);
        this.xact_states.put(xact_id, state);
        
        // Calculate initial path estimate
        try {
            this.estimatePath(graph, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        this.populateEstimate(state, estimate);
        
        this.xact_count++;
        return (estimate);
    }

    /**
     * Takes a series of queries and executes them in order given the partition
     * information. Provides an estimate of where the transaction might go next.
     * 
     * @param xact_id
     * @param catalog_stmts
     * @param partitions
     * @return
     */
    public Estimate executeQueries(long xact_id, Statement catalog_stmts[], int partitions[][]) {
        assert (catalog_stmts.length == partitions.length);
        MarkovGraph g = procedure_graphs.get((catalog_stmts[0].getParent()));
        Estimate estimate = new Estimate();
        for (int i = 0; i < catalog_stmts.length; i++) {
            Statement catalog_stmt = catalog_stmts[i];
            int stmt_partitions[] = partitions[i];
            consume(catalog_stmt, stmt_partitions, g, xact_id);
        } // FOR
        populateEstimate(xact_states.get(xact_id), estimate);
        
        // Once the workload shifts we detect it and trigger this method. Recomputes
        // the graph with the data we collected with the current workload method.
        if (g.shouldRecompute(xact_count, RECOMPUTE_TOLERANCE)) {
            g.recomputeGraph();
        }
        return (estimate);
    }

    /**
     * The transaction with provided xact_id is finished
     * 
     * @param xact_id
     *            finished transaction
     */
    public void commit(long xact_id) {
        State s = this.xact_states.remove(xact_id);
        if (s == null) {
            String msg = "No state information exists for txn #" + xact_id;
            LOG.debug(msg);
            return;
            // throw new RuntimeException(msg);
        }
        MarkovGraph g = this.xact_graphs.remove(xact_id);
        g.getCommitVertex().addInstanceTime(xact_id, s.getStopTime());
    }

    /**
     * The transaction with provided xact_id has aborted
     * 
     * @param xact_id
     */
    public void abort(long xact_id) {
        State s = this.xact_states.remove(xact_id);
        if (s == null) {
            String msg = "No state information exists for txn #" + xact_id;
            LOG.debug(msg);
            return;
            // throw new RuntimeException(msg);
        }
        MarkovGraph g = this.xact_graphs.remove(xact_id);
        g.getAbortVertex().addInstanceTime(xact_id, s.getStopTime());
    }

    // ----------------------------------------------------------------------------
    // INTERNAL ESTIMATION METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Given an empty estimate object and the current Vertex, we fill in the
     * relevant information for the transaction coordinator to use.
     * @param estimate
     *            - the Estimate object which will be filled in
     * @param current
     *            - the Vertex we are currently at in the MarkovGraph
     */
    protected void populateEstimate(State state, Estimate estimate) {
        Vertex current = state.getCurrent();
        estimate.singlepartition = current.getSingleSitedProbability();
        estimate.userabort = current.getAbortProbability();
        estimate.time = current.getExecutiontime();
        for (int i : CatalogUtil.getAllPartitionIds(this.catalog_db)) {
            estimate.finished[i] = current.getDoneProbability(i);
            estimate.read[i] = current.getReadOnlyProbability(i);
            estimate.write[i] = current.getWriteProbability(i);
        } // FOR
    }

    /**
     * At the start of a transaction, estimate the execution path that the
     * transaction will likely take during its lifetime
     * 
     * @param markov
     * @param args
     * @throws Exception
     */
    protected List<Vertex> estimatePath(final MarkovGraph markov, final Object args[]) throws Exception {
        final List<Vertex> path = new Vector<Vertex>();
        
        // Walk through the graph as far as we can
        Vertex start = markov.getStartVertex();
        assert (start != null);
        
        //new PathEstimator(markov, this, args);

        return (path);
    }
    
    /**
     * TODO(svelagap)
     * 
     * @param catalog_stmt
     * @param partitions
     * @param g
     * @param xact_id
     */
    protected void consume(Statement catalog_stmt, int partitions[], MarkovGraph g, long xact_id) {
        State state = this.xact_states.get(xact_id);
        if (g == null || g.getEdgeCount() == 0) {
            // When would this happen?
            assert(false) : "Unexpected execution path in TransactionEstimator";
            // xact_states.put(xact_id, g.getStartVertex());
            return;
        }
        assert(state != null);
        
        Vertex current = state.getCurrent();
        for (Edge e : g.getOutEdges(current)) {
            Vertex v = g.getSource(e);
            //TODO (svelagap): Should use equals, not isMatch..
            if (v.isMatch(catalog_stmt, partitions)) {
                state.setCurrent(v);
                // TODO (svelagap) : We need to keep track of query_instance_index
                v.addInstanceTime(xact_id, System.currentTimeMillis());
                e.incrementInstancehits();
                v.incrementInstancehits();
                return;
            }
        } // FOR
        // What happens if we get down here??
        assert(false) : "Unexpected execution path in TransactionEstimator";
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
        MarkovGraphsContainer graphs_per_partition = MarkovUtil.createGraphs(args.catalog_db, args.workload, p_estimator);
        assert(graphs_per_partition != null);
        
        // Then construct a TransactionEstimator per partition/procedure
        // TODO(pavlo): Do we need to be able to serialize a TransactionEstimator?
        // System.err.println("Number of graphs: " + MarkovUtil.load(args.catalog_db, args.getParam(ArgumentsParser.PARAM_MARKOV), CatalogUtil.getAllPartitions(args.catalog_db)).get(0).size());
        
    }

}