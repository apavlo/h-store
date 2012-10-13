package edu.brown.hstore.estimators;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;

import edu.brown.graphs.GraphvizExport;
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
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * The current state of a transaction
 */
public final class MarkovEstimatorState extends EstimatorState {
    private static final Logger LOG = Logger.getLogger(MarkovEstimatorState.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean d = debug.get();
    
    protected final List<MarkovVertex> actual_path = new ArrayList<MarkovVertex>();
    protected final List<MarkovEdge> actual_path_edges = new ArrayList<MarkovEdge>();
    private final List<MarkovEstimate> estimates = new ArrayList<MarkovEstimate>();

    protected MarkovGraph markov;
    protected MarkovPathEstimator initial_estimator;
    protected MarkovEstimate initial_estimate;
    private int num_estimates;
    
    protected transient MarkovVertex current;
    protected transient final PartitionSet cache_past_partitions = new PartitionSet();
    protected transient final PartitionSet cache_last_partitions = new PartitionSet();
    
    /**
     * State Factory
     */
    public static class Factory extends TypedPoolableObjectFactory<MarkovEstimatorState> {
        private int num_partitions;
        
        public Factory(int num_partitions) {
            super(HStoreConf.singleton().site.pool_profiling);
            this.num_partitions = num_partitions;
        }
        
        @Override
        public MarkovEstimatorState makeObjectImpl() throws Exception {
            return (new MarkovEstimatorState(this.num_partitions));
        }
    };
    
    /**
     * Constructor
     * @param markov - the graph that this txn is using
     * @param estimated_path - the initial path estimation from MarkovPathEstimator
     */
    private MarkovEstimatorState(int num_partitions) {
        super(num_partitions);
    }
    
    public void init(Long txn_id, int base_partition, MarkovGraph markov, MarkovPathEstimator initial_estimator, long start_time) {
        this.markov = markov;
        this.initial_estimator = initial_estimator;
        this.initial_estimate = initial_estimator.getEstimate();
        this.setCurrent(markov.getStartVertex(), null);
        super.init(txn_id, base_partition, start_time);
    }
    
    @Override
    public void finish() {
        // Only return the MarkovPathEstimator to it's object pool if it hasn't been cached
        if (this.initial_estimator.isCached() == false) {
            if (d) LOG.debug(String.format("Initial MarkovPathEstimator is not marked as cached for txn #%d. Returning to pool... [hashCode=%d]",
                             this.txn_id, this.initial_estimator.hashCode()));
            try {
                MarkovEstimator.POOL_ESTIMATORS.returnObject(this.initial_estimator);
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
                String.format("MarkovEstimate #%d == Initial MarkovEstimate for txn #%d [hashCode=%d]",
                              i, this.txn_id, this.initial_estimate.hashCode());
            this.estimates.get(i).finish();
        } // FOR
        this.num_estimates = 0;
        
        this.markov.incrementTransasctionCount();
        this.actual_path.clear();
        this.actual_path_edges.clear();
        this.current = null;
        this.initial_estimator = null;
        this.initial_estimate = null;
        super.finish();
        MarkovEstimator.POOL_STATES.returnObject(this);
    }
    
    /**
     * Get the next Estimate object for this State
     * @return
     */
    protected MarkovEstimate createNextEstimate(MarkovVertex v) {
        MarkovEstimate next = null;
        if (this.num_estimates < this.estimates.size()) {
            next = this.estimates.get(this.num_estimates);
        } else {
            next = new MarkovEstimate(this.num_partitions);
            this.estimates.add(next);
        }
        next.init(this, v, this.num_estimates++);
        return (next);
    }

    public MarkovGraph getMarkovGraph() {
        return (this.markov);
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
    public MarkovVertex getCurrent() {
        return (this.current);
    }
    /**
     * Set the current vertex for this transaction and update the actual path
     * @param current
     */
    public void setCurrent(MarkovVertex current, MarkovEdge e) {
        if (this.current != null) assert(this.current.equals(current) == false);
        this.actual_path.add(current);
        if (e != null) this.actual_path_edges.add(e);
        this.current = current;
    }
    
    public List<MarkovVertex> getInitialPath() {
        return (this.initial_estimator.getVisitPath());
    }
    public float getInitialPathConfidence() {
        return (this.initial_estimator.getConfidence());
    }
    public List<MarkovVertex> getActualPath() {
        return (this.actual_path);
    }

    @Override
    public MarkovEstimate getInitialEstimate() {
        return (this.initial_estimate);
    }

    public List<MarkovVertex> getLastPath() {
        // HACK: Always return the initial path for now
        return (this.initial_estimator.getVisitPath());
    }
    
    @Override
    public MarkovEstimate getLastEstimate() {
        return (this.num_estimates > 0 ? this.estimates.get(this.num_estimates-1) : this.initial_estimate);
    }
    
    /**
     * Debug method to dump out the Markov graph to a Graphviz file
     * Returns the path to the file
     */
    public File dumpMarkovGraph() {
        MarkovGraph markov = this.getMarkovGraph();
        GraphvizExport<MarkovVertex, MarkovEdge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(this.getInitialPath()));
        gv.highlightPath(markov.getPath(this.getActualPath()), "blue");
        return gv.writeToTempFile(this.markov.getProcedure());
    }
    
    @Override
    public String toString() {
        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
        m0.put("TransactionId", this.txn_id);
        m0.put("Procedure", this.markov.getProcedure().getName());
        m0.put("MarkovGraph Id", this.markov.getGraphId());
        
        Map<String, Object> m1 = new LinkedHashMap<String, Object>();
        m1.put("Initial Partitions", this.initial_estimator.getTouchedPartitions());
        m1.put("Initial Confidence", this.getInitialPathConfidence());
        m1.put("Initial Estimate", this.getInitialEstimate().toString());
        
        Map<String, Object> m2 = new LinkedHashMap<String, Object>();
        m2.put("Actual Partitions", this.getTouchedPartitions());
        m2.put("Current Estimate", this.current.debug());
        
        return StringUtil.formatMaps(m0, m1, m2);
    }
} // END CLASS