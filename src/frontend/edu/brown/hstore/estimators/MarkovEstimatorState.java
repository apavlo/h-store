package edu.brown.hstore.estimators;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import edu.brown.graphs.GraphvizExport;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
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
    
    protected final List<MarkovVertex> actual_path = new ArrayList<MarkovVertex>();
    protected final List<MarkovEdge> actual_path_edges = new ArrayList<MarkovEdge>();
    
    protected MarkovGraph markov;
    protected transient MarkovVertex current;
    protected transient final PartitionSet cache_past_partitions = new PartitionSet();
    protected transient final PartitionSet cache_last_partitions = new PartitionSet();
    
    /**
     * State Factory
     */
    public static class Factory extends TypedPoolableObjectFactory<MarkovEstimatorState> {
        private final CatalogContext catalogContext;
        
        public Factory(CatalogContext catalogContext) {
            super(HStoreConf.singleton().site.pool_profiling);
            this.catalogContext = catalogContext;
        }
        @Override
        public MarkovEstimatorState makeObjectImpl() throws Exception {
            return (new MarkovEstimatorState(this.catalogContext));
        }
    };
    
    /**
     * Constructor
     * @param markov - the graph that this txn is using
     * @param estimated_path - the initial path estimation from MarkovPathEstimator
     */
    private MarkovEstimatorState(CatalogContext catalogContext) {
        super(catalogContext);
    }
    
    public void init(Long txn_id, int base_partition, MarkovGraph markov, long start_time) {
        this.markov = markov;
        this.setCurrent(markov.getStartVertex(), null);
        super.init(txn_id, base_partition, start_time);
    }
    
    @Override
    public void finish() {
        // Only return the MarkovPathEstimator to it's object pool if it hasn't been cached
//        if (this.initial_estimator.isCached() == false) {
//            if (d) LOG.debug(String.format("Initial MarkovPathEstimator is not marked as cached for txn #%d. Returning to pool... [hashCode=%d]",
//                             this.txn_id, this.initial_estimator.hashCode()));
//            try {
//                MarkovEstimator.POOL_ESTIMATORS.returnObject(this.initial_estimator);
//            } catch (Exception ex) {
//                throw new RuntimeException("Failed to return MarkovPathEstimator for txn" + this.txn_id, ex);
//            }
//        } else if (d) {
//            LOG.debug(String.format("Initial MarkovPathEstimator is marked as cached for txn #%d. Will not return to pool... [hashCode=%d]",
//                      this.txn_id, this.initial_estimator.hashCode()));
//        }
     
        // We maintain a local cache of Estimates, so there is no pool to return them to
        // The MarkovPathEstimator is responsible for its own MarkovEstimate object, so we
        // don't want to return that here.
        for (int i = 0; i < this.num_estimates; i++) {
            MarkovEstimate est = (MarkovEstimate)this.estimates.get(i);
            if (est != null) est.finish();
        } // FOR
        this.num_estimates = 0;
        
        this.markov.incrementTransasctionCount();
        this.actual_path.clear();
        this.actual_path_edges.clear();
        this.current = null;
        super.finish();
    }
    
    /**
     * Get the next Estimate object for this State
     * @return
     */
    protected MarkovEstimate createNextEstimate(MarkovVertex v) {
        MarkovEstimate next = null;
        // FIXME
//        if (this.num_estimates < this.estimates.size()) {
//            next = (MarkovEstimate)this.estimates.get(this.num_estimates);
//        } else {
            next = new MarkovEstimate(this.catalogContext);
            this.estimates.add(next);
//        }
        next.init(v, this.num_estimates++);
        return (next);
    }

    // ----------------------------------------------------------------------------
    // MARKOV GRAPH METHODS
    // ----------------------------------------------------------------------------
    
    public MarkovGraph getMarkovGraph() {
        return (this.markov);
    }

    public List<MarkovVertex> getActualPath() {
        return (this.actual_path);
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
    
    /**
     * Debug method to dump out the Markov graph to a Graphviz file
     * Returns the path to the file
     */
    public File dumpMarkovGraph() {
        List<MarkovEdge> initialPath = this.markov.getPath(((MarkovEstimate)this.getInitialEstimate()).getMarkovPath());
        List<MarkovEdge> actualPath = this.markov.getPath(this.actual_path);
        GraphvizExport<MarkovVertex, MarkovEdge> gv = MarkovUtil.exportGraphviz(this.markov, true, initialPath);
        gv.highlightPath(actualPath, "blue");
        return gv.writeToTempFile(this.markov.getProcedure());
    }
    
    @Override
    public String toString() {
        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
        m0.put("TransactionId", this.txn_id);
        m0.put("Procedure", this.markov.getProcedure().getName());
        m0.put("MarkovGraph Id", this.markov.getGraphId());
        
        Map<String, Object> m1 = new LinkedHashMap<String, Object>();
        m1.put("Initial Estimate", this.getInitialEstimate().toString());
        
        Map<String, Object> m2 = new LinkedHashMap<String, Object>();
        m2.put("Actual Partitions", this.getTouchedPartitions());
        m2.put("Current Estimate", this.current.debug());
        
        return StringUtil.formatMaps(m0, m1, m2);
    }
} // END CLASS