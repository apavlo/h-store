package edu.brown.hstore.estimators.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import edu.brown.graphs.GraphvizExport;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovEdge;
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
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final List<MarkovVertex> actual_path = new ArrayList<MarkovVertex>();
    protected final List<MarkovEdge> actual_path_edges = new ArrayList<MarkovEdge>();
    
    private MarkovGraph markov;
    private MarkovVertex current;
    private Object args[];
    
    protected final PartitionSet cache_past_partitions = new PartitionSet();
    protected final PartitionSet cache_last_partitions = new PartitionSet();
    
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
     */
    private MarkovEstimatorState(CatalogContext catalogContext) {
        super(catalogContext);
    }
    
    public void init(Long txn_id, int base_partition, MarkovGraph markov, Object args[], long start_time) {
        this.markov = markov;
        this.args = args;
        this.setCurrent(markov.getStartVertex(), null);
        super.init(txn_id, base_partition, start_time);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.markov != null && super.isInitialized());
    }
    
    @Override
    public void finish() {
        this.markov.incrementTransasctionCount();
        this.actual_path.clear();
        this.actual_path_edges.clear();
        this.current = null;
        this.markov = null;
        this.args = null;
        super.finish();
    }
    
    /**
     * Get the next Estimate object for this State
     * This will not add the Estimate to this the State's list
     * That must be done separately.
     * @return
     */
    protected MarkovEstimate createNextEstimate(MarkovVertex v, boolean initial) {
        assert(v != null);
        MarkovEstimate next = new MarkovEstimate(this.catalogContext);
        int batchId = (initial ? EstimatorUtil.INITIAL_ESTIMATE_BATCH : this.getEstimateCount());
        next.init(v, batchId);
        return (next);
    }

    @Override
    protected void addInitialEstimate(Estimate estimate) {
        super.addInitialEstimate(estimate);
    }
    
    @Override
    protected Estimate addEstimate(Estimate est) {
        return super.addEstimate(est);
    }
    
    @Override
    protected void shouldAllowUpdates(boolean enable) {
        super.shouldAllowUpdates(enable);
    }
    
    // ----------------------------------------------------------------------------
    // MARKOV GRAPH METHODS
    // ----------------------------------------------------------------------------
    
    public Object[] getProcedureParameters() {
        return (this.args);
    }
    
    public MarkovGraph getMarkovGraph() {
        return (this.markov);
    }

    /**
     * Get the actual path that the txn took through the MarkovGraph
     * This is only updated if transaction updates are enabled.
     * @return
     */
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
        m0.put("Procedure", (this.markov != null ? this.markov.getProcedure().getName() : null));
        m0.put("MarkovGraph Id", (this.markov != null ? this.markov.getGraphId() : null));
        
        Map<String, Object> m1 = new LinkedHashMap<String, Object>();
        m1.put("Initial Estimate", this.getInitialEstimate());
        
        Map<String, Object> m2 = new LinkedHashMap<String, Object>();
        m2.put("Actual Partitions", this.getTouchedPartitions());
        m2.put("Current Estimate", (this.current != null ? this.current.debug() : null));
        
        return StringUtil.formatMaps(m0, m1, m2);
    }
} // END CLASS