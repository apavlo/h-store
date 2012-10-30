package edu.brown.hstore.estimators.markov;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.estimators.DynamicTransactionEstimate;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovVertex;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;

public class MarkovEstimate implements Poolable, DynamicTransactionEstimate {
    private static final Logger LOG = Logger.getLogger(MarkovEstimate.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final CatalogContext catalogContext;
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA
    // ----------------------------------------------------------------------------
    
    protected float confidence = EstimatorUtil.NULL_MARKER;
    protected float singlepartition;
    protected float abort;
    protected float greatest_abort = EstimatorUtil.NULL_MARKER;

    // MarkovPathEstimator data 
    protected final List<MarkovVertex> path = new ArrayList<MarkovVertex>();
    protected final PartitionSet touched_partitions = new PartitionSet();
    protected final PartitionSet read_partitions = new PartitionSet();
    protected final PartitionSet write_partitions = new PartitionSet();

    private MarkovVertex vertex;
    private int batch;
    private long time;
    private boolean initializing = true;
    private boolean valid = true;
    
    // ----------------------------------------------------------------------------
    // PARTITION-SPECIFIC DATA
    // ----------------------------------------------------------------------------
    
    /**
     * The number of Statements executed at each partition
     */
    private final int touched[];
    
    // Probabilities
    private final float finished[];
    private final float read[];
    private final float write[];
    
    // ----------------------------------------------------------------------------
    // TRANSIENT DATA
    // ----------------------------------------------------------------------------
    
    // Cached
    protected PartitionSet finished_partitionset;
    protected PartitionSet touched_partitionset;
    protected PartitionSet most_touched_partitionset;
    protected PartitionSet read_partitionset;
    protected PartitionSet write_partitionset;
    
    private List<CountedStatement> query_estimate[];
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS + INITIALIZATION
    // ----------------------------------------------------------------------------
    
    @SuppressWarnings("unchecked")
    public MarkovEstimate(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
        
        this.touched = new int[this.catalogContext.numberOfPartitions];
        this.finished = new float[this.catalogContext.numberOfPartitions];
        this.read = new float[this.catalogContext.numberOfPartitions];
        this.write = new float[this.catalogContext.numberOfPartitions];
        this.query_estimate = (List<CountedStatement>[])new List<?>[this.catalogContext.numberOfPartitions];
        
        this.finish(); // initialize!
        this.initializing = false;
    }
    
    /**
     * Given an empty estimate object and the current Vertex, we fill in the
     * relevant information for the transaction coordinator to use.
     * @param estimate the Estimate object which will be filled in
     * @param v the Vertex we are currently at in the MarkovGraph
     */
    public MarkovEstimate init(MarkovVertex v, int batch) {
        assert(v != null);
        assert(this.initializing == false);
        assert(this.vertex == null) : "Trying to initialize the same object twice!";
        
        this.confidence = 1.0f;
        this.batch = batch;
        this.vertex = v;
        this.time = v.getExecutionTime();
        
        return (this);
    }
    
    @Override
    public final boolean isInitialized() {
        return (this.vertex != null); //  && this.path.isEmpty() == false);
    }
    
    @Override
    public void finish() {
        if (this.initializing == false) {
            if (debug.get()) LOG.debug(String.format("Cleaning up MarkovEstimate [hashCode=%d]", this.hashCode()));
            this.vertex = null;
        }
        for (int i = 0; i < this.touched.length; i++) {
            this.touched[i] = 0;
            this.finished[i] = EstimatorUtil.NULL_MARKER;
            this.read[i] = EstimatorUtil.NULL_MARKER;
            this.write[i] = EstimatorUtil.NULL_MARKER;
            if (this.query_estimate[i] != null) this.query_estimate[i].clear();
        } // FOR
        this.confidence = EstimatorUtil.NULL_MARKER;
        this.singlepartition = EstimatorUtil.NULL_MARKER;
        this.abort = EstimatorUtil.NULL_MARKER;
        this.greatest_abort = EstimatorUtil.NULL_MARKER;
        this.path.clear();
        
        this.touched_partitions.clear();
        this.read_partitions.clear();
        this.write_partitions.clear();
        
        if (this.finished_partitionset != null) this.finished_partitionset.clear();
        if (this.touched_partitionset != null) this.touched_partitionset.clear();
        if (this.most_touched_partitionset != null) this.most_touched_partitionset.clear();
        if (this.read_partitionset != null) this.read_partitionset.clear();
        if (this.write_partitionset != null) this.write_partitionset.clear();
        this.valid = true;
    }
    
    @Override
    public boolean isInitialEstimate() {
        return (this.batch == EstimatorUtil.INITIAL_ESTIMATE_BATCH);
    }
    @Override
    public int getBatchId() {
        return (this.batch);
    }
    public boolean isValid() {
        if (this.vertex == null) {
            if (debug.get()) LOG.warn("MarkovGraph vertex is null");
            return (false);
        }
        return (this.valid);
    }
    
    @Override
    public boolean hasQueryEstimate(int partition) {
        return (this.path.isEmpty() == false && this.touched_partitions.contains(partition));
    }
    
    @Override
    public List<CountedStatement> getQueryEstimate(int partition) {
        if (this.query_estimate[partition] == null) {
            this.query_estimate[partition] = new ArrayList<CountedStatement>();
        }
        if (this.query_estimate[partition].isEmpty()) {
            for (MarkovVertex v : this.path) {
                PartitionSet partitions = v.getPartitions();
                if (partitions.contains(partition)) {
                    this.query_estimate[partition].add(v.getCountedStatement());
                }
            } // FOR
        }
        return (this.query_estimate[partition]);
    }
    
    protected CatalogContext getCatalogContext() {
        return (this.catalogContext);
    }
    
    public List<MarkovVertex> getMarkovPath() {
//        assert(this.path.isEmpty() == false) :
//            "Trying to access MarkovPath before it was set";
        return (this.path);
    }
    
    /**
     * The last vertex in this batch
     * @return
     */
    public MarkovVertex getVertex() {
        return (this.vertex);
    }
    
    public boolean isTargetPartition(EstimationThresholds t, int partition) {
        return ((1 - this.finished[partition]) >= t.getFinishedThreshold());
    }
    public int getTouchedCounter(int partition) {
        return (this.touched[partition]);
    }
    
    // ----------------------------------------------------------------------------
    // PARTITION COUNTERS
    // ----------------------------------------------------------------------------
    
    @Override
    public PartitionSet getTouchedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.touched_partitionset == null) this.touched_partitionset = new PartitionSet();
        this.getPartitions(this.touched_partitionset, this.finished, t.getFinishedThreshold(), true);
        return (this.touched_partitionset);
    }
    
    /**
     * Increment an internal counter of the number of Statements
     * that are going to be executed at each partition
     * @param partition
     */
    protected void incrementTouchedCounter(int partition) {
        this.touched[partition]++;
    }
    
    protected void incrementTouchedCounter(PartitionSet partitions) {
        for (int p = 0; p < this.touched.length; p++) {
            if (partitions.contains(p)) this.touched[p]++;
        } // FOR
    }
    
    // ----------------------------------------------------------------------------
    // CONFIDENCE COEFFICIENT
    // ----------------------------------------------------------------------------
    
    public boolean isConfidenceCoefficientSet() {
        return (this.confidence != EstimatorUtil.NULL_MARKER);
    }
    public float getConfidenceCoefficient() {
        return (this.confidence);
    }
    public void setConfidenceCoefficient(float probability) {
        this.confidence = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // SINGLE-PARTITIONED PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addSinglePartitionProbability(float probability) {
        this.singlepartition = probability + (this.singlepartition == EstimatorUtil.NULL_MARKER ? 0 : this.singlepartition);
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Global - SINGLE-P %.02f", this.singlepartition));
    }
    @Override
    public void setSinglePartitionProbability(float probability) {
        this.singlepartition = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Global - SINGLE-P %.02f", this.singlepartition));
    }
    @Override
    public float getSinglePartitionProbability() {
        return (this.singlepartition);
    }
    @Override
    public boolean isSinglePartitionProbabilitySet() {
        return (this.singlepartition != EstimatorUtil.NULL_MARKER);
    }

    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addReadOnlyProbability(int partition, float probability) {
        this.read[partition] = probability + (this.read[partition] == EstimatorUtil.NULL_MARKER ? 0 : this.read[partition]); 
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Partition %02d - READONLY %.02f", partition, this.read[partition]));
    }
    @Override
    public void setReadOnlyProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.read.length) : "Invalid Partition: " + partition;
        this.read[partition] = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Partition %02d - READONLY %.02f", partition, this.read[partition]));
    }
    @Override
    public float getReadOnlyProbability(int partition) {
        return (this.read[partition]);
    }
    @Override
    public boolean isReadOnlyProbabilitySet(int partition) {
        return (this.read[partition] != EstimatorUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addWriteProbability(int partition, float probability) {
        this.write[partition] = probability + (this.write[partition] == EstimatorUtil.NULL_MARKER ? 0 : this.write[partition]);
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Partition %02d - WRITE %.02f", partition, this.write[partition]));
    }
    @Override
    public void setWriteProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.write.length) : "Invalid Partition: " + partition;
        this.write[partition] = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Partition %02d - WRITE %.02f", partition, this.write[partition]));
    }
    @Override
    public float getWriteProbability(int partition) {
        return (this.write[partition]);
    }
    @Override
    public boolean isWriteProbabilitySet(int partition) {
        return (this.write[partition] != EstimatorUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------

    @Override
    public void addFinishProbability(int partition, float probability) {
        this.finished[partition] = probability + (this.finished[partition] == EstimatorUtil.NULL_MARKER ? 0 : this.finished[partition]);
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Partition %02d - FINISH %.02f", partition, this.finished[partition]));
    }
    @Override
    public void setFinishProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.finished.length) : "Invalid Partition: " + partition;
        this.finished[partition] = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Partition %02d - FINISH %.02f", partition, this.finished[partition]));
    }
    @Override
    public float getFinishProbability(int partition) {
        return (this.finished[partition]);
    }
    @Override
    public boolean isFinishProbabilitySet(int partition) {
        return (this.finished[partition] != EstimatorUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addAbortProbability(float probability) {
        this.abort = probability + (this.abort == EstimatorUtil.NULL_MARKER ? 0 : this.abort); 
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Global - ABORT %.02f", this.abort));
    }
    @Override
    public void setAbortProbability(float probability) {
        this.abort = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.get()) LOG.trace(String.format("SET Global - ABORT %.02f", this.abort));
    }
    @Override
    public float getAbortProbability() {
        return (this.abort);
    }
    @Override
    public boolean isAbortProbabilitySet() {
        return (this.abort != EstimatorUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // Convenience methods using EstimationThresholds object
    // ----------------------------------------------------------------------------
    
    private boolean checkProbabilityAllPartitions(float probs[], float threshold) {
        for (int partition = 0; partition < probs.length; partition++) {
            if (probs[partition] < threshold) return (false);
        } // FOR
        return (true);
    }
    
    @Override
    public boolean isSinglePartitioned(EstimationThresholds t) {
        return (this.getTouchedPartitions(t).size() <= 1);
    }
    @Override
    public boolean isAbortable(EstimationThresholds t) {
        return (this.abort >= t.getAbortThreshold());
    }
    @Override
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
        return (this.read[partition] >= t.getReadThreshold());
    }
    @Override
    public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
        return (this.checkProbabilityAllPartitions(this.read, t.getReadThreshold()));
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isWritePartition(EstimationThresholds t, int partition) {
        return (this.write[partition] >= t.getWriteThreshold());
    }
    
    // ----------------------------------------------------------------------------
    // FINISHED PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isFinishPartition(EstimationThresholds t, int partition) {
        return (this.finished[partition] >= t.getFinishedThreshold());
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public long getExecutionTime() {
        return time;
    }
    
    private void getPartitions(PartitionSet partitions, float values[], float limit, boolean inverse) {
        partitions.clear();
        if (inverse) {
            for (int i = 0; i < values.length; i++) {
                if ((1 - values[i]) >= limit)
                    partitions.add(this.catalogContext.getAllPartitionIdArray()[i]);
            } // FOR
        } else {
            for (int i = 0; i < values.length; i++) {
                if (values[i] >= limit)
                    partitions.add(this.catalogContext.getAllPartitionIdArray()[i]);
            } // FOR
        }
    }

    @Override
    public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.read_partitionset == null) this.read_partitionset = new PartitionSet();
        this.getPartitions(this.read_partitionset, this.read, (float)t.getReadThreshold(), false);
        return (this.read_partitionset);
    }
    @Override
    public PartitionSet getWritePartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.write_partitionset == null) this.write_partitionset = new PartitionSet();
        this.getPartitions(this.write_partitionset, this.write, (float)t.getWriteThreshold(), false);
        return (this.write_partitionset);
    }
    @Override
    public PartitionSet getFinishPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.finished_partitionset == null) this.finished_partitionset = new PartitionSet();
        this.getPartitions(this.finished_partitionset, this.finished, (float)t.getFinishedThreshold(), false);
        return (this.finished_partitionset);
    }
    
    public PartitionSet getMostTouchedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.touched_partitionset == null) this.touched_partitionset = new PartitionSet();
        this.getPartitions(this.touched_partitionset, this.finished, t.getFinishedThreshold(), true);
        
        if (this.most_touched_partitionset == null) this.most_touched_partitionset = new PartitionSet();
        int max_ctr = 0;
        for (Integer p : this.touched_partitionset) {
            if (this.touched[p.intValue()] > 0 && max_ctr <= this.touched[p.intValue()]) {
                if (max_ctr == this.touched[p.intValue()]) this.most_touched_partitionset.add(p);
                else {
                    this.most_touched_partitionset.clear();
                    this.most_touched_partitionset.add(p);
                    max_ctr = this.touched[p.intValue()];
                }
            }
        } // FOR
        return (this.most_touched_partitionset);
    }
    
    @Override
    public String toString() {
        final String f = "%-6.02f"; 
        
        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
        m0.put("BatchEstimate", (this.batch == EstimatorUtil.INITIAL_ESTIMATE_BATCH ? "<INITIAL>" : "#" + this.batch));
        m0.put("HashCode", this.hashCode());
        m0.put("Valid", this.valid);
        m0.put("Vertex", this.vertex);
        m0.put("Confidence", this.confidence);
        m0.put("Single-P", (this.singlepartition != EstimatorUtil.NULL_MARKER ? String.format(f, this.singlepartition) : "-"));
        m0.put("User Abort", (this.abort != EstimatorUtil.NULL_MARKER ? String.format(f, this.abort) : "-"));
        
        String header[] = {
            "",
            "ReadO",
            "Write",
            "Finished",
            "TouchCtr",
        };
        Object rows[][] = new Object[this.touched.length][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new String[] {
                String.format("Partition #%02d", i),
                (this.read[i] != EstimatorUtil.NULL_MARKER ? String.format(f, this.read[i]) : "-"),
                (this.write[i] != EstimatorUtil.NULL_MARKER ? String.format(f, this.write[i]) : "-"),
                (this.finished[i] != EstimatorUtil.NULL_MARKER ? String.format(f, this.finished[i]) : "-"),
                Integer.toString(this.touched[i]),
            };
        } // FOR
        Map<String, String> m1 = TableUtil.tableMap(header, rows);

        return (StringUtil.formatMapsBoxed(m0, m1));
    }
}
