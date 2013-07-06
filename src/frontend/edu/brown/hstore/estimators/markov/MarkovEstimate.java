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
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;

public class MarkovEstimate implements Poolable, DynamicTransactionEstimate {
    private static final Logger LOG = Logger.getLogger(MarkovEstimate.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final CatalogContext catalogContext;
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA
    // ----------------------------------------------------------------------------
    
    protected float confidence = EstimatorUtil.NULL_MARKER;
//    protected float singlepartition;
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
    private final FastIntHistogram touched;
    
    // Probabilities
    private final float done[];
//    private final float read[];
    private final float write[];
    
    // ----------------------------------------------------------------------------
    // TRANSIENT DATA
    // ----------------------------------------------------------------------------
    
    // Cached
    private transient PartitionSet done_partitionset;
    private transient PartitionSet touched_partitionset;
    private transient PartitionSet most_touched_partitionset;
    private transient PartitionSet read_partitionset;
    private transient PartitionSet write_partitionset;
    
    private List<CountedStatement> query_estimate[];
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTORS + INITIALIZATION
    // ----------------------------------------------------------------------------
    
    @SuppressWarnings("unchecked")
    public MarkovEstimate(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
        
        this.touched = new FastIntHistogram(true, this.catalogContext.numberOfPartitions); 
        this.done = new float[this.catalogContext.numberOfPartitions];
//        this.read = new float[this.catalogContext.numberOfPartitions];
        this.write = new float[this.catalogContext.numberOfPartitions];
        this.query_estimate = (List<CountedStatement>[])new List<?>[this.catalogContext.numberOfPartitions];
        
        this.finish(); // initialize!
        this.initializing = false;
    }
    
    /**
     * Given an empty estimate object and the current Vertex, we fill in the
     * relevant information for the transaction coordinator to use.
     * @param v the Vertex we are currently at in the MarkovGraph
     * @param batch the current batch id
     */
    public MarkovEstimate init(MarkovVertex v, int batch) {
        assert(v != null);
        assert(this.initializing == false);
        assert(this.vertex == null) : "Trying to initialize the same object twice!";
        
        this.confidence = 1.0f;
        this.batch = batch;
        this.vertex = v;
        this.time = v.getRemainingExecutionTime();
        
        return (this);
    }
    
    @Override
    public final boolean isInitialized() {
        return (this.vertex != null); //  && this.path.isEmpty() == false);
    }
    
    @Override
    public void finish() {
        if (this.initializing == false) {
            if (debug.val) LOG.debug(String.format("Cleaning up MarkovEstimate [hashCode=%d]", this.hashCode()));
            this.vertex = null;
        }
        for (int i = 0; i < this.done.length; i++) {
            this.done[i] = EstimatorUtil.NULL_MARKER;
//            this.read[i] = EstimatorUtil.NULL_MARKER;
            this.write[i] = EstimatorUtil.NULL_MARKER;
            if (this.query_estimate[i] != null) this.query_estimate[i].clear();
        } // FOR
        this.confidence = EstimatorUtil.NULL_MARKER;
//        this.singlepartition = EstimatorUtil.NULL_MARKER;
        this.abort = EstimatorUtil.NULL_MARKER;
        this.greatest_abort = EstimatorUtil.NULL_MARKER;
        this.path.clear();
        
        this.touched.clearValues();
        this.touched_partitions.clear();
        this.read_partitions.clear();
        this.write_partitions.clear();
        
        if (this.done_partitionset != null) this.done_partitionset.clear();
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
            if (debug.val) LOG.warn("MarkovGraph vertex is null");
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
        return ((1 - this.done[partition]) >= t.getDoneThreshold());
    }
    public int getTouchedCounter(int partition) {
        return ((int)this.touched.get(partition, 0));
    }
    
    // ----------------------------------------------------------------------------
    // PARTITION COUNTERS
    // ----------------------------------------------------------------------------
    
    @Override
    public PartitionSet getTouchedPartitions(EstimationThresholds t) {
        return (this.touched_partitions);
//        assert(t != null);
//        if (this.touched_partitionset == null) {
//            this.touched_partitionset = new PartitionSet();
//        }
//        this.getPartitions(this.touched_partitionset, this.finished, t.getFinishedThreshold(), true);
//        return (this.touched_partitionset);
    }
    
    /**
     * Increment an internal counter of the number of Statements
     * that are going to be executed at each partition
     * @param partition
     */
    protected void incrementTouchedCounter(int partition) {
        this.touched.put(partition);
    }
    
    protected void incrementTouchedCounter(PartitionSet partitions) {
        for (int partition : partitions) {
            this.touched.put(partition);
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
    
//    @Override
//    public void addSinglePartitionProbability(float probability) {
//        this.singlepartition = probability + (this.singlepartition == EstimatorUtil.NULL_MARKER ? 0 : this.singlepartition);
//        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
//        if (trace.val) LOG.trace(String.format("SET Global - SINGLE-P %.02f", this.singlepartition));
//    }
//    @Override
//    public void setSinglePartitionProbability(float probability) {
//        this.singlepartition = probability;
//        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
//        if (trace.val) LOG.trace(String.format("SET Global - SINGLE-P %.02f", this.singlepartition));
//    }
//    @Override
//    public float getSinglePartitionProbability() {
//        return (this.singlepartition);
//    }
//    @Override
//    public boolean isSinglePartitionProbabilitySet() {
//        return (this.singlepartition != EstimatorUtil.NULL_MARKER);
//    }
    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    
//    @Override
//    public void addReadOnlyProbability(int partition, float probability) {
//        this.read[partition] = probability + (this.read[partition] == EstimatorUtil.NULL_MARKER ? 0 : this.read[partition]); 
//        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
//        if (trace.val) LOG.trace(String.format("SET Partition %02d - READONLY %.02f", partition, this.read[partition]));
//    }
//    @Override
//    public void setReadOnlyProbability(int partition, float probability) {
//        assert(partition >= 0) : "Invalid Partition: " + partition;
//        assert(partition < this.read.length) : "Invalid Partition: " + partition;
//        this.read[partition] = probability;
//        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
//        if (trace.val) LOG.trace(String.format("SET Partition %02d - READONLY %.02f", partition, this.read[partition]));
//    }
//    @Override
//    public float getReadOnlyProbability(int partition) {
//        return (this.read[partition]);
//    }
//    @Override
//    public boolean isReadOnlyProbabilitySet(int partition) {
//        return (this.read[partition] != EstimatorUtil.NULL_MARKER);
//    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addWriteProbability(int partition, float probability) {
        this.write[partition] = probability + (this.write[partition] == EstimatorUtil.NULL_MARKER ? 0 : this.write[partition]);
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.val)
            LOG.trace(String.format("SET Partition %02d - WRITE %.02f / hash=%d",
                      partition, this.write[partition], this.hashCode()));
    }
    @Override
    public void setWriteProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.write.length) : "Invalid Partition: " + partition;
        this.write[partition] = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.val)
            LOG.trace(String.format("SET Partition %02d - WRITE %.02f / hash=%d",
                      partition, this.write[partition], this.hashCode()));
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
    public void addDoneProbability(int partition, float probability) {
        this.done[partition] = probability + (this.done[partition] == EstimatorUtil.NULL_MARKER ? 0 : this.done[partition]);
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.val)
            LOG.trace(String.format("SET Partition %02d - FINISH %.02f / hash=%d",
                      partition, this.done[partition], this.hashCode()));
    }
    @Override
    public void setDoneProbability(int partition, float probability) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.done.length) : "Invalid Partition: " + partition;
        this.done[partition] = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.val)
            LOG.trace(String.format("SET Partition %02d - FINISH %.02f / hash=%d",
                      partition, this.done[partition], this.hashCode()));
    }
    @Override
    public float getDoneProbability(int partition) {
        return (this.done[partition]);
    }
    @Override
    public boolean isDoneProbabilitySet(int partition) {
        return (this.done[partition] != EstimatorUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addAbortProbability(float probability) {
        this.abort = probability + (this.abort == EstimatorUtil.NULL_MARKER ? 0 : this.abort); 
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.val)
            LOG.trace(String.format("SET Global - ABORT %.02f / hash=%d",
                      this.abort, this.hashCode()));
    }
    @Override
    public void setAbortProbability(float probability) {
        this.abort = probability;
        this.valid = this.valid && (probability != EstimatorUtil.NULL_MARKER);
        if (trace.val)
            LOG.trace(String.format("SET Global - ABORT %.02f",
                      this.abort, this.hashCode()));
    }
    @Override
    public float getAbortProbability() {
        return (this.abort);
    }
    @Override
    public boolean isAbortProbabilitySet() {
        return (this.abort != EstimatorUtil.NULL_MARKER);
    }
    @Override
    public boolean isAbortable(EstimationThresholds t) {
        return (this.abort >= t.getAbortThreshold());
    }
    
    // ----------------------------------------------------------------------------
    // SINGLE-PARTITION
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isSinglePartitioned(EstimationThresholds t) {
        return (this.getTouchedPartitions(t).size() <= 1);
    }
    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------

    @Override
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
        if (this.write[partition] != EstimatorUtil.NULL_MARKER) {
            float readOnly = 1.0f - this.write[partition];
            boolean ret = (readOnly >= t.getReadThreshold());
            if (trace.val)
                LOG.trace(String.format("Partition %d: (1.0 - WRITE[%.03f]) = READ_ONLY[%.03f] >= %.03f ==> %s",
                          partition, this.write[partition], readOnly, t.getReadThreshold(), ret));
            return (ret);
        }
        return (true);
    }
    @Override
    public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
        for (int partition = 0; partition < this.write.length; partition++) {
            if (this.isReadOnlyPartition(t, partition) == false) {
                return (false);
            }
        } // FOR
        return (true);
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isWritePartition(EstimationThresholds t, int partition) {
        return (this.write[partition] >= t.getWriteThreshold());
    }
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isDonePartition(EstimationThresholds t, int partition) {
        return (this.done[partition] >= t.getDoneThreshold());
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public long getRemainingExecutionTime() {
        return (this.time);
    }
    
    private void getPartitions(PartitionSet partitions, float values[], float limit, boolean inverse) {
        partitions.clear();
        for (int partition = 0; partition < values.length; partition++) {
            if ( (inverse == true && ((1 - values[partition]) >= limit)) ||
                 (inverse == false && (values[partition] >= limit))) {
                partitions.add(partition);
            }
        } // FOR
    }

//    @Override
//    public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
//        assert(t != null);
//        if (this.read_partitionset == null) {
//            this.read_partitionset = new PartitionSet();
//        }
//        PartitionSet doneP = this.getDonePartitions(t);
//        PartitionSet writeP = this.getWritePartitions(t);
//        
//        this.getPartitions(this.read_partitionset, this.read, (float)t.getReadThreshold(), false);
//        return (this.read_partitionset);
//    }
    @Override
    public PartitionSet getWritePartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.write_partitionset == null) {
            this.write_partitionset = new PartitionSet();
        }
        this.getPartitions(this.write_partitionset, this.write, (float)t.getWriteThreshold(), false);
        return (this.write_partitionset);
    }
    @Override
    public PartitionSet getDonePartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.done_partitionset == null) {
            this.done_partitionset = new PartitionSet();
        }
        this.getPartitions(this.done_partitionset, this.done, (float)t.getDoneThreshold(), false);
        return (this.done_partitionset);
    }
    
    public PartitionSet getMostTouchedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.touched_partitionset == null) this.touched_partitionset = new PartitionSet();
        this.getPartitions(this.touched_partitionset, this.done, t.getDoneThreshold(), true);
        
        if (this.most_touched_partitionset == null) {
            this.most_touched_partitionset = new PartitionSet();
        }
        int max_ctr = 0;
        for (int p : this.touched_partitionset.values()) {
            int numTouched = (int)this.touched.get(p, 0); 
            if (numTouched > 0 && max_ctr <= numTouched) {
                if (max_ctr == numTouched) this.most_touched_partitionset.add(p);
                else {
                    this.most_touched_partitionset.clear();
                    this.most_touched_partitionset.add(p);
                    max_ctr = numTouched;
                }
            }
        } // FOR
        return (this.most_touched_partitionset);
    }
    
    @Override
    public String toString() {
        final String fmt = "%-6.02f"; 
        
        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
        m0.put("BatchEstimate", (this.batch == EstimatorUtil.INITIAL_ESTIMATE_BATCH ?
                                 "<INITIAL>" : "#" + this.batch));
        m0.put("HashCode", this.hashCode());
        m0.put("Valid", this.valid);
        m0.put("Vertex", this.vertex);
        m0.put("Confidence", String.format(fmt, this.confidence));
//        m0.put("Single-Partition", (this.singlepartition != EstimatorUtil.NULL_MARKER ?
//                                    String.format(f, this.singlepartition) : "-"));
        m0.put("User Abort", (this.abort != EstimatorUtil.NULL_MARKER ? String.format(fmt, this.abort) : "-"));
        
        String header[] = {
            "",
//            "READ_ONLY",
            "WRITE",
            "DONE",
            "TOUCH_CTR",
        };
        Object rows[][] = new Object[this.done.length][];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = new String[] {
                String.format("Partition #%02d", i),
//                (this.read[i] != EstimatorUtil.NULL_MARKER ? String.format(fmt, this.read[i]) : "-"),
                (this.write[i] != EstimatorUtil.NULL_MARKER ? String.format(fmt, this.write[i]) : "-"),
                (this.done[i] != EstimatorUtil.NULL_MARKER ? String.format(fmt, this.done[i]) : "-"),
                Integer.toString((int)this.touched.get(i, 0)),
            };
        } // FOR
        Map<String, String> m1 = TableUtil.tableMap(header, rows);

        return (StringUtil.formatMapsBoxed(m0, m1));
    }
}
