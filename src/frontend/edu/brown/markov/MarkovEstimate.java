package edu.brown.markov;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Statement;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
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
    
    // Global
    private float confidence;
    private float singlepartition;
    private float abort;

    // Partition-specific
    private final int touched[];
    
    // Probabilities
    private final float finished[];
    private final float read[];
    private final float write[];
    
    private PartitionSet finished_partitions;
    private PartitionSet touched_partitions;
    private PartitionSet most_touched_partitions;
    private PartitionSet read_partitions;
    private PartitionSet write_partitions;

    private transient MarkovVertex vertex;
    private transient List<MarkovVertex> path;
    private transient int batch;
    private transient Long time;
    private transient boolean initializing = true;
    private transient Boolean valid = null;

    private int reused = 0;
    
    public MarkovEstimate(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
        
        this.touched = new int[this.catalogContext.numberOfPartitions];
        this.finished = new float[this.catalogContext.numberOfPartitions];
        this.read = new float[this.catalogContext.numberOfPartitions];
        this.write = new float[this.catalogContext.numberOfPartitions];
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
        this.batch = batch;
        this.vertex = v;
        
        if (this.vertex.isStartVertex() == false) {
            this.setSinglePartitionProbability(v.getSinglePartitionProbability());
            this.setAbortProbability(v.getAbortProbability());
            for (int i = 0; i < this.touched.length; i++) {
                this.setFinishProbability(i, v.getFinishProbability(i));
                this.setReadOnlyProbability(i, v.getReadOnlyProbability(i));
                this.setWriteProbability(i, v.getWriteProbability(i));
            } // FOR
            this.time = v.getExecutionTime();
        }
        return (this);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.vertex != null);
    }
    
    @Override
    public void finish() {
        if (this.initializing == false) {
            if (debug.get()) LOG.debug(String.format("Cleaning up MarkovEstimate [hashCode=%d]", this.hashCode()));
            this.vertex = null;
        }
        for (int i = 0; i < this.touched.length; i++) {
            this.touched[i] = 0;
            this.finished[i] = MarkovUtil.NULL_MARKER;
            this.read[i] = MarkovUtil.NULL_MARKER;
            this.write[i] = MarkovUtil.NULL_MARKER;
        } // FOR
        this.confidence = MarkovUtil.NULL_MARKER;
        this.singlepartition = MarkovUtil.NULL_MARKER;
        this.abort = MarkovUtil.NULL_MARKER;
        
        if (this.finished_partitions != null) this.finished_partitions.clear();
        if (this.touched_partitions != null) this.touched_partitions.clear();
        if (this.most_touched_partitions != null) this.most_touched_partitions.clear();
        if (this.read_partitions != null) this.read_partitions.clear();
        if (this.write_partitions != null) this.write_partitions.clear();
        this.valid = null;
    }
    
    /**
     * Returns true if this estimate is valid and can be used by the runtime system
     * @return
     */
    public boolean isValid() {
        if (this.vertex == null) {
            if (debug.get()) LOG.warn("MarkovGraph vertex is null");
            return (false);
        }
        return (this.valid != null && this.valid);
        
//        for (int i = 0; i < this.touched.length; i++) {
//            if (this.finished[i] == MarkovUtil.NULL_MARKER) {
//                if (debug.get()) LOG.warn("finished[" + i + "] is null");
//                return (false);
//            } else if (this.read[i] == MarkovUtil.NULL_MARKER) {
//                if (debug.get()) LOG.warn("read[" + i + "] is null");
//                return (false);
//            } else if (this.write[i] == MarkovUtil.NULL_MARKER) {
//                if (debug.get()) LOG.warn("write[" + i + "] is null");
//                return (false);
//            }
//        } // FOR
//        if (this.singlepartition == MarkovUtil.NULL_MARKER) return (false);
//        if (this.userabort == MarkovUtil.NULL_MARKER) return (false);
//        return (true);
    }
    
    @Override
    public boolean hasQueryList() {
        return (this.path != null);
    }
    
    @Override
    public Statement[] getEstimatedQueries(int partition) {
        // TODO Auto-generated method stub
        return null;
    }
    
    protected void setMarkovPath(List<MarkovVertex> path) {
        this.path = path;
    }
    
    public List<MarkovVertex> getMarkovPath() {
        return (this.path);
    }
    
    /**
     * The last vertex in this batch
     * @return
     */
    public MarkovVertex getVertex() {
        return (this.vertex);
    }
    
    /**
     * Return that BatchId for this Estimate
     * @return
     */
    public int getBatchId() {
        return (this.batch);
    }

    protected void incrementTouchedCounter(int partition) {
        this.touched[partition]++;
    }
    
    public int getReusedCounter() {
        return (this.reused);
    }
    
    public int incrementReusedCounter() {
        return (++this.reused);
    }
    
    // ----------------------------------------------------------------------------
    // PROBABILITIES
    // ----------------------------------------------------------------------------
    
    public float getConfidenceCoefficient() {
        return (this.confidence);
    }
    
    protected void setConfidenceProbability(float prob) {
        this.confidence = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    
    // ----------------------------------------------------------------------------
    // SINGLE-PARTITIONED PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addSinglePartitionProbability(float probability) {
        this.singlepartition = probability + (this.singlepartition == MarkovUtil.NULL_MARKER ? 0 : this.singlepartition); 
        if (probability == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public void setSinglePartitionProbability(float prob) {
        this.singlepartition = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public float getSinglePartitionProbability() {
        return (this.singlepartition);
    }
    @Override
    public boolean isSinglePartitionProbabilitySet() {
        return (this.singlepartition != MarkovUtil.NULL_MARKER);
    }

    
    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addReadOnlyProbability(int partition, float probability) {
        this.read[partition] = probability + (this.read[partition] == MarkovUtil.NULL_MARKER ? 0 : this.read[partition]); 
        if (probability == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public void setReadOnlyProbability(int partition, float prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.read.length) : "Invalid Partition: " + partition;
        this.read[partition] = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public float getReadOnlyProbability(int partition) {
        return (this.read[partition]);
    }
    @Override
    public boolean isReadOnlyProbabilitySet(int partition) {
        return (this.read[partition] != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addWriteProbability(int partition, float probability) {
        this.write[partition] = probability + (this.write[partition] == MarkovUtil.NULL_MARKER ? 0 : this.write[partition]); 
        if (probability == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public void setWriteProbability(int partition, float prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.write.length) : "Invalid Partition: " + partition;
        this.write[partition] = prob;    
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public float getWriteProbability(int partition) {
        return (this.write[partition]);
    }
    @Override
    public boolean isWriteProbabilitySet(int partition) {
        return (this.write[partition] != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------

    @Override
    public void addFinishProbability(int partition, float probability) {
        this.finished[partition] = probability + (this.finished[partition] == MarkovUtil.NULL_MARKER ? 0 : this.finished[partition]); 
        if (probability == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public void setFinishProbability(int partition, float prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.finished.length) : "Invalid Partition: " + partition;
        this.finished[partition] = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public float getFinishProbability(int partition) {
        return (this.finished[partition]);
    }
    @Override
    public boolean isFinishProbabilitySet(int partition) {
        return (this.finished[partition] != MarkovUtil.NULL_MARKER);
    }
    
    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------
    
    @Override
    public void addAbortProbability(float probability) {
        this.abort = probability + (this.abort == MarkovUtil.NULL_MARKER ? 0 : this.abort); 
        if (probability == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public void setAbortProbability(float prob) {
        this.abort = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    @Override
    public float getAbortProbability() {
        return (this.abort);
    }
    @Override
    public boolean isAbortProbabilitySet() {
        return (this.abort != MarkovUtil.NULL_MARKER);
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
    
    public boolean isTargetPartition(EstimationThresholds t, int partition) {
        return ((1 - this.finished[partition]) >= t.getFinishedThreshold());
    }
    public boolean isConfidenceProbabilitySet() {
        return (this.confidence != MarkovUtil.NULL_MARKER);
    }
    public int getTouchedCounter(int partition) {
        return (this.touched[partition]);
    }
    public float getConfidenceProbability() {
        return (this.confidence);
    }
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
        if (this.read_partitions == null) this.read_partitions = new PartitionSet();
        this.getPartitions(this.read_partitions, this.read, (float)t.getReadThreshold(), false);
        return (this.read_partitions);
    }
    @Override
    public PartitionSet getWritePartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.write_partitions == null) this.write_partitions = new PartitionSet();
        this.getPartitions(this.write_partitions, this.write, (float)t.getWriteThreshold(), false);
        return (this.write_partitions);
    }
    @Override
    public PartitionSet getFinishPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.finished_partitions == null) this.finished_partitions = new PartitionSet();
        this.getPartitions(this.finished_partitions, this.finished, (float)t.getFinishedThreshold(), false);
        return (this.finished_partitions);
    }
    @Override
    public PartitionSet getTouchedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.touched_partitions == null) this.touched_partitions = new PartitionSet();
        this.getPartitions(this.touched_partitions, this.finished, t.getFinishedThreshold(), true);
        return (this.touched_partitions);
    }
    
    public PartitionSet getMostTouchedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.touched_partitions == null) this.touched_partitions = new PartitionSet();
        this.getPartitions(this.touched_partitions, this.finished, t.getFinishedThreshold(), true);
        
        if (this.most_touched_partitions == null) this.most_touched_partitions = new PartitionSet();
        int max_ctr = 0;
        for (Integer p : this.touched_partitions) {
            if (this.touched[p.intValue()] > 0 && max_ctr <= this.touched[p.intValue()]) {
                if (max_ctr == this.touched[p.intValue()]) this.most_touched_partitions.add(p);
                else {
                    this.most_touched_partitions.clear();
                    this.most_touched_partitions.add(p);
                    max_ctr = this.touched[p.intValue()];
                }
            }
        } // FOR
        return (this.most_touched_partitions);
    }
    
    @Override
    public String toString() {
        final String f = "%-6.02f"; 
        
        Map<String, Object> m0 = new LinkedHashMap<String, Object>();
        m0.put("BatchEstimate", (this.batch == MarkovUtil.INITIAL_ESTIMATE_BATCH ? "<INITIAL>" : "#" + this.batch));
        m0.put("HashCode", this.hashCode());
        m0.put("Valid", this.valid);
        m0.put("Reused Ctr", this.reused);
        m0.put("Vertex", this.vertex);
        m0.put("Confidence", this.confidence);
        m0.put("Single-P", (this.singlepartition != MarkovUtil.NULL_MARKER ? String.format(f, this.singlepartition) : "-"));
        m0.put("User Abort", (this.abort != MarkovUtil.NULL_MARKER ? String.format(f, this.abort) : "-"));
        
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
                (this.read[i] != MarkovUtil.NULL_MARKER ? String.format(f, this.read[i]) : "-"),
                (this.write[i] != MarkovUtil.NULL_MARKER ? String.format(f, this.write[i]) : "-"),
                (this.finished[i] != MarkovUtil.NULL_MARKER ? String.format(f, this.finished[i]) : "-"),
                Integer.toString(this.touched[i]),
            };
        } // FOR
        Map<String, String> m1 = TableUtil.tableMap(header, rows);

        return (StringUtil.formatMapsBoxed(m0, m1));
    }
}
