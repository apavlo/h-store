package edu.brown.markov;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import edu.brown.utils.LoggerUtil;
import edu.brown.utils.Poolable;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class MarkovEstimate implements Poolable {
    private static final Logger LOG = Logger.getLogger(MarkovEstimate.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }


    // Global
    private float singlepartition;
    private float userabort;

    // Partition-specific
    private final int touched[];
    
    private final float finished[];
    private Set<Integer> finished_partitions;
    private Set<Integer> target_partitions;
    
    private final float read[];
    private Set<Integer> read_partitions;
    
    private final float write[];
    private Set<Integer> write_partitions;

    private transient Vertex vertex;
    private transient int batch;
    private transient Long time;
    private transient boolean initializing = true;
    private transient Boolean valid = null;

    public int reused = 0;
    
    protected MarkovEstimate(int num_partitions) {
        this.touched = new int[num_partitions];
        this.finished = new float[num_partitions];
        this.read = new float[num_partitions];
        this.write = new float[num_partitions];
        this.finish(); // initialize!
        this.initializing = false;
    }
    
    /**
     * Given an empty estimate object and the current Vertex, we fill in the
     * relevant information for the transaction coordinator to use.
     * @param estimate the Estimate object which will be filled in
     * @param v the Vertex we are currently at in the MarkovGraph
     */
    public MarkovEstimate init(Vertex v, int batch) {
        assert(v != null);
        assert(this.initializing == false);
        assert(this.vertex == null) : "Trying to initialize the same object twice!";
        this.batch = batch;
        this.vertex = v;
        
        if (this.vertex.isStartVertex() == false) {
            this.setSinglePartitionProbability(v.getSingleSitedProbability());
            this.setAbortProbability(v.getAbortProbability());
            for (int i = 0; i < this.touched.length; i++) {
                this.setFinishPartitionProbability(i, v.getDoneProbability(i));
                this.setReadOnlyPartitionProbability(i, v.getReadOnlyProbability(i));
                this.setWritePartitionProbability(i, v.getWriteProbability(i));
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
        this.singlepartition = MarkovUtil.NULL_MARKER;
        this.userabort = MarkovUtil.NULL_MARKER;
        
        if (this.finished_partitions != null) this.finished_partitions.clear();
        if (this.target_partitions != null) this.target_partitions.clear();
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
    
    /**
     * The last vertex in this batch
     * @return
     */
    public Vertex getVertex() {
        return vertex;
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
    
    // ----------------------------------------------------------------------------
    // Probabilities
    // ----------------------------------------------------------------------------
    protected void setSinglePartitionProbability(float prob) {
        this.singlepartition = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    protected void setAbortProbability(float prob) {
        this.userabort = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    protected void setReadOnlyPartitionProbability(int partition, float prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.read.length) : "Invalid Partition: " + partition;
        this.read[partition] = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    protected void setWritePartitionProbability(int partition, float prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.write.length) : "Invalid Partition: " + partition;
        this.write[partition] = prob;    
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    protected void setFinishPartitionProbability(int partition, float prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.finished.length) : "Invalid Partition: " + partition;
        this.finished[partition] = prob;
        if (prob == MarkovUtil.NULL_MARKER) this.valid = false;
        else if (this.valid == null) this.valid = true;
    }
    
    // ----------------------------------------------------------------------------
    // Convenience methods using EstimationThresholds object
    // ----------------------------------------------------------------------------
    
    private boolean checkProbabilityAllPartitions(float probs[], double threshold) {
        for (int partition = 0; partition < probs.length; partition++) {
            if (threshold < probs[partition]) return (false);
        } // FOR
        return (true);
    }
    
    public boolean isSinglePartition(EstimationThresholds t) {
        return (this.singlepartition >= t.getSinglePartitionThreshold());
    }
    public boolean isUserAbort(EstimationThresholds t) {
        return (this.userabort >= t.getAbortThreshold());
    }
    public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
        return (this.checkProbabilityAllPartitions(this.read, t.getReadThreshold()));
    }
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
        return (this.read[partition] >= t.getReadThreshold());
    }
    public boolean isWritePartition(EstimationThresholds t, int partition) {
        return (this.write[partition] >= t.getWriteThreshold());
    }
    public boolean isWriteAllPartitions(EstimationThresholds t) {
        return (this.checkProbabilityAllPartitions(this.write, t.getWriteThreshold()));
    }
    public boolean isFinishedPartition(EstimationThresholds t, int partition) {
        return (this.finished[partition] >= t.getDoneThreshold());
    }
    public boolean isFinishedAllPartitions(EstimationThresholds t) {
        return (this.checkProbabilityAllPartitions(this.finished, t.getDoneThreshold()));
    }
    public boolean isTargetPartition(EstimationThresholds t, int partition) {
        return ((1 - this.finished[partition]) >= t.getDoneThreshold());
    }

    public boolean isSinglePartitionProbabilitySet() {
        return (this.singlepartition != MarkovUtil.NULL_MARKER);
    }
    public boolean isAbortProbabilitySet() {
        return (this.userabort != MarkovUtil.NULL_MARKER);
    }
    public boolean isReadOnlyProbabilitySet(int partition) {
        return (this.read[partition] != MarkovUtil.NULL_MARKER);
    }
    public boolean isWriteProbabilitySet(int partition) {
        return (this.write[partition] != MarkovUtil.NULL_MARKER);
    }
    public boolean isFinishedProbabilitySet(int partition) {
        return (this.finished[partition] != MarkovUtil.NULL_MARKER);
    }
    
    public int getTouchedCounter(int partition) {
        return (this.touched[partition]);
    }
    public float getSinglePartitionProbability() {
        return (this.singlepartition);
    }
    public float getAbortProbability() {
        return (this.userabort);
    }
    public float getReadOnlyProbablity(int partition) {
        return (this.read[partition]);
    }
    public float getWriteProbability(int partition) {
        return (this.write[partition]);
    }
    public float getFinishedProbability(int partition) {
        return (this.finished[partition]);
    }

    public long getExecutionTime() {
        return time;
    }
    
    private void getPartitions(Set<Integer> partitions, float values[], float limit, boolean inverse) {
        partitions.clear();
        for (int i = 0; i < values.length; i++) {
            if (inverse) {
                if ((1 - values[i]) >= limit) partitions.add(i);
            } else {
                if (values[i] >= limit) partitions.add(i);
            }
        } // FOR
    }

    /**
     * Get the partitions that this transaction will only read from
     * @param t
     * @return
     */
    public Set<Integer> getReadOnlyPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.read_partitions == null) this.read_partitions = new HashSet<Integer>();
        this.getPartitions(this.read_partitions, this.read, (float)t.getReadThreshold(), false);
        return (this.read_partitions);
    }
    /**
     * Get the partitions that this transaction will write to
     * @param t
     * @return
     */
    public Set<Integer> getWritePartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.write_partitions == null) this.write_partitions = new HashSet<Integer>();
        this.getPartitions(this.write_partitions, this.write, (float)t.getWriteThreshold(), false);
        return (this.write_partitions);
    }
    /**
     * Get the partitions that this transaction is finished with at this point in the transaction
     * @param t
     * @return
     */
    public Set<Integer> getFinishedPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.finished_partitions == null) this.finished_partitions = new HashSet<Integer>();
        this.getPartitions(this.finished_partitions, this.finished, (float)t.getDoneThreshold(), false);
        return (this.finished_partitions);
    }
    /**
     * Get the partitions that this transaction will need to read/write data on 
     * @param t
     * @return
     */
    public Set<Integer> getTargetPartitions(EstimationThresholds t) {
        assert(t != null);
        if (this.target_partitions == null) this.target_partitions = new HashSet<Integer>();
        this.getPartitions(this.target_partitions, this.finished, (float)t.getDoneThreshold(), true);
        return (this.target_partitions);
    }
    
    @Override
    public String toString() {
        final String f = "%-6.02f"; 
        
        Map<String, Object> m0 = new ListOrderedMap<String, Object>();
        m0.put("BatchEstimate", "#" + this.batch);
        m0.put("HashCode", this.hashCode());
        m0.put("Valid", this.valid);
        m0.put("Reused Ctr", this.reused);
        m0.put("Vertex", this.vertex);
        m0.put("Single-P", (this.singlepartition != MarkovUtil.NULL_MARKER ? String.format(f, this.singlepartition) : "-"));
        m0.put("User Abort", (this.userabort != MarkovUtil.NULL_MARKER ? String.format(f, this.userabort) : "-"));
        
        String header[] = {
            "",
            "ReadO",
            "Write",
            "Finished",
            "Counter",
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
