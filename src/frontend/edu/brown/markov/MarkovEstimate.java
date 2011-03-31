package edu.brown.markov;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;

import edu.brown.utils.Poolable;
import edu.brown.utils.StringUtil;

public class MarkovEstimate implements Poolable {
    // Global
    private double singlepartition;
    private double userabort;

    // Partition-specific
    private final int touched[];
    
    private final double finished[];
    private Set<Integer> finished_partitions;
    private Set<Integer> target_partitions;
    
    private final double read[];
    private Set<Integer> read_partitions;
    
    private final double write[];
    private Set<Integer> write_partitions;

    private transient Vertex vertex;
    private transient int batch;
    private transient Long time;

    protected MarkovEstimate(int num_partitions) {
        this.touched = new int[num_partitions];
        this.finished = new double[num_partitions];
        this.read = new double[num_partitions];
        this.write = new double[num_partitions];
        this.finish(); // initialize!
    }
    
    /**
     * Given an empty estimate object and the current Vertex, we fill in the
     * relevant information for the transaction coordinator to use.
     * @param estimate the Estimate object which will be filled in
     * @param v the Vertex we are currently at in the MarkovGraph
     */
    public MarkovEstimate init(Vertex v, int batch) {
        assert(v != null);
        this.batch = batch;
        this.vertex = v;
        
        if (this.vertex.isStartVertex() == false) {
            this.singlepartition = v.getSingleSitedProbability();
            this.userabort = v.getAbortProbability();
            this.time = v.getExecutiontime();
            for (int i = 0; i < this.touched.length; i++) {
                this.finished[i] = v.getDoneProbability(i);
                this.read[i] = v.getReadOnlyProbability(i);
                this.write[i] = v.getWriteProbability(i);
            } // FOR
        }
        return (this);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.vertex != null);
    }
    
    @Override
    public void finish() {
        this.vertex = null;
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
    }
    
    /**
     * Returns true if this estimate is valid and can be used by the runtime system
     * @return
     */
    public boolean isValid() {
        if (this.vertex == null) return (false);
        
        for (int i = 0; i < this.touched.length; i++) {
            if (this.finished[i] == MarkovUtil.NULL_MARKER ||
                this.read[i] == MarkovUtil.NULL_MARKER ||
                this.write[i] == MarkovUtil.NULL_MARKER) {
                return (false);
            }
        } // FOR
        if (this.singlepartition == MarkovUtil.NULL_MARKER) return (false);
        if (this.userabort == MarkovUtil.NULL_MARKER) return (false);
        return (true);
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
    protected void setSinglePartitionProbability(double prob) {
        this.singlepartition = prob;
    }
    protected void setAbortProbability(double prob) {
        this.userabort = prob;
    }
    protected void setReadOnlyPartitionProbability(int partition, double prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.read.length) : "Invalid Partition: " + partition;
        this.read[partition] = prob;
    }
    protected void setWritePartitionProbability(int partition, double prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.write.length) : "Invalid Partition: " + partition;
        this.write[partition] = prob;    
    }
    protected void setFinishPartitionProbability(int partition, double prob) {
        assert(partition >= 0) : "Invalid Partition: " + partition;
        assert(partition < this.finished.length) : "Invalid Partition: " + partition;
        this.finished[partition] = prob;
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
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
        return (this.read[partition] >= t.getReadThreshold());
    }
    public boolean isWritePartition(EstimationThresholds t, int partition) {
        return (this.write[partition] >= t.getWriteThreshold());
    }
    public boolean isFinishedPartition(EstimationThresholds t, int partition) {
        return (this.finished[partition] >= t.getDoneThreshold());
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
    public double getSinglePartitionProbability() {
        return (this.singlepartition);
    }
    public double getAbortProbability() {
        return (this.userabort);
    }
    public double getReadOnlyProbablity(int partition) {
        return (this.read[partition]);
    }
    public double getWriteProbability(int partition) {
        return (this.write[partition]);
    }
    public double getFinishedProbability(int partition) {
        return (this.finished[partition]);
    }

    public long getExecutionTime() {
        return time;
    }
    
    private void getPartitions(Set<Integer> partitions, double values[], double limit, boolean inverse) {
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
        this.getPartitions(this.read_partitions, this.read, t.getReadThreshold(), false);
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
        this.getPartitions(this.write_partitions, this.write, t.getWriteThreshold(), false);
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
        this.getPartitions(this.finished_partitions, this.finished, t.getDoneThreshold(), false);
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
        this.getPartitions(this.target_partitions, this.finished, t.getDoneThreshold(), true);
        return (this.target_partitions);
    }
    
    @Override
    public String toString() {
        final String f = "%-6.02f"; 
        
        Map<String, Object> m0 = new ListOrderedMap<String, Object>();
        m0.put("Batch Estimate", "#" + this.batch);
        m0.put("Vertex", this.vertex);
        m0.put("Single-Partition", String.format(f, this.singlepartition));
        m0.put("User Abort", String.format(f, this.userabort));
        
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
                String.format("Partition %02d", i),
                String.format(f, this.read[i]),
                String.format(f, this.write[i]),
                String.format(f, this.finished[i]),
                String.format("%d", this.touched[i]),
            };
        } // FOR
        Map<String, String> m1 = StringUtil.tableMap(header, rows);

        return (StringUtil.formatMapsBoxed(m0, m1));
    }
}
