package edu.brown.markov;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.json.*;
import org.voltdb.catalog.Database;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

/**
 * Represents a set of thresholds used when estimating transaction information
 */
public class EstimationThresholds implements JSONSerializable {
    
    private static final double DEFAULT_THRESHOLD = 0.90d;

    public enum Members {
        SINGLE_PARTITION,
        READ,
        WRITE,
        DONE,
        ABORT,
    };
    
    public double single_partition = DEFAULT_THRESHOLD;
    public double read = DEFAULT_THRESHOLD;
    public double write = DEFAULT_THRESHOLD;
    public double done = DEFAULT_THRESHOLD;
    public double abort = 0.01;
    
    public EstimationThresholds() {
        // Nothing to see here...
    }
    
    public EstimationThresholds(double default_value) {
        this.single_partition = default_value;
        this.read = default_value;
        this.write = default_value;
        this.done = default_value;
        this.abort = default_value;
    }
    
    /**
     * @return the single_partition
     */
    public double getSinglePartitionThreshold() {
        return this.single_partition;
    }
    /**
     * @param single_partition the single_partition to set
     */
    public void setSinglePartitionThreshold(double single_partition) {
        this.single_partition = single_partition;
    }

    /**
     * @return the read
     */
    public double getReadThreshold() {
        return this.read;
    }
    /**
     * @param read the read to set
     */
    public void setReadThreshold(double read) {
        this.read = read;
    }

    /**
     * @return the write
     */
    public double getWriteThreshold() {
        return this.write;
    }
    /**
     * @param write the write to set
     */
    public void setWriteThreshold(double write) {
        this.write = write;
    }

    /**
     * @return the done
     */
    public double getDoneThreshold() {
        return this.done;
    }
    /**
     * @param done the done to set
     */
    public void setDoneThreshold(double done) {
        this.done = done;
    }

    /**
     * @return the abort
     */
    public double getAbortThreshold() {
        return this.abort;
    }
    /**
     * @param abort the abort to set
     */
    public void setAbortThreshold(double abort) {
        this.abort = abort;
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Single-Partition", this.single_partition);
        m.put("Read", this.read);
        m.put("Write", this.write);
        m.put("Done", this.done);
        m.put("Abort", this.abort);
        return (StringUtil.formatMaps(m));
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------

    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, EstimationThresholds.class, EstimationThresholds.Members.values());
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, EstimationThresholds.class, EstimationThresholds.Members.values());
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        assert(args != null);
        EstimationThresholds et = new EstimationThresholds();
        System.out.println(et.toJSONString());
    }
}
