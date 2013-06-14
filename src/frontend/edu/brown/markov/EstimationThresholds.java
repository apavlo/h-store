package edu.brown.markov;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * Represents a set of thresholds used when estimating transaction information
 */
public class EstimationThresholds implements JSONSerializable {
    
    private static final float DEFAULT_THRESHOLD = 0.80f;

    public enum Members {
        SINGLE_PARTITION,
        READ,
        WRITE,
        FINISHED,
        ABORT,
    };
    
    public float single_partition = DEFAULT_THRESHOLD;
    public float read = DEFAULT_THRESHOLD;
    public float write = DEFAULT_THRESHOLD;
    public float done = DEFAULT_THRESHOLD;
    public float abort = 0.0001f;
    
    public EstimationThresholds() {
        // Nothing to see here...
    }
    
    public EstimationThresholds(float default_value) {
        this.single_partition = default_value;
        this.read = default_value;
        this.write = default_value;
        this.done = default_value;
//        this.abort = default_value;
    }
    
    private static EstimationThresholds CACHED_DEFAULT; 
    public static EstimationThresholds factory() {
        if (CACHED_DEFAULT == null) {
            synchronized (EstimationThresholds.class) {
                if (CACHED_DEFAULT == null) {
                    CACHED_DEFAULT = new EstimationThresholds();
                }
            } // SYNCH
        }
        return (CACHED_DEFAULT);
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
    public void setSinglePartitionThreshold(float single_partition) {
        this.single_partition = single_partition;
    }

    /**
     * @return the read
     */
    public float getReadThreshold() {
        return this.read;
    }
    /**
     * @param read the read to set
     */
    public void setReadThreshold(float read) {
        this.read = read;
    }

    /**
     * @return the write
     */
    public float getWriteThreshold() {
        return this.write;
    }
    /**
     * @param write the write to set
     */
    public void setWriteThreshold(float write) {
        this.write = write;
    }

    /**
     * @return the done
     */
    public float getDoneThreshold() {
        return this.done;
    }
    /**
     * @param done the done to set
     */
    public void setDoneThreshold(float done) {
        this.done = done;
    }

    /**
     * @return the abort
     */
    public float getAbortThreshold() {
        return this.abort;
    }
    /**
     * @param abort the abort to set
     */
    public void setAbortThreshold(float abort) {
        this.abort = abort;
    }
    
    @Override
    public String toString() {
        Class<?> confClass = this.getClass();
        StringBuilder sb = new StringBuilder();
        for (Field f : confClass.getFields()) {
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            if (sb.length() > 0) sb.append(", ");
            sb.append(String.format("%s=%s", f.getName().toUpperCase(), obj));
        } // FOR
        return sb.toString();
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(File output_path) throws IOException {
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
