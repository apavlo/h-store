package edu.brown.api.results;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.statistics.HistogramUtil;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.MathUtil;

public class EntityResult implements JSONSerializable {
    public long txnCount;
    public double txnPercentage;
    public double txnPerMilli;
    public double txnPerSecond;
    
    public double txnAvgLatency = 0d;
    public double txnStdDevLatency = 0d;
    public double txnMinLatency = 0d;
    public double txnMaxLatency = 0d;
    
    public EntityResult(long totalTxnCount, long duration, long txnCount, ObjectHistogram<Integer> latencies) {
        this.txnCount = txnCount;
        if (totalTxnCount == 0) {
            this.txnPercentage = 0;
            this.txnPerMilli = 0;
            this.txnPerSecond = 0;
            this.txnAvgLatency = 0;
            this.txnStdDevLatency = 0;
            this.txnMinLatency = 0;
            this.txnMaxLatency = 0;
        } else {
            this.txnPercentage = (txnCount / (double)totalTxnCount) * 100;
            this.txnPerMilli = txnCount / (double)duration * 1000.0;
            this.txnPerSecond = txnCount / (double)duration * 1000.0 * 60.0;
            
            if (latencies.getMinValue() != null)
                this.txnMinLatency = latencies.getMinValue().doubleValue();
            if (latencies.getMaxValue() != null)
                this.txnMaxLatency = latencies.getMaxValue().doubleValue();
            Collection<Integer> allLatencies = HistogramUtil.weightedValues(latencies);
            if (allLatencies.size() > 0) {
                this.txnAvgLatency = MathUtil.sum(allLatencies) / (double)allLatencies.size();
                this.txnStdDevLatency = MathUtil.stdev(CollectionUtil.toDoubleArray(allLatencies));
            }
        }
    }
    
    public long getTxnCount() {
        return this.txnCount;
    }
    public double getTxnPercentage() {
        return this.txnPercentage;
    }
    public double getTxnPerMilli() {
        return this.txnPerMilli;
    }
    public double getTxnPerSecond() {
        return this.txnPerSecond;
    }
    public double getTxnAvgLatency() {
        return this.txnAvgLatency;
    }
    public double getTxnStdDevLatency() {
        return this.txnStdDevLatency;
    }
    public double getTxnMinLatency() {
        return this.txnMinLatency;
    }
    public double getTxnMaxLatency() {
        return this.txnMaxLatency;
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
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
        JSONUtil.fieldsToJSON(stringer, this, EntityResult.class, JSONUtil.getSerializableFields(this.getClass()));
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, EntityResult.class, true, JSONUtil.getSerializableFields(this.getClass()));
    }
}