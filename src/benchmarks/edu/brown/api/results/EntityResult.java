package edu.brown.api.results;

import java.io.File;
import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class EntityResult implements JSONSerializable {
    public long txnCount;
    public double txnPercentage;
    public double txnPerMilli;
    public double txnPerSecond;
    public double avgLatencyMilli;
    
    public EntityResult(long totalTxnCount, long duration, long txnCount) {
        this.txnCount = txnCount;
        if (totalTxnCount == 0) {
            this.txnPercentage = 0;
            this.txnPerMilli = 0;
            this.txnPerSecond = 0;
        } else {
            this.txnPercentage = (txnCount / (double)totalTxnCount) * 100;
            this.txnPerMilli = txnCount / (double)duration * 1000.0;
            this.txnPerSecond = txnCount / (double)duration * 1000.0 * 60.0;
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
    public double getAvgLatency() {
        return this.avgLatencyMilli;
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