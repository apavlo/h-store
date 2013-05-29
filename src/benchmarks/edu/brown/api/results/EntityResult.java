package edu.brown.api.results;

import java.io.File;
import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.api.BenchmarkControllerUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class EntityResult implements JSONSerializable {
    public long txnCount;
    public double txnPercentage;
    public double txnPerMilli;
    public double txnPerSecond;
    
    public long dtxnCount;
    public double dtxnPercentage;
    
    public double totalAvgLatency = 0d;
    public double totalStdevLatency = 0d;
    public double totalMinLatency = 0d;
    public double totalMaxLatency = 0d;
    
    public double spAvgLatency = 0d;
    public double spStdevLatency = 0d;
    public double spMinLatency = 0d;
    public double spMaxLatency = 0d;
    
    public double dtxnAvgLatency = 0d;
    public double dtxnStdevLatency = 0d;
    public double dtxnMinLatency = 0d;
    public double dtxnMaxLatency = 0d;
    
    public EntityResult(long totalTxnCount, long duration, long txnCount, long dtxnCount,
                        Histogram<Integer> totalLatencies, Histogram<Integer> spLatencies, Histogram<Integer> dtxnLatencies) {
        this.txnCount = txnCount;
        this.dtxnCount = dtxnCount;
        if (totalTxnCount == 0) {
            this.dtxnPercentage = 0;
            this.txnPercentage = 0;
            this.txnPerMilli = 0;
            this.txnPerSecond = 0;
            this.totalAvgLatency = 0;
            this.totalStdevLatency = 0;
            this.totalMinLatency = 0;
            this.totalMaxLatency = 0;
        } else {
            this.txnPercentage = (txnCount / (double)totalTxnCount) * 100;
            this.txnPerMilli = txnCount / (double)duration * 1000.0;
            this.txnPerSecond = txnCount / (double)duration * 1000.0 * 60.0;
            if (txnCount > 0) {
                this.dtxnPercentage = (dtxnCount / (double)txnCount) * 100;
            } else {
                this.dtxnPercentage = 0;
            }
            
            if (totalLatencies.isEmpty() == false) {
                double x[] = BenchmarkControllerUtil.computeLatencies(totalLatencies);
                int i = 0;
                this.totalMinLatency = x[i++];
                this.totalMaxLatency = x[i++];
                this.totalAvgLatency = x[i++];
                this.totalStdevLatency = x[i++];
            }
            if (spLatencies.isEmpty() == false) {
                double x[] = BenchmarkControllerUtil.computeLatencies(spLatencies);
                int i = 0;
                this.spMinLatency = x[i++];
                this.spMaxLatency = x[i++];
                this.spAvgLatency = x[i++];
                this.spStdevLatency = x[i++];
            }
            if (dtxnLatencies.isEmpty() == false) {
                double x[] = BenchmarkControllerUtil.computeLatencies(dtxnLatencies);
                int i = 0;
                this.dtxnMinLatency = x[i++];
                this.dtxnMaxLatency = x[i++];
                this.dtxnAvgLatency = x[i++];
                this.dtxnStdevLatency = x[i++];
            }
        }
    }
    
    public long getTxnCount() {
        return this.txnCount;
    }
    public double getTxnPercentage() {
        return this.txnPercentage;
    }
    public long getDistributedTxnCount() {
        return this.dtxnCount;
    }
    public double getDistributedTxnPercentage() {
        return this.dtxnPercentage;
    }
    public long getSinglePartitionTxnCount() {
        return (this.txnCount - this.dtxnCount);
    }
    public double getSinglePartitionTxnPercentage() {
        return (100 - this.dtxnPercentage);
    }
    public double getTxnPerMilli() {
        return this.txnPerMilli;
    }
    public double getTxnPerSecond() {
        return this.txnPerSecond;
    }
    
    public double getTotalAvgLatency() {
        return this.totalAvgLatency;
    }
    public double getTotalStdevLatency() {
        return this.totalStdevLatency;
    }
    public double getTotalMinLatency() {
        return this.totalMinLatency;
    }
    public double getTotalMaxLatency() {
        return this.totalMaxLatency;
    }
    
    public double getSinglePartitionAvgLatency() {
        return this.totalAvgLatency;
    }
    public double getSinglePartitionStdevLatency() {
        return this.totalStdevLatency;
    }
    public double getSinglePartitionMinLatency() {
        return this.totalMinLatency;
    }
    public double getSinglePartitionMaxLatency() {
        return this.totalMaxLatency;
    }
    
    public double getDistributedAvgLatency() {
        return this.totalAvgLatency;
    }
    public double getDistributedStdevLatency() {
        return this.totalStdevLatency;
    }
    public double getDistributedMinLatency() {
        return this.totalMinLatency;
    }
    public double getDistributedMaxLatency() {
        return this.totalMaxLatency;
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