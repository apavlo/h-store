package edu.brown.api.results;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class BenchmarkComponentResults implements JSONSerializable {

    public FastIntHistogram transactions;
    
    private boolean enableLatencies = false;
    public Map<Integer, List<Integer>> latencies;
    
    private boolean enableBasePartitions = false;
    public Histogram<Integer> basePartitions = new Histogram<Integer>(true);
    
    private boolean enableResponseStatuses = false;
    public Histogram<String> responseStatuses = new Histogram<String>(true);

    public BenchmarkComponentResults() {
        // Needed for deserialization
    }
    
    public BenchmarkComponentResults(int numTxns) {
        this.transactions = new FastIntHistogram(numTxns);
        this.transactions.setKeepZeroEntries(true);
    }
    
    public BenchmarkComponentResults copy() {
        BenchmarkComponentResults copy = null;
        if (this.transactions != null) {
            copy = new BenchmarkComponentResults(this.transactions.fastSize());
            copy.transactions.putHistogram(this.transactions);
        } else {
            copy = new BenchmarkComponentResults();
        }
        copy.enableLatencies = this.enableLatencies;
        copy.latencies = new HashMap<Integer, List<Integer>>();
        for (Entry<Integer, List<Integer>> e : this.latencies.entrySet()) {
            copy.latencies.put(e.getKey(), new ArrayList<Integer>(e.getValue()));
        } // FOR
        
        copy.enableBasePartitions = this.enableBasePartitions;
        copy.basePartitions.putHistogram(this.basePartitions);
        copy.enableResponseStatuses = this.enableResponseStatuses;
        return (copy);
    }
    
    public boolean isLatenciesEnabled() {
        return (this.enableLatencies);
    }
    public void setEnableLatencies(boolean val) {
        if (val && this.latencies == null) {
            this.latencies = new HashMap<Integer, List<Integer>>();
        }
        this.enableLatencies = val;
    }
    
    public boolean isBasePartitionsEnabled() {
        return (this.enableBasePartitions);
    }
    public void setEnableBasePartitions(boolean val) {
        this.enableBasePartitions = val;
    }

    public boolean isResponsesStatusesEnabled() {
        return (this.enableResponseStatuses);
    }
    public void setEnableResponsesStatuses(boolean val) {
        this.enableResponseStatuses = val;
    }
    
    public void clear() {
        if (this.transactions != null) {
            this.transactions.clearValues();
        }
        if (this.enableBasePartitions) {
            this.basePartitions.clearValues();
        }
        if (this.enableResponseStatuses) {
            this.responseStatuses.clearValues();
        }
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
        String exclude[] = {
            (this.enableLatencies == false ? "latencies" : ""),
            (this.enableBasePartitions == false ? "basePartitions" : ""),
            (this.enableResponseStatuses == false ? "responseStatuses" : ""),
        };
        Field fields[] = JSONUtil.getSerializableFields(this.getClass(), exclude);
        JSONUtil.fieldsToJSON(stringer, this, BenchmarkComponentResults.class, fields);
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, BenchmarkComponentResults.class, true,
                JSONUtil.getSerializableFields(this.getClass()));
    }
} // END CLASS