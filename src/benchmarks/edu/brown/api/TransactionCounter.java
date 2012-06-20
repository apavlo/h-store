package edu.brown.api;

import java.io.IOException;
import java.lang.reflect.Field;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class TransactionCounter implements JSONSerializable {

    public FastIntHistogram transactions;
    
    private boolean enableBasePartitions = false;
    public Histogram<Integer> basePartitions = new Histogram<Integer>(true);
    
    private boolean enableResponseStatuses = false;
    public Histogram<String> responseStatuses = new Histogram<String>(true);

    public TransactionCounter() {
        // Needed for deserialization
    }
    
    public TransactionCounter(int numTxns) {
        this.transactions = new FastIntHistogram(numTxns);
        this.transactions.setKeepZeroEntries(true);
    }
    
    public TransactionCounter copy() {
        TransactionCounter copy = null;
        if (this.transactions != null) {
            copy = new TransactionCounter(this.transactions.fastSize());
            copy.transactions.putHistogram(this.transactions);
        } else {
            copy = new TransactionCounter();
        }
        copy.enableBasePartitions = this.enableBasePartitions;
        copy.basePartitions.putHistogram(this.basePartitions);
        copy.enableResponseStatuses = this.enableResponseStatuses;
        return (copy);
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
        String exclude[] = {
            (this.enableBasePartitions == false ? "basePartitions" : ""),
            (this.enableResponseStatuses == false ? "responseStatuses" : ""),
            
        };
        Field fields[] = JSONUtil.getSerializableFields(this.getClass(), exclude);
        JSONUtil.fieldsToJSON(stringer, this, TransactionCounter.class, fields);
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, TransactionCounter.class, true, JSONUtil.getSerializableFields(this.getClass()));
    }
} // END CLASS