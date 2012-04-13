package edu.brown.benchmark;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class TransactionCounter implements JSONSerializable {
    
    public Histogram<Integer> basePartitions = new Histogram<Integer>(true);
    public Histogram<String> transactions = new Histogram<String>(true);

    public TransactionCounter copy() {
        TransactionCounter copy = new TransactionCounter();
        copy.basePartitions.putHistogram(this.basePartitions);
        copy.transactions.putHistogram(this.transactions);
        return (copy);
    }
    
    public void clear() {
        this.basePartitions.clearValues();
        this.transactions.clearValues();
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
        JSONUtil.fieldsToJSON(stringer, this, TransactionCounter.class, JSONUtil.getSerializableFields(this.getClass()));
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, TransactionCounter.class, true, JSONUtil.getSerializableFields(this.getClass()));
    }
} // END CLASS