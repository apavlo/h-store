package edu.brown.api.results;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * Raw results collected for each BenchmarkComponent instance
 * @author pavlo
 */
public class BenchmarkComponentResults implements JSONSerializable {

    /**
     * The number of txns executed base on ProcedureID
     */
    public FastIntHistogram transactions;
    
    /**
     * The number of speculatively executed txns
     */
    public FastIntHistogram specexecs;
    
    /**
     * The number of distributed txns executed based on ProcedureId
     */
    public FastIntHistogram dtxns;
    
    /**
     * Transaction Name Index -> Latencies
     */
    public final Map<Integer, Histogram<Integer>> latencies = new HashMap<Integer, Histogram<Integer>>();
    
    public FastIntHistogram basePartitions = new FastIntHistogram(true);
    private boolean enableBasePartitions = false;
    
    public FastIntHistogram responseStatuses = new FastIntHistogram(true, Status.values().length);
    private boolean enableResponseStatuses = false;

    public BenchmarkComponentResults() {
        // Needed for deserialization
    }
    
    public BenchmarkComponentResults(int numProcedures) {
        this.transactions = new FastIntHistogram(true, numProcedures);
        this.specexecs = new FastIntHistogram(true, numProcedures);
        this.dtxns = new FastIntHistogram(true, numProcedures);
    }
    
    public BenchmarkComponentResults copy() {
        final BenchmarkComponentResults copy = new BenchmarkComponentResults(this.transactions.size());

        assert(copy.transactions != null);
        copy.transactions.setDebugLabels(this.transactions.getDebugLabels());
        copy.transactions.put(this.transactions);
        
        assert(copy.specexecs != null);
        copy.specexecs.setDebugLabels(this.transactions.getDebugLabels());
        copy.specexecs.put(this.specexecs);
        
        assert(copy.dtxns != null);
        copy.dtxns.setDebugLabels(this.transactions.getDebugLabels());
        copy.dtxns.put(this.dtxns);
        
        copy.latencies.clear();
        for (Entry<Integer, Histogram<Integer>> e : this.latencies.entrySet()) {
            Histogram<Integer> h = new ObjectHistogram<Integer>();
            synchronized (e.getValue()) {
                h.put(e.getValue());
            } // SYNCH
            copy.latencies.put(e.getKey(), h);
        } // FOR
        
        copy.enableBasePartitions = this.enableBasePartitions;
        copy.basePartitions.put(this.basePartitions);
        
        copy.enableResponseStatuses = this.enableResponseStatuses;
        copy.responseStatuses.put(this.responseStatuses);
        
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
    
    public void clear(boolean includeTxns) {
        if (includeTxns && this.transactions != null) {
            this.transactions.clearValues();
            this.specexecs.clearValues();
            this.dtxns.clearValues();
        }
        this.latencies.clear();
        this.basePartitions.clearValues();
        this.responseStatuses.clearValues();
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
            (this.enableBasePartitions == false ? "basePartitions" : ""),
            (this.enableResponseStatuses == false ? "responseStatuses" : ""),
        };
        Field fields[] = JSONUtil.getSerializableFields(this.getClass(), exclude);
        JSONUtil.fieldsToJSON(stringer, this, BenchmarkComponentResults.class, fields);
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        this.latencies.clear();
        Field fields[] = JSONUtil.getSerializableFields(this.getClass());
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, BenchmarkComponentResults.class, true, fields);
        assert(this.transactions != null);
        assert(this.specexecs != null);
        assert(this.dtxns != null);
    }
} // END CLASS