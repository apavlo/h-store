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
    public FastIntHistogram transactions = new FastIntHistogram(true);
    
    /**
     * The number of speculatively executed txns
     */
    public FastIntHistogram specexecs = new FastIntHistogram(true);
    
    /**
     * The number of distributed txns executed based on ProcedureId
     */
    public FastIntHistogram dtxns = new FastIntHistogram(true);
    
    /**
     * Transaction Name Index -> Latencies
     */
    public final Map<Integer, ObjectHistogram<Integer>> spLatencies = new HashMap<Integer, ObjectHistogram<Integer>>();
    public final Map<Integer, ObjectHistogram<Integer>> dtxnLatencies = new HashMap<Integer, ObjectHistogram<Integer>>();
    
    public FastIntHistogram basePartitions = new FastIntHistogram(true);
    private boolean enableBasePartitions = false;
    
    public FastIntHistogram responseStatuses = new FastIntHistogram(true, Status.values().length);
    private boolean enableResponseStatuses = false;

    /**
     * Constructor
     */
    public BenchmarkComponentResults() {
        // Nothing to do...
    }
    
    public BenchmarkComponentResults copy() {
        final BenchmarkComponentResults copy = new BenchmarkComponentResults();

        assert(copy.transactions != null);
        copy.transactions.setDebugLabels(this.transactions.getDebugLabels());
        copy.transactions.put(this.transactions);
        
        assert(copy.specexecs != null);
        copy.specexecs.setDebugLabels(this.transactions.getDebugLabels());
        copy.specexecs.put(this.specexecs);
        
        assert(copy.dtxns != null);
        copy.dtxns.setDebugLabels(this.transactions.getDebugLabels());
        copy.dtxns.put(this.dtxns);
        
        copy.spLatencies.clear();
        synchronized (this.spLatencies) {
            for (Entry<Integer, ObjectHistogram<Integer>> e : this.spLatencies.entrySet()) {
                ObjectHistogram<Integer> h = new ObjectHistogram<Integer>();
                synchronized (e.getValue()) {
                    h.put(e.getValue());
                } // SYNCH
                copy.spLatencies.put(e.getKey(), h);
            } // FOR
        } // SYNCH
        
        copy.dtxnLatencies.clear();
        synchronized (this.dtxnLatencies) {
            for (Entry<Integer, ObjectHistogram<Integer>> e : this.dtxnLatencies.entrySet()) {
                ObjectHistogram<Integer> h = new ObjectHistogram<Integer>();
                synchronized (e.getValue()) {
                    h.put(e.getValue());
                } // SYNCH
                copy.dtxnLatencies.put(e.getKey(), h);
            } // FOR
        } // SYNCH
        
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
        this.spLatencies.clear();
        this.dtxnLatencies.clear();
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
        
        // HACK
        this.specexecs.setDebugLabels(null);
        this.dtxns.setDebugLabels(null);
        
        Field fields[] = JSONUtil.getSerializableFields(this.getClass(), exclude);
        JSONUtil.fieldsToJSON(stringer, this, BenchmarkComponentResults.class, fields);
        
        this.specexecs.setDebugLabels(this.transactions.getDebugLabels());
        this.dtxns.setDebugLabels(this.transactions.getDebugLabels());
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        this.spLatencies.clear();
        this.dtxnLatencies.clear();
        Field fields[] = JSONUtil.getSerializableFields(this.getClass());
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, BenchmarkComponentResults.class, true, fields);
        assert(this.transactions != null);
        assert(this.specexecs != null);
        assert(this.dtxns != null);
        
        // HACK: Copy the transaction's debug labels into these histograms
        if (this.specexecs.hasDebugLabels() == false) {
            this.specexecs.setDebugLabels(this.transactions.getDebugLabels());
        }
        if (this.dtxns.hasDebugLabels() == false) {
            this.dtxns.setDebugLabels(this.transactions.getDebugLabels());
        }
    }
} // END CLASS