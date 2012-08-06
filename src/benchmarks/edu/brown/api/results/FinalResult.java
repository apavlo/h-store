package edu.brown.api.results;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.api.results.BenchmarkResults.Result;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.MathUtil;

public class FinalResult implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(FinalResult.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public long duration;
    public long totalTxnCount;
    public double totalTxnPerSecond;
    public long minTxnCount;
    public double minTxnPerSecond;
    public long maxTxnCount;
    public double maxTxnPerSecond;
    public double stddevTxnPerSecond;
    public final Map<String, EntityResult> txnResults = new HashMap<String, EntityResult>();
    public final Map<String, EntityResult> clientResults = new HashMap<String, EntityResult>();

    public final Map<String, Double> txnAvgLatency = new HashMap<String, Double>();
    public final Map<String, Double> txnStdDevLatency = new HashMap<String, Double>();
    public final Map<String, Double> txnMinLatency = new HashMap<String, Double>();
    public final Map<String, Double> txnMaxLatency = new HashMap<String, Double>();
    
    public FinalResult(BenchmarkResults results) {
        
        // Final Transactions Per Second
        this.duration = results.getTotalDuration();
        this.totalTxnCount = 0;
        this.minTxnCount = Long.MAX_VALUE;
        this.maxTxnCount = 0;
        
        Histogram<String> clientCounts = new Histogram<String>(true);
        Histogram<String> txnCounts = new Histogram<String>(true);
        
        double intervalTotals[] = results.computeIntervalTotals();
        if (debug.get()) LOG.debug("INTERVAL TOTALS: " + Arrays.toString(intervalTotals));
        for (int i = 0; i < intervalTotals.length; i++) {
            intervalTotals[i] /= (results.m_pollIntervalInMillis / 1000.0);
        } // FOR
        if (debug.get()) LOG.debug("INTERVAL TPS: " + Arrays.toString(intervalTotals));
        this.stddevTxnPerSecond = MathUtil.stdev(intervalTotals);
        
        for (String client : results.getClientNames()) {
            clientCounts.set(client, 0);
            for (String txn : results.getTransactionNames()) {
                if (txnCounts.contains(txn) == false) txnCounts.set(txn, 0);
                Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                for (Result r : rs) {
                    this.totalTxnCount += r.transactionCount;
                    clientCounts.put(client, r.transactionCount);
                    txnCounts.put(txn, r.transactionCount);
                } // FOR
            } // FOR
        } // FOR
        this.totalTxnPerSecond = totalTxnCount / (double)duration * 1000.0;
        
        // Min/Max Transactions Per Second
        for (int i = 0; i < results.getCompletedIntervalCount(); i++) {
            long txnCount = 0;
            for (String client : results.getClientNames()) {
                for (String txn : results.getTransactionNames()) {
                    Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                    if (i < rs.length) txnCount += rs[i].transactionCount;
                } // FOR (txn)
            } // FOR (client)
            if (debug.get())
                LOG.debug(String.format("[%02d] minTxnCount = %d <-> %d", i, minTxnCount, txnCount));
            this.minTxnCount = Math.min(this.minTxnCount, txnCount);
            this.maxTxnCount = Math.max(this.maxTxnCount, txnCount);
        } // FOR
        double interval = results.getIntervalDuration() / 1000.0d;
        this.minTxnPerSecond = this.minTxnCount / interval;
        this.maxTxnPerSecond = this.maxTxnCount / interval;
        
        // LATENCIES
        
        // TRANSACTIONS
        for (String transactionName : txnCounts.values()) {
            EntityResult er = new EntityResult(this.totalTxnCount, this.duration, txnCounts.get(transactionName));
            this.txnResults.put(transactionName, er);
        }
        // CLIENTS
        for (String clientName : results.getClientNames()) {
            EntityResult er = new EntityResult(this.totalTxnCount, this.duration, clientCounts.get(clientName));
            this.clientResults.put(clientName.replace("client-", ""), er);
        } // FOR
    }
    
    public long getDuration() {
        return this.duration;
    }
    public long getTotalTxnCount() {
        return this.totalTxnCount;
    }
    public double getTotalTxnPerSecond() {
        return this.totalTxnPerSecond;
    }
    public long getMinTxnCount() {
        return this.minTxnCount;
    }
    public double getMinTxnPerSecond() {
        return this.minTxnPerSecond;
    }
    public long getMaxTxnCount() {
        return this.maxTxnCount;
    }
    public double getMaxTxnPerSecond() {
        return this.maxTxnPerSecond;
    }
    public double getStandardDeviationTxnPerSecond() {
        return this.stddevTxnPerSecond;
    }
    public Collection<String> getTransactionNames() {
        return this.txnResults.keySet();
    }
    public EntityResult getTransactionResult(String txnName) {
        return this.txnResults.get(txnName);
    }
    public Collection<String> getClientNames() {
        return this.clientResults.keySet();
    }
    public EntityResult getClientResult(String clientName) {
        return this.clientResults.get(clientName);
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
        JSONUtil.fieldsToJSON(stringer, this, FinalResult.class, JSONUtil.getSerializableFields(this.getClass()));
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, FinalResult.class, true, JSONUtil.getSerializableFields(this.getClass()));
    }
}