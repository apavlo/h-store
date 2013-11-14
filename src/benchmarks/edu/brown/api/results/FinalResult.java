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

import edu.brown.api.BenchmarkControllerUtil;
import edu.brown.api.results.BenchmarkResults.Result;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.MathUtil;

public class FinalResult implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(FinalResult.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public long duration;
    public long txnTotalCount;
    public long specexecTotalCount;
    public long dtxnTotalCount;
    public double txnTotalPerSecond;
    public long txnMinCount;
    public double txnMinPerSecond;
    public long txnMaxCount;
    public double txnMaxPerSecond;
    public double stddevTxnPerSecond;

    public double totalAvgLatency;
    public double totalStdevLatency;
    public double totalMinLatency;
    public double totalMaxLatency;
    
    public double spAvgLatency;
    public double spStdevLatency;
    public double spMinLatency;
    public double spMaxLatency;
    
    public double dtxnAvgLatency;
    public double dtxnStdevLatency;
    public double dtxnMinLatency;
    public double dtxnMaxLatency;

    // added by hawk
    // for #workflow
    public long   wkfTotalCount;
    public long   wkfMinCount;
    public long   wkfMaxCount;

    public double wkfPerSecond;
    public double wkfMinPerSecond;
    public double wkfMaxPerSecond;
    public double stddevWkfPerSecond;

    public double wkfAvgLatency;
    public double wkfStdevLatency;
    public double wkfMinLatency;
    public double wkfMaxLatency;
    // ended by hawk
    
    /** TransactionName -> Results */
    public final Map<String, EntityResult> txnResults = new HashMap<String, EntityResult>();
    /** ClientName -> Results */
    public final Map<String, EntityResult> clientResults = new HashMap<String, EntityResult>();
    
    //added by hawk
    public final Map<String, EntityResult> wkfResults = new HashMap<String, EntityResult>();
    
    public FinalResult(BenchmarkResults results) {
        
        // Final Transactions Per Second
        this.duration = results.getTotalDuration();
        
        this.txnTotalCount = 0;
        this.txnMinCount = Long.MAX_VALUE;
        this.txnMaxCount = 0;
        this.dtxnTotalCount = 0;
        // added by hawk
        this.wkfTotalCount = 0;
        this.wkfMinCount = Long.MAX_VALUE;
        this.wkfMaxCount = 0;
        // ended by hawk
        
        Histogram<String> clientTxnCounts = new ObjectHistogram<String>(true);
        Histogram<String> clientWkfCounts = new ObjectHistogram<String>(true);//added by hawk
        Histogram<String> clientSpecExecCounts = new ObjectHistogram<String>(true);
        Histogram<String> clientDtxnCounts = new ObjectHistogram<String>(true);
        Histogram<String> txnCounts = new ObjectHistogram<String>(true);
        Histogram<String> specexecCounts = new ObjectHistogram<String>(true);
        Histogram<String> dtxnCounts = new ObjectHistogram<String>(true);
        Histogram<String> wkfCounts = new ObjectHistogram<String>(true);//added by hawk
        
        double intervalTotals[] = results.computeIntervalTotals();
        if (debug.val) LOG.debug("INTERVAL TOTALS: " + Arrays.toString(intervalTotals));
        for (int i = 0; i < intervalTotals.length; i++) {
            intervalTotals[i] /= (results.pollIntervalInMillis / 1000.0);
        } // FOR
        if (debug.val) LOG.debug("INTERVAL TPS: " + Arrays.toString(intervalTotals));
        this.stddevTxnPerSecond = MathUtil.stdev(intervalTotals);
        
        //added by hawk
        double workflowIntervalTotals[] = results.computeWorkflowIntervalTotals();
        if (debug.val) LOG.debug("INTERVAL WORKFLOW TOTALS: " + Arrays.toString(workflowIntervalTotals));
        for (int i = 0; i < workflowIntervalTotals.length; i++) {
            workflowIntervalTotals[i] /= (results.pollIntervalInMillis / 1000.0);
        } // FOR
        if (debug.val) LOG.debug("INTERVAL TPS: " + Arrays.toString(workflowIntervalTotals));
        this.stddevWkfPerSecond = MathUtil.stdev(workflowIntervalTotals);
        //ended by hawk
        
        for (String clientName : results.getClientNames()) {
            clientTxnCounts.set(clientName, 0);
            for (String txnName : results.getTransactionNames()) {
                if (txnCounts.contains(txnName) == false) txnCounts.set(txnName, 0);
                Result[] rs = results.getResultsForClientAndTransaction(clientName, txnName);
                for (Result r : rs) {
                    this.txnTotalCount += r.transactionCount;
                    clientTxnCounts.put(clientName, r.transactionCount);
                    txnCounts.put(txnName, r.transactionCount);
                    
                    this.specexecTotalCount += r.specexecCount;
                    clientSpecExecCounts.put(clientName, r.specexecCount);
                    specexecCounts.put(txnName, r.specexecCount);
                    
                    this.dtxnTotalCount += r.dtxnCount;
                    clientDtxnCounts.put(clientName, r.dtxnCount);
                    dtxnCounts.put(txnName, r.dtxnCount);
                } // FOR
            } // FOR
            //added by hawk
            clientWkfCounts.set(clientName, 0);
            for (String wkfName : results.getWorkflowNames()) {
                if (wkfCounts.contains(wkfName) == false) wkfCounts.set(wkfName, 0);
                Result[] rs = results.getResultsForClientAndWorkflow(clientName, wkfName);
                for (Result r : rs) {
                    this.wkfTotalCount += r.workflowCount;
                    clientWkfCounts.put(clientName, r.workflowCount);
                    wkfCounts.put(wkfName, r.transactionCount);
                } // FOR
            } // FOR
            //ended by hawk
        } // FOR
        this.txnTotalPerSecond = this.txnTotalCount / (double)this.duration * 1000.0;
        this.wkfPerSecond = this.wkfTotalCount / (double)this.duration * 1000.0;//added by hawk
        
        // Min/Max Transactions Per Second
        for (int i = 0; i < results.getCompletedIntervalCount(); i++) {
            long txnCount = 0;
            long wkfCount = 0;//added by hawk
            for (String client : results.getClientNames()) {
                for (String txn : results.getTransactionNames()) {
                    Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                    if (i < rs.length) 
                    {
                        txnCount += rs[i].transactionCount;
                    }
                } // FOR (txn)
                for (String wkf : results.getWorkflowNames()) {
                    Result[] rs = results.getResultsForClientAndWorkflow(client, wkf);
                    if (i < rs.length) 
                    {
                        wkfCount += rs[i].workflowCount;
                    }
                } // FOR (wkf)

            } // FOR (client)
            if (debug.val)
                LOG.debug(String.format("[%02d] minTxnCount = %d <-> %d", i, this.txnMinCount, txnCount));
            this.txnMinCount = Math.min(this.txnMinCount, txnCount);
            this.txnMaxCount = Math.max(this.txnMaxCount, txnCount);
            //added by hawk
            this.wkfMinCount = Math.min(this.wkfMinCount, wkfCount);
            this.wkfMaxCount = Math.max(this.wkfMaxCount, wkfCount);
            //ended by hawk
        } // FOR
        double interval = results.getIntervalDuration() / 1000.0d;
        this.txnMinPerSecond = this.txnMinCount / interval;
        this.txnMaxPerSecond = this.txnMaxCount / interval;
        //added by hawk
        this.wkfMinPerSecond = this.wkfMinCount / interval;
        this.wkfMaxPerSecond = this.wkfMaxCount / interval;
        //ended by hawk
        // TRANSACTION RESULTS
        Histogram<Integer> totalLatencies = new ObjectHistogram<Integer>();
        Histogram<Integer> spLatencies = new ObjectHistogram<Integer>();
        Histogram<Integer> dtxnLatencies = new ObjectHistogram<Integer>();
        Histogram<Integer> wkfLatencies = new ObjectHistogram<Integer>();//added by hawk
        for (String txnName : txnCounts.values()) {
            Histogram<Integer> allTxnLatencies = results.getTransactionTotalLatencies(txnName);
            Histogram<Integer> spTxnLatencies = results.getTransactionSinglePartitionLatencies(txnName);
            Histogram<Integer> dtxnTxnLatencies = results.getTransactionDistributedLatencies(txnName);
            Histogram<Integer> WkfLatencies = new ObjectHistogram<Integer>();//added by hawk
            assert(txnCounts!=null);
            assert(dtxnCounts!=null);
            assert(this!=null);
            assert(allTxnLatencies!=null);
            assert(spTxnLatencies!=null);
            assert(dtxnTxnLatencies!=null);
            EntityResult er = new EntityResult(this.txnTotalCount, this.duration,
                                               txnCounts.get(txnName), dtxnCounts.get(txnName),0l, WkfLatencies,//modified by hawk
                                               allTxnLatencies, spTxnLatencies, dtxnTxnLatencies);
            this.txnResults.put(txnName, er);
            totalLatencies.put(allTxnLatencies);
            spLatencies.put(spTxnLatencies);
            dtxnLatencies.put(dtxnTxnLatencies);
        } // FOR
        for (String wkfName : wkfCounts.values()) {
            Histogram<Integer> WkfLatencies = results.getWorkflowLatencies(wkfName);
            EntityResult er = new EntityResult(0l, this.duration,
                                               0l, 0l, wkfCounts.get(wkfName), WkfLatencies,//modified by hawk
                                               null, null, null);
            this.wkfResults.put(wkfName, er);
            wkfLatencies.put(WkfLatencies);
        } // FOR
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
        //added by hawk
        if (wkfLatencies.isEmpty() == false) {
            double x[] = BenchmarkControllerUtil.computeLatencies(wkfLatencies);
            int i = 0;
            this.wkfMinLatency = x[i++];
            this.wkfMaxLatency = x[i++];
            this.wkfAvgLatency = x[i++];
            this.wkfStdevLatency = x[i++];
        }
        //ended by hawk
        
        // CLIENTS RESULTS
        for (String clientName : results.getClientNames()) {
            totalLatencies = results.getClientTotalLatencies(clientName);
            spLatencies = results.getClientSinglePartitionLatencies(clientName);
            dtxnLatencies = results.getClientDistributedLatencies(clientName);
            wkfLatencies = results.getTotalWorkflowLatencies(clientName);//added by hawk
            EntityResult er = new EntityResult(this.txnTotalCount, this.duration,
                                               clientTxnCounts.get(clientName), clientDtxnCounts.get(clientName),wkfCounts.get("0"), wkfLatencies,//modified by hawk
                                               totalLatencies, spLatencies, dtxnLatencies);
            this.clientResults.put(clientName.replace("client-", ""), er);
        } // FOR
        
        
    }
    
    public long getDuration() {
        return this.duration;
    }
    public long getTotalTxnCount() {
        return this.txnTotalCount;
    }
    public long getTotalSpecExecCount() {
        return this.specexecTotalCount;
    }
    public long getTotalDtxnCount() {
        return this.dtxnTotalCount;
    }
    public double getTotalTxnPerSecond() {
        return this.txnTotalPerSecond;
    }
    public long getMinTxnCount() {
        return this.txnMinCount;
    }
    public double getMinTxnPerSecond() {
        return this.txnMinPerSecond;
    }
    public long getMaxTxnCount() {
        return this.txnMaxCount;
    }
    public double getMaxTxnPerSecond() {
        return this.txnMaxPerSecond;
    }
    public double getStandardDeviationTxnPerSecond() {
        return this.stddevTxnPerSecond;
    }
    public Collection<String> getTransactionNames() {
        return this.txnResults.keySet();
    }
    
    public double getTotalAvgLatency() {
        return this.totalAvgLatency;
    }
    public double getTotalStdDevLatency() {
        return this.totalStdevLatency;
    }
    public double getTotalMinLatency() {
        return this.totalMinLatency;
    }
    public double getTotalMaxLatency() {
        return this.totalMaxLatency;
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
    
    //added by hawk
    public long getWkfCount() {
        return this.wkfTotalCount;
    }
    public long getMinWkfCount() {
        return this.wkfMinCount;
    }
    public long getMaxWkfCount() {
        return this.wkfMaxCount;
    }
    public double getWkfPerSecond() {
        return this.wkfPerSecond;
    }
    public double getMinWkfPerSecond() {
        return this.wkfMinPerSecond;
    }
    public double getMaxWkfPerSecond() {
        return this.wkfMaxPerSecond;
    }
    public double getStandardDeviationWkfPerSecond() {
        return this.stddevWkfPerSecond;
    }
    public double getWkfAvgLatency() {
        return this.wkfAvgLatency;
    }
    public double getWkfStdDevLatency() {
        return this.wkfStdevLatency;
    }
    public double getWkfMinLatency() {
        return this.wkfMinLatency;
    }
    public double getWkfMaxLatency() {
        return this.wkfMaxLatency;
    }
    //ended by hawk
    
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