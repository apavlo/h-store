/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package edu.brown.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public class BenchmarkResults {
    private static final Logger LOG = Logger.getLogger(BenchmarkResults.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static class Error {
        public Error(String clientName, String message, int pollIndex) {
            this.clientName = clientName;
            this.message = message;
            this.pollIndex = pollIndex;
        }
        public final String clientName;
        public final String message;
        public final int pollIndex;
    }

    public static class Result {
        public Result(long benchmarkTimeDelta, long transactionCount) {
            this.benchmarkTimeDelta = benchmarkTimeDelta;
            this.transactionCount = transactionCount;
        }
        public final long benchmarkTimeDelta;
        public final long transactionCount;
        
        @Override
        public String toString() {
            return String.format("<TxnCount:%d, Delta:%d>", this.transactionCount, this.benchmarkTimeDelta);
        }
    }

    public static class FinalResult implements JSONSerializable {
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
        
        public FinalResult(BenchmarkResults results) {
            
            // Final Transactions Per Second
            this.duration = results.getTotalDuration();
            this.totalTxnCount = 0;
            int num_polls = 0;
            for (String client : results.getClientNames()) {
                for (String txn : results.getTransactionNames()) {
                    Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                    for (Result r : rs)
                        this.totalTxnCount += r.transactionCount;
                    num_polls = Math.max(num_polls, rs.length);
                } // FOR
            } // FOR
            this.totalTxnPerSecond = totalTxnCount / (double)duration * 1000.0;
            
            // Min/Max/StdDev Transactions Per Second
            if (debug.get()) LOG.debug("Num Polls: " + num_polls);
            this.minTxnCount = Long.MAX_VALUE;
            this.maxTxnCount = 0;
            for (int i = 0; i < num_polls; i++) {
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
            
            // TRANSACTIONS
            for (String transactionName : results.getTransactionNames()) {
                long txnCount = 0;
                for (String clientName : results.getClientNames()) {
                    Result[] rs = results.getResultsForClientAndTransaction(clientName, transactionName);
                    for (Result r : rs)
                        txnCount += r.transactionCount;
                }
                EntityResult er = new EntityResult(this.totalTxnCount, this.duration, txnCount);
                this.txnResults.put(transactionName, er);
            }
            
            // CLIENTS
            for (String clientName : results.getClientNames()) {
                long txnCount = 0;
                for (String txnName : results.getTransactionNames()) {
                    Result[] rs = results.getResultsForClientAndTransaction(clientName, txnName);
                    for (Result r : rs)
                        txnCount += r.transactionCount;
                }
                clientName = clientName.replace("client-", "");
                EntityResult er = new EntityResult(this.totalTxnCount, this.duration, txnCount);
                this.clientResults.put(clientName, er);
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
            JSONUtil.fieldsToJSON(stringer, this, FinalResult.class, JSONUtil.getSerializableFields(this.getClass()));
        }
        @Override
        public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
            JSONUtil.fieldsFromJSON(json_object, catalog_db, this, FinalResult.class, true, JSONUtil.getSerializableFields(this.getClass()));
        }
    }
    
    public static class EntityResult implements JSONSerializable {
        public long txnCount;
        public double txnPercentage;
        public double txnPerMilli;
        public double txnPerSecond;
        
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
            JSONUtil.fieldsToJSON(stringer, this, EntityResult.class, JSONUtil.getSerializableFields(this.getClass()));
        }
        @Override
        public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
            JSONUtil.fieldsFromJSON(json_object, catalog_db, this, EntityResult.class, true, JSONUtil.getSerializableFields(this.getClass()));
        }
    }
    
    /**
     * ClientName -> TxnName -> List<Result>
     */
    private final SortedMap<String, SortedMap<String, List<Result>>> m_data = new TreeMap<String, SortedMap<String, List<Result>>>();
    private final Set<Error> m_errors = new HashSet<Error>();

    private final long m_durationInMillis;
    private final long m_pollIntervalInMillis;
    private final int m_clientCount;
    private final Histogram<Integer> m_basePartitions = new Histogram<Integer>();
    
    // cached data for performance and consistency
    private final SortedSet<String> m_transactionNames = new TreeSet<String>();

    BenchmarkResults(long pollIntervalInMillis, long durationInMillis, int clientCount) {
        assert((durationInMillis % pollIntervalInMillis) == 0) : "duration does not comprise an integral number of polling intervals.";

        m_durationInMillis = durationInMillis;
        m_pollIntervalInMillis = pollIntervalInMillis;
        m_clientCount = clientCount;
    }

    public Set<Error> getAnyErrors() {
        if (m_errors.size() == 0)
            return null;
        Set<Error> retval = new TreeSet<Error>();
        for (Error e : m_errors)
            retval.add(e);
        return retval;
    }
    
    public FinalResult getFinalResult() {
        return new FinalResult(this);
    }

    /**
     * Returns the number of interval polls that have complete information
     * from all of the clients.
     * @return
     */
    public int getCompletedIntervalCount() {
        // make sure all
        if (m_data.size() < m_clientCount)
            return 0;
        assert(m_data.size() == m_clientCount) : 
            String.format("%d != %d", m_data.size(), m_clientCount);

        int min = Integer.MAX_VALUE;
        // String txnName = m_transactionNames.iterator().next();
        for (Map<String, List<Result>> txnResults : m_data.values()) {
            for (String txnName : txnResults.keySet()) {
                List<Result> results = txnResults.get(txnName);
                if (results != null) min = Math.min(results.size(), min);
            } // FOR
        } // FOR

        return min;
    }

    public long getIntervalDuration() {
        return m_pollIntervalInMillis;
    }

    public long getTotalDuration() {
        return m_durationInMillis;
    }


    public Set<String> getTransactionNames() {
        Set<String> retval = new TreeSet<String>();
        retval.addAll(m_transactionNames);
        return retval;
    }

    public Set<String> getClientNames() {
        Set<String> retval = new TreeSet<String>();
        retval.addAll(m_data.keySet());
        return retval;
    }
    
    public Histogram<Integer> getBasePartitions() {
        return (m_basePartitions);
    }

    public Result[] getResultsForClientAndTransaction(String clientName, String transactionName) {
        int intervals = getCompletedIntervalCount();
        
        Map<String, List<Result>> txnResults = m_data.get(clientName);
        List<Result> results = txnResults.get(transactionName);
        assert(results != null) :
            String.format("Null results for txn '%s' from client '%s'\n%s",
                          transactionName, clientName, StringUtil.formatMaps(txnResults));
        
        long txnsTillNow = 0;
        Result[] retval = new Result[intervals];
        for (int i = 0; i < intervals; i++) {
            Result r = results.get(i);
            retval[i] = new Result(r.benchmarkTimeDelta, r.transactionCount - txnsTillNow);
            txnsTillNow = r.transactionCount;
        }
//        assert(intervals == results.size());
        return retval;
    }

    void setPollResponseInfo(String clientName, int pollIndex, long time, Map<String, Long> transactionCounts, String errMsg) {
        long benchmarkTime = pollIndex * m_pollIntervalInMillis;
        long offsetTime = time - benchmarkTime;

        if (debug.get())
            LOG.debug(String.format("Setting Poll Response Info for '%s' [%d]:\n%s",
                                    clientName, pollIndex, StringUtil.formatMaps(transactionCounts)));
        
        if (errMsg != null) {
            Error err = new Error(clientName, errMsg, pollIndex);
            m_errors.add(err);
        }
        else {
            // put the transactions names:
            for (String txnName : transactionCounts.keySet())
                m_transactionNames.add(txnName);

            // ensure there is an entry for the client
            SortedMap<String, List<Result>> txnResults = m_data.get(clientName);
            if (txnResults == null) {
                txnResults = new TreeMap<String, List<Result>>();
                for (String txnName : transactionCounts.keySet())
                    txnResults.put(txnName, new ArrayList<Result>());
                m_data.put(clientName, txnResults);
            }

            for (Entry<String, Long> entry : transactionCounts.entrySet()) {
                Result r = new Result(offsetTime, entry.getValue());
                List<Result> results = m_data.get(clientName).get(entry.getKey());
                assert(results != null);
                assert(results.size() == pollIndex) :
                    String.format("%s != %d\n%s => %s", results.size(), pollIndex, entry, results);
                results.add(r);
            }
        }
    }

    BenchmarkResults copy() {
        BenchmarkResults retval = new BenchmarkResults(m_pollIntervalInMillis, m_durationInMillis, m_clientCount);

        retval.m_basePartitions.putHistogram(m_basePartitions);
        retval.m_errors.addAll(m_errors);
        retval.m_transactionNames.addAll(m_transactionNames);

        for (Entry<String, SortedMap<String, List<Result>>> entry : m_data.entrySet()) {
            SortedMap<String, List<Result>> txnsForClient = new TreeMap<String, List<Result>>();

            for (Entry<String, List<Result>> entry2 : entry.getValue().entrySet()) {
                ArrayList<Result> newResults = new ArrayList<Result>();
                for (Result r : entry2.getValue())
                    newResults.add(r);
                txnsForClient.put(entry2.getKey(), newResults);
            }

            retval.m_data.put(entry.getKey(), txnsForClient);
        }

        return retval;
    }

    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Transaction Names", StringUtil.join("\n", m_transactionNames));
        m.put("Transaction Data", m_data);
        m.put("Base Partitions", m_basePartitions);
        
        return "BenchmarkResults\n" + StringUtil.formatMaps(m);
    }
}
