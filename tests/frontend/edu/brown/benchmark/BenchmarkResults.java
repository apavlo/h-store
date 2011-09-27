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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class BenchmarkResults {

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
    }

    public static class FinalResult implements JSONSerializable {
        public long duration;
        public long totalTxnCount;
        public double txnPerSecond;
        public final Map<String, EntityResult> txnResults = new HashMap<String, EntityResult>();
        public final Map<String, EntityResult> clientResults = new HashMap<String, EntityResult>();
        
        public FinalResult(BenchmarkResults results) {
            
            this.duration = results.getTotalDuration();
            this.totalTxnCount = 0;
            for (String client : results.getClientNames()) {
                for (String txn : results.getTransactionNames()) {
                    Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                    for (Result r : rs)
                        this.totalTxnCount += r.transactionCount;
                } // FOR
            } // FOR
            this.txnPerSecond = totalTxnCount / (double)duration * 1000.0;
            
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
        public double getTxnPerSecond() {
            return this.txnPerSecond;
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
    
    private final HashMap<String, HashMap<String, ArrayList<Result>>> m_data =
        new HashMap<String, HashMap<String, ArrayList<Result>>>();
    private final Set<Error> m_errors = new HashSet<Error>();

    private final long m_durationInMillis;
    private final long m_pollIntervalInMillis;
    private final int m_clientCount;

    // cached data for performance and consistency
    private final Set<String> m_transactionNames = new HashSet<String>();

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

    public int getCompletedIntervalCount() {
        // make sure all
        if (m_data.size() < m_clientCount)
            return 0;
        assert(m_data.size() == m_clientCount);

        int min = Integer.MAX_VALUE;
        String txnName = m_transactionNames.iterator().next();
        for (HashMap<String, ArrayList<Result>> txnResults : m_data.values()) {
            ArrayList<Result> results = txnResults.get(txnName);
            if (results.size() < min)
                min = results.size();
        }

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

    public Result[] getResultsForClientAndTransaction(String clientName, String transactionName) {
        HashMap<String, ArrayList<Result>> txnResults = m_data.get(clientName);
        ArrayList<Result> results = txnResults.get(transactionName);
        int intervals = getCompletedIntervalCount();
        Result[] retval = new Result[intervals];

        long txnsTillNow = 0;
        for (int i = 0; i < intervals; i++) {
            Result r = results.get(i);
            retval[i] = new Result(r.benchmarkTimeDelta, r.transactionCount - txnsTillNow);
            txnsTillNow = r.transactionCount;
        }
        return retval;
    }

    void setPollResponseInfo(String clientName, int pollIndex, long time, Map<String, Long> transactionCounts, String errMsg) {
        long benchmarkTime = pollIndex * m_pollIntervalInMillis;
        long offsetTime = time - benchmarkTime;

        if (errMsg != null) {
            Error err = new Error(clientName, errMsg, pollIndex);
            m_errors.add(err);
        }
        else {
            // put the transactions names:
            for (String txnName : transactionCounts.keySet())
                m_transactionNames.add(txnName);

            // ensure there is an entry for the client
            HashMap<String, ArrayList<Result>> txnResults = m_data.get(clientName);
            if (txnResults == null) {
                txnResults = new HashMap<String, ArrayList<Result>>();
                for (String txnName : transactionCounts.keySet())
                    txnResults.put(txnName, new ArrayList<Result>());
                m_data.put(clientName, txnResults);
            }

            for (Entry<String, Long> entry : transactionCounts.entrySet()) {
                Result r = new Result(offsetTime, entry.getValue());
                ArrayList<Result> results = m_data.get(clientName).get(entry.getKey());
                assert(results != null);
                assert(results.size() == pollIndex) : String.format("[%d] %s => %s", pollIndex, entry, results);
                results.add(r);
            }
        }
    }

    BenchmarkResults copy() {
        BenchmarkResults retval = new BenchmarkResults(m_pollIntervalInMillis, m_durationInMillis, m_clientCount);

        retval.m_errors.addAll(m_errors);
        retval.m_transactionNames.addAll(m_transactionNames);

        for (Entry<String, HashMap<String, ArrayList<Result>>> entry : m_data.entrySet()) {
            HashMap<String, ArrayList<Result>> txnsForClient =
                new HashMap<String, ArrayList<Result>>();

            for (Entry <String, ArrayList<Result>> entry2 : entry.getValue().entrySet()) {
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
        // TODO Auto-generated method stub
        return super.toString();
    }
}
