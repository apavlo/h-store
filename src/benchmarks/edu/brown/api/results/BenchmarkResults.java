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

package edu.brown.api.results;

import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class BenchmarkResults {
    private static final Logger LOG = Logger.getLogger(BenchmarkResults.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
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
        public final long timestamp;
        public final long benchmarkTimeDelta;
        public final long transactionCount;
        public final long specexecCount;
        public final long dtxnCount;
        public final Histogram<Integer> spLatencies = new ObjectHistogram<Integer>();
        public final Histogram<Integer> dtxnLatencies = new ObjectHistogram<Integer>();
        
        public Result(long timestamp, long benchmarkTimeDelta, long transactionCount, long specexecCount, long dtxnCount) {
            this.timestamp = timestamp;
            this.benchmarkTimeDelta = benchmarkTimeDelta;
            this.transactionCount = transactionCount;
            this.specexecCount = specexecCount;
            this.dtxnCount = dtxnCount;
        }
        @Override
        public String toString() {
            return String.format("<TxnCount:%d / DtxnCount:%d / Delta:%d>",
                                 this.transactionCount, this.dtxnCount, this.benchmarkTimeDelta);
        }
    }

    /**
     * ClientName -> TxnName -> List<Result>
     */
    private final Map<String, Map<String, List<Result>>> data = new TreeMap<String, Map<String, List<Result>>>();
    private final Set<Error> errors = new HashSet<Error>();

    protected final long durationInMillis;
    protected final long pollIntervalInMillis;
    protected final int clientCount;
    protected long lastTimestamp;
    protected boolean enableNanoseconds = false;
    
    private boolean enableBasePartitions = false;
    private final Histogram<Integer> basePartitions = new ObjectHistogram<Integer>();
    
    private final Histogram<String> responseStatuses = new ObjectHistogram<String>();
    
    private int completedIntervals = 0;
    private final Histogram<String> clientResultCount = new ObjectHistogram<String>();
    
    // cached data for performance and consistency
    // TxnName -> FastIntHistogram Offset
    private final Map<String, Integer> transactionNames = new TreeMap<String, Integer>();
    
    private Pair<Long, Long> CACHE_computeTotalAndDelta = null;

    public BenchmarkResults(long pollIntervalInMillis, long durationInMillis, int clientCount) {
        assert((durationInMillis % pollIntervalInMillis) == 0) : "duration does not comprise an integral number of polling intervals.";

        this.durationInMillis = durationInMillis;
        this.pollIntervalInMillis = pollIntervalInMillis;
        this.clientCount = clientCount;
        
        Map<Integer, String> statusLabels = new HashMap<Integer, String>();
        for (Status s : Status.values()) {
            statusLabels.put(s.ordinal(), s.name());
        }
        this.responseStatuses.setDebugLabels(statusLabels);
    }

    /**
     * Process latency results as nanoseconds instead of milliseconds  
     * @param val
     */
    public void setEnableNanoseconds(boolean val) {
        this.enableNanoseconds = val;
    }
    
    public void setEnableBasePartitions(boolean val) {
        this.enableBasePartitions = val;
    }

    public Set<Error> getAnyErrors() {
        if (errors.size() == 0)
            return null;
        Set<Error> retval = new TreeSet<Error>();
        for (Error e : errors)
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
        // make sure we have reports from all the clients
        assert(data.size() == clientCount) : 
            String.format("%d != %d", data.size(), clientCount);
        return (this.completedIntervals);
    }

    /**
     * Return the total elapsed time of the benchmark in milliseconds
     */
    public long getElapsedTime() {
        return (this.completedIntervals * this.pollIntervalInMillis);
    }
    public long getIntervalDuration() {
        return pollIntervalInMillis;
    }
    public long getTotalDuration() {
        return durationInMillis;
    }
    public long getLastTimestamp() {
        return (this.lastTimestamp);
    }

    public String[] getTransactionNames() {
        String txnNames[] = new String[this.transactionNames.size()];
        for (String txnName : this.transactionNames.keySet()) {
            int offset = this.transactionNames.get(txnName).intValue();
            txnNames[offset] = txnName;
        }
        return (txnNames);
    }

    public Set<String> getClientNames() {
        Set<String> retval = new TreeSet<String>();
        retval.addAll(this.data.keySet());
        return retval;
    }
    public Histogram<Integer> getBasePartitions() {
        return (this.basePartitions);
    }
    public Histogram<String> getResponseStatuses() {
        return (this.responseStatuses);
    }

    private Histogram<Integer> getAllLatencies(boolean singlep, boolean dtxn) {
        ObjectHistogram<Integer> latencies = new ObjectHistogram<Integer>();
        for (Map<String, List<Result>> clientResults : data.values()) {
            for (List<Result> txnResults : clientResults.values()) {
                for (Result r : txnResults) {
                    if (r != null) {
                        if (singlep) latencies.put(r.spLatencies);
                        if (dtxn) latencies.put(r.dtxnLatencies);
                    }
                } // FOR
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Histogram<Integer> getAllTotalLatencies() {
        return this.getAllLatencies(true, true);
    }
    public Histogram<Integer> getAllSinglePartitionLatencies() {
        return this.getAllLatencies(true, false);
    }
    public Histogram<Integer> getAllDistributedLatencies() {
        return this.getAllLatencies(false, true);
    }
    
    private Histogram<Integer> getLastLatencies(boolean singlep, boolean dtxn) {
        Histogram<Integer> latencies = new ObjectHistogram<Integer>();
        for (Map<String, List<Result>> clientResults : data.values()) {
            for (List<Result> txnResults : clientResults.values()) {
                Result r = CollectionUtil.last(txnResults);
                if (r != null) {
                    if (singlep) latencies.put(r.spLatencies);
                    if (dtxn) latencies.put(r.dtxnLatencies);
                }
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Histogram<Integer> getLastTotalLatencies() {
        return this.getLastLatencies(true, true);
    }
    public Histogram<Integer> getLastSinglePartitionLatencies() {
        return this.getLastLatencies(true, false);
    }
    public Histogram<Integer> getLastDistributedLatencies() {
        return this.getLastLatencies(false, true);
    }
    
    private Histogram<Integer> getClientLatencies(String clientName, boolean dtxn) {
        Histogram<Integer> latencies = new ObjectHistogram<Integer>();
        Map<String, List<Result>> clientResults = data.get(clientName);
        if (clientResults == null) return (latencies);
        for (List<Result> results : clientResults.values()) {
            for (Result r : results) {
                latencies.put(dtxn ? r.dtxnLatencies : r.spLatencies);
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Histogram<Integer> getClientTotalLatencies(String clientName) {
        Histogram<Integer> latencies = new ObjectHistogram<Integer>();
        latencies.put(this.getClientLatencies(clientName, false));
        latencies.put(this.getClientLatencies(clientName, true));
        return (latencies);
    }
    public Histogram<Integer> getClientSinglePartitionLatencies(String clientName) {
        return this.getClientLatencies(clientName, false);
    }
    public Histogram<Integer> getClientDistributedLatencies(String clientName) {
        return this.getClientLatencies(clientName, true);
    }
    
    private Histogram<Integer> getTransactionLatencies(String txnName, boolean dtxn) {
        Histogram<Integer> latencies = new ObjectHistogram<Integer>();
        for (Map<String, List<Result>> clientResults : data.values()) {
            if (clientResults.containsKey(txnName) == false) continue;
            for (Result r : clientResults.get(txnName)) {
                latencies.put(dtxn ? r.dtxnLatencies : r.spLatencies);
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Histogram<Integer> getTransactionTotalLatencies(String txnName) {
        Histogram<Integer> latencies = new ObjectHistogram<Integer>();
        latencies.put(this.getTransactionLatencies(txnName, false));
        latencies.put(this.getTransactionLatencies(txnName, true));
        return (latencies);
    }
    public Histogram<Integer> getTransactionSinglePartitionLatencies(String txnName) {
        return this.getTransactionLatencies(txnName, false);
    }
    public Histogram<Integer> getTransactionDistributedLatencies(String txnName) {
        return this.getTransactionLatencies(txnName, true);
    }
    
    public Result[] getResultsForClientAndTransaction(String clientName, String txnName) {
        int intervals = getCompletedIntervalCount();
        
        Map<String, List<Result>> txnResults = data.get(clientName);
        List<Result> results = txnResults.get(txnName);
        assert(results != null) :
            String.format("Null results for txn '%s' from client '%s'\n%s",
                          txnName, clientName, StringUtil.formatMaps(txnResults));
        
        long txnsTillNow = 0;
        long specexecsTillNow = 0;
        long dtxnsTillNow = 0;
        Result[] retval = new Result[intervals];
        for (int i = 0; i < intervals; i++) {
            Result r = results.get(i);
            retval[i] = new Result(r.timestamp,
                                   r.benchmarkTimeDelta,
                                   r.transactionCount - txnsTillNow,
                                   r.specexecCount - specexecsTillNow,
                                   r.dtxnCount - dtxnsTillNow);
            txnsTillNow = r.transactionCount;
            specexecsTillNow = r.specexecCount;
            dtxnsTillNow = r.dtxnCount;
        } // FOR
//        assert(intervals == results.size());
        return retval;
    }
    
    public double[] computeIntervalTotals() {
        double results[] = new double[this.completedIntervals];
        Arrays.fill(results, 0d);
        
        for (Map<String, List<Result>> clientResults : data.values()) {
            for (List<Result> txnResults : clientResults.values()) {
                Result last = null;
                for (int i = 0; i < results.length; i++) {
                    Result r = txnResults.get(i); 
                    long total = r.transactionCount;
                    if (last != null) total -= last.transactionCount;
                    results[i] += total;
                    last = r;
                } // FOR
            } // FOR
        } // FOR
        return (results);
    }
    
    /**
     * Compute the total number of transactions executed for the entire benchmark
     * and the delta from that last time we were polled
     * @return 
     */
    public Pair<Long, Long> computeTotalAndDelta() {
        if (CACHE_computeTotalAndDelta == null) {
            synchronized (this) {
                long totalTxnCount = 0;
                long txnDelta = 0;
                
                for (Map<String, List<Result>> clientResults : this.data.values()) {
                    for (List<Result> txnResults : clientResults.values()) {
                        // Get previous result
                        int num_results = txnResults.size();
                        long prevTxnCount = (num_results > 1 ? txnResults.get(num_results-2).transactionCount : 0);
                        
                        // Get current result
                        Result current = CollectionUtil.last(txnResults);
                        long delta = current.transactionCount - prevTxnCount;
                        totalTxnCount += current.transactionCount;
                        txnDelta += delta;
                    } // FOR
                } // FOR
                CACHE_computeTotalAndDelta = Pair.of(totalTxnCount, txnDelta);
            } // SYNCH
        }
        return (CACHE_computeTotalAndDelta); 
    }

    /**
     * Store new TransactionCounter from a client thread
     * @param clientName
     * @param pollIndex
     * @param timestamp
     * @param cmpResults
     * @param errMsg
     * @return
     */
    public BenchmarkResults addPollResponseInfo(String clientName,
                                                int pollIndex,
                                                long timestamp,
                                                BenchmarkComponentResults cmpResults,
                                                String errMsg) {
        
        long benchmarkTime = pollIndex * this.pollIntervalInMillis;
        long offsetTime = timestamp - benchmarkTime;
        this.lastTimestamp = timestamp;

        if (errMsg != null) {
            Error err = new Error(clientName, errMsg, pollIndex);
            errors.add(err);
            return (null);
        }
        
        if (debug.val)
            LOG.debug(String.format("Setting Poll Response Info for '%s' [%d]:\n%s",
                      clientName, pollIndex, cmpResults.transactions));
        
        // Update Touched Histograms
        // This doesn't need to be synchronized
        if (this.enableBasePartitions) {
            this.basePartitions.put(cmpResults.basePartitions);
        }
        for (Status s : Status.values()) {
            long cnt = cmpResults.responseStatuses.get(s.ordinal(), 0);
            if (cnt > 0) this.responseStatuses.put(s.name(), cnt);
        } // FOR
        
        BenchmarkResults finishedIntervalClone = null;
        synchronized (this) {
            // put the transactions names:
            if (this.transactionNames.isEmpty()) {
                assert(cmpResults.transactions.getDebugLabels() != null);
                for (Entry<Object, String> e : cmpResults.transactions.getDebugLabels().entrySet()) {
                    Integer offset = (Integer)e.getKey();
                    this.transactionNames.put(e.getValue(), offset);
                } // FOR
            }
            
            // ensure there is an entry for the client
            Map<String, List<Result>> txnResults = this.data.get(clientName);
            if (txnResults == null) {
                txnResults = new TreeMap<String, List<Result>>();
                this.data.put(clientName, txnResults);
            }
    
            for (String txnName : this.transactionNames.keySet()) {
                List<Result> results = txnResults.get(txnName);
                if (results == null) {
                    results = new ArrayList<Result>();
                    txnResults.put(txnName, results);
                }
                assert(results != null);
                
                Integer txnOffset = this.transactionNames.get(txnName);
                Result r = new Result(timestamp,
                                      offsetTime,
                                      cmpResults.transactions.get(txnOffset.intValue(), 0),
                                      cmpResults.specexecs.get(txnOffset.intValue(), 0),
                                      cmpResults.dtxns.get(txnOffset.intValue(), 0));
                if (cmpResults.spLatencies != null) {
                    Histogram<Integer> latencies = cmpResults.spLatencies.get(txnOffset);
                    if (latencies != null) {
                        synchronized (latencies) {
                            r.spLatencies.put(latencies);
                        } // SYNCH
                    }
                }
                if (cmpResults.dtxnLatencies != null) {
                    Histogram<Integer> latencies = cmpResults.dtxnLatencies.get(txnOffset);
                    if (latencies != null) {
                        synchronized (latencies) {
                            r.dtxnLatencies.put(latencies);
                        } // SYNCH
                    }
                }
                results.add(r);
            } // FOR
            this.clientResultCount.put(clientName);
            if (debug.val)
                LOG.debug(String.format("New Result for '%s' => %d [minCount=%d]",
                          clientName, this.clientResultCount.get(clientName), this.clientResultCount.getMinCount()));
            if (this.clientResultCount.getMinCount() > this.completedIntervals && this.data.size() == this.clientCount) {
                this.completedIntervals = (int)this.clientResultCount.getMinCount();
                finishedIntervalClone = this.copy();
            }
        } // SYNCH
        
        return (finishedIntervalClone);
    }

    public BenchmarkResults copy() {
        BenchmarkResults clone = new BenchmarkResults(this.pollIntervalInMillis, this.durationInMillis, this.clientCount);
        clone.lastTimestamp = this.lastTimestamp;

        if (this.enableBasePartitions) {
            clone.basePartitions.put(this.basePartitions);
        }
        clone.responseStatuses.put(this.responseStatuses);
        clone.errors.addAll(this.errors);
        clone.transactionNames.putAll(this.transactionNames);
        clone.completedIntervals = this.completedIntervals;
        clone.clientResultCount.put(this.clientResultCount);

        for (Entry<String, Map<String, List<Result>>> entry : this.data.entrySet()) {
            Map<String, List<Result>> txnsForClient = new TreeMap<String, List<Result>>();
            for (Entry<String, List<Result>> entry2 : entry.getValue().entrySet()) {
                ArrayList<Result> newResults = new ArrayList<Result>();
                for (Result r : entry2.getValue())
                    newResults.add(r);
                txnsForClient.put(entry2.getKey(), newResults);
            }
            clone.data.put(entry.getKey(), txnsForClient);
        } // FOR

        return clone;
    }

    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Transaction Names", StringUtil.join("\n", transactionNames.keySet()));
        m.put("Transaction Data", data);
        m.put("Responses Statuses", basePartitions);
        
        if (this.enableBasePartitions) {
            m.put("Base Partitions", basePartitions);
        }
        return "BenchmarkResults\n" + StringUtil.formatMaps(m);
    }
}
