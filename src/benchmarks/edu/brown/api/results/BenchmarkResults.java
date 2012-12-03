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

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
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
        public Result(long benchmarkTimeDelta, long transactionCount, long specexecCount, long dtxnCount) {
            this.benchmarkTimeDelta = benchmarkTimeDelta;
            this.transactionCount = transactionCount;
            this.specexecCount = specexecCount;
            this.dtxnCount = dtxnCount;
        }
        public final long benchmarkTimeDelta;
        public final long transactionCount;
        public final long specexecCount;
        public final long dtxnCount;
        public final Histogram<Integer> latencies = new Histogram<Integer>();
        
        @Override
        public String toString() {
            return String.format("<TxnCount:%d / DtxnCount:%d / Delta:%d>",
                                 this.transactionCount, this.dtxnCount, this.benchmarkTimeDelta);
        }
    }

    /**
     * ClientName -> TxnName -> List<Result>
     */
    private final SortedMap<String, SortedMap<String, List<Result>>> m_data = new TreeMap<String, SortedMap<String, List<Result>>>();
    private final Set<Error> m_errors = new HashSet<Error>();

    protected final long m_durationInMillis;
    protected final long m_pollIntervalInMillis;
    protected final int m_clientCount;
    protected boolean enableNanoseconds = false;
    
    private boolean enableBasePartitions = false;
    private final Histogram<Integer> basePartitions = new Histogram<Integer>();
    
    private final Histogram<String> responseStatuses = new Histogram<String>();
    
    private int completedIntervals = 0;
    private final Histogram<String> clientResultCount = new Histogram<String>();
    
    // cached data for performance and consistency
    // TxnName -> FastIntHistogram Offset
    private final SortedMap<String, Integer> m_transactionNames = new TreeMap<String, Integer>();
    
    private Pair<Long, Long> CACHE_computeTotalAndDelta = null;

    public BenchmarkResults(long pollIntervalInMillis, long durationInMillis, int clientCount) {
        assert((durationInMillis % pollIntervalInMillis) == 0) : "duration does not comprise an integral number of polling intervals.";

        m_durationInMillis = durationInMillis;
        m_pollIntervalInMillis = pollIntervalInMillis;
        m_clientCount = clientCount;
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
        // make sure we have reports from all the clients
        assert(m_data.size() == m_clientCount) : 
            String.format("%d != %d", m_data.size(), m_clientCount);
        return (this.completedIntervals);
    }

    /**
     * Return the total elapsed time of the benchmark in milliseconds
     */
    public long getElapsedTime() {
        return (this.completedIntervals * this.m_pollIntervalInMillis);
    }
    
    
    public long getIntervalDuration() {
        return m_pollIntervalInMillis;
    }

    public long getTotalDuration() {
        return m_durationInMillis;
    }


    public String[] getTransactionNames() {
        String txnNames[] = new String[m_transactionNames.size()];
        for (String txnName : m_transactionNames.keySet()) {
            int offset = m_transactionNames.get(txnName).intValue();
            txnNames[offset] = txnName;
        }
        return (txnNames);
    }

    public Set<String> getClientNames() {
        Set<String> retval = new TreeSet<String>();
        retval.addAll(m_data.keySet());
        return retval;
    }
    public Histogram<Integer> getBasePartitions() {
        return (basePartitions);
    }
    public Histogram<String> getResponseStatuses() {
        return (responseStatuses);
    }

    public Histogram<Integer> getAllLatencies() {
        Histogram<Integer> latencies = new Histogram<Integer>();
        for (SortedMap<String, List<Result>> clientResults : m_data.values()) {
            for (List<Result> txnResults : clientResults.values()) {
                for (Result r : txnResults) {
                    latencies.put(r.latencies);
                } // FOR
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Histogram<Integer> getLastLatencies() {
        Histogram<Integer> latencies = new Histogram<Integer>();
        for (SortedMap<String, List<Result>> clientResults : m_data.values()) {
            for (List<Result> txnResults : clientResults.values()) {
                Result r = CollectionUtil.last(txnResults);
                if (r != null) latencies.put(r.latencies);
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Histogram<Integer> getLatenciesForClient(String clientName) {
        Histogram<Integer> latencies = new Histogram<Integer>();
        SortedMap<String, List<Result>> clientResults = m_data.get(clientName);
        if (clientResults == null) return (latencies);
        for (List<Result> results : clientResults.values()) {
            for (Result r : results) {
                latencies.put(r.latencies);
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Histogram<Integer> getLatenciesForTransaction(String txnName) {
        Histogram<Integer> latencies = new Histogram<Integer>();
        for (SortedMap<String, List<Result>> clientResults : m_data.values()) {
            if (clientResults.containsKey(txnName) == false) continue;
            for (Result r : clientResults.get(txnName)) {
                latencies.put(r.latencies);
            } // FOR
        } // FOR
        return (latencies);
    }
    
    public Result[] getResultsForClientAndTransaction(String clientName, String txnName) {
        int intervals = getCompletedIntervalCount();
        
        Map<String, List<Result>> txnResults = m_data.get(clientName);
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
            retval[i] = new Result(r.benchmarkTimeDelta,
                                   r.transactionCount - txnsTillNow,
                                   r.specexecCount - specexecsTillNow,
                                   r.dtxnCount - dtxnsTillNow);
            txnsTillNow = r.transactionCount;
            dtxnsTillNow = r.dtxnCount;
        }
//        assert(intervals == results.size());
        return retval;
    }
    
    public double[] computeIntervalTotals() {
        double results[] = new double[this.completedIntervals];
        Arrays.fill(results, 0d);
        
        for (SortedMap<String, List<Result>> clientResults : m_data.values()) {
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
                
                for (SortedMap<String, List<Result>> clientResults : m_data.values()) {
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
     * @param time
     * @param cmpResults
     * @param errMsg
     * @return
     */
    public BenchmarkResults addPollResponseInfo(String clientName,
                                                int pollIndex,
                                                long time,
                                                BenchmarkComponentResults cmpResults,
                                                String errMsg) {
        
        long benchmarkTime = pollIndex * m_pollIntervalInMillis;
        long offsetTime = time - benchmarkTime;

        if (errMsg != null) {
            Error err = new Error(clientName, errMsg, pollIndex);
            m_errors.add(err);
            return (null);
        }
        
        if (debug.get())
            LOG.debug(String.format("Setting Poll Response Info for '%s' [%d]:\n%s",
                                    clientName, pollIndex, cmpResults.transactions));
        
        // Update Touched Histograms
        // This doesn't need to be synchronized
        if (this.enableBasePartitions) {
            this.basePartitions.put(cmpResults.basePartitions);
        }
        this.responseStatuses.put(cmpResults.responseStatuses);
        
        BenchmarkResults finishedIntervalClone = null;
        synchronized (this) {
            // put the transactions names:
            if (m_transactionNames.isEmpty()) {
                assert(cmpResults.transactions.getDebugLabels() != null);
                for (Entry<Object, String> e : cmpResults.transactions.getDebugLabels().entrySet()) {
                    Integer offset = (Integer)e.getKey();
                    m_transactionNames.put(e.getValue(), offset);
                }
            }
            
            // ensure there is an entry for the client
            SortedMap<String, List<Result>> txnResults = m_data.get(clientName);
            if (txnResults == null) {
                txnResults = new TreeMap<String, List<Result>>();
                m_data.put(clientName, txnResults);
            }
    
            for (String txnName : m_transactionNames.keySet()) {
                List<Result> results = txnResults.get(txnName);
                if (results == null) {
                    results = new ArrayList<Result>();
                    txnResults.put(txnName, results);
                }
                assert(results != null);
                
                Integer offset = m_transactionNames.get(txnName);
                Result r = new Result(offsetTime,
                                      cmpResults.transactions.get(offset.intValue()),
                                      cmpResults.specexecs.get(offset.intValue()),
                                      cmpResults.dtxns.get(offset.intValue()));
                if (cmpResults.latencies != null) {
                    Histogram<Integer> latencies = cmpResults.latencies.get(offset);
                    if (latencies != null) {
                        synchronized (latencies) {
                            r.latencies.put(latencies);
                        } // SYNCH
                    }
                    
                }
                results.add(r);
            } // FOR
            this.clientResultCount.put(clientName);
            if (debug.get())
                LOG.debug(String.format("New Result for '%s' => %d [minCount=%d]",
                                       clientName, this.clientResultCount.get(clientName), this.clientResultCount.getMinCount()));
            if (this.clientResultCount.getMinCount() > this.completedIntervals && m_data.size() == m_clientCount) {
                this.completedIntervals = (int)this.clientResultCount.getMinCount();
                finishedIntervalClone = this.copy();
            }
        } // SYNCH
        
        return (finishedIntervalClone);
    }

    public BenchmarkResults copy() {
        BenchmarkResults clone = new BenchmarkResults(m_pollIntervalInMillis, m_durationInMillis, m_clientCount);

        if (this.enableBasePartitions) {
            clone.basePartitions.put(basePartitions);
        }
        clone.responseStatuses.put(responseStatuses);
        clone.m_errors.addAll(m_errors);
        clone.m_transactionNames.putAll(m_transactionNames);
        clone.completedIntervals = this.completedIntervals;
        clone.clientResultCount.put(this.clientResultCount);

        for (Entry<String, SortedMap<String, List<Result>>> entry : m_data.entrySet()) {
            SortedMap<String, List<Result>> txnsForClient = new TreeMap<String, List<Result>>();

            for (Entry<String, List<Result>> entry2 : entry.getValue().entrySet()) {
                ArrayList<Result> newResults = new ArrayList<Result>();
                for (Result r : entry2.getValue())
                    newResults.add(r);
                txnsForClient.put(entry2.getKey(), newResults);
            }

            clone.m_data.put(entry.getKey(), txnsForClient);
        }

        return clone;
    }

    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Transaction Names", StringUtil.join("\n", m_transactionNames.keySet()));
        m.put("Transaction Data", m_data);
        m.put("Responses Statuses", basePartitions);
        
        if (this.enableBasePartitions) {
            m.put("Base Partitions", basePartitions);
        }
        return "BenchmarkResults\n" + StringUtil.formatMaps(m);
    }
}
