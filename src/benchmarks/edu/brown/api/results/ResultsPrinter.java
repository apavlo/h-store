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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

import edu.brown.api.BenchmarkInterest;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.HistogramUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;

/**
 * Standard printer for displaying benchmark results
 * @author pavlo
 */
public class ResultsPrinter implements BenchmarkInterest {
    private static final Logger LOG = Logger.getLogger(ResultsPrinter.class);
//
//    private static final String COL_HEADERS[] = {
//        "", // NAME
//        "", // TOTAL
//        "", // TOTAL %
//        "THROUGHPUT",
//        "LATENCY",
//    };
//    
    private static final String COL_FORMATS[] = {
        "%23s:",
        "%8d total",
        "(%5.1f%%)",
        "%8.2f txn/s",
        "%8s ms latency",
    };
    
    private static final String RESULT_FORMAT = "%.2f";
    private static final String SPACER = "  ";
    
    protected final boolean output_interval;
    protected final boolean output_clients;
    protected final boolean output_basepartitions;
    protected final boolean output_responses;
    private boolean stop = false;
    
    public ResultsPrinter(HStoreConf hstore_conf) {
        this.output_interval = hstore_conf.client.output_interval;
        this.output_clients = hstore_conf.client.output_clients;
        this.output_basepartitions = hstore_conf.client.output_basepartitions;
        this.output_responses = hstore_conf.client.output_status;
    }
    
    public void stop() {
        this.stop = true;
    };
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        if (this.stop) return (null);
        
        StringBuilder sb = new StringBuilder();
        FinalResult fr = new FinalResult(results);
        
        final int width = 100; 
        sb.append(String.format("\n%s\n", StringUtil.header("BENCHMARK RESULTS", "=", width)));
        
        // -------------------------------
        // GLOBAL TOTALS
        // -------------------------------
        StringBuilder throughput = new StringBuilder();
        throughput.append(String.format(RESULT_FORMAT + " txn/s", fr.getTotalTxnPerSecond()))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, fr.getMinTxnPerSecond()))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, fr.getMaxTxnPerSecond()))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, fr.getStandardDeviationTxnPerSecond()))
             .append("]");
        
        StringBuilder latencies = new StringBuilder();
        latencies.append(String.format(RESULT_FORMAT + " ms", fr.getTotalAvgLatency()))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, fr.getTotalMinLatency()))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, fr.getTotalMaxLatency()))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, fr.getTotalStdDevLatency()))
             .append("]");
        
        String txnInfo = String.format("Total:%d / Distributed:%d (%.1f%%)",
                                        fr.getTotalTxnCount(),
                                        fr.getTotalDtxnCount(),
                                        (fr.getTotalDtxnCount() / (double)fr.getTotalTxnCount())*100);
        if (fr.getTotalSpecExecCount() > 0) {
            txnInfo += String.format(" / SpecExec:%d (%.1f%%)",
                                        fr.getTotalSpecExecCount(),
                                        (fr.getTotalSpecExecCount() / (double)fr.getTotalTxnCount())*100);
        }
        
        
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("Execution Time", String.format("%d ms", fr.getDuration()));
        m.put("Transactions", txnInfo);
        m.put("Throughput", throughput.toString()); 
        m.put("Latency", latencies.toString());
        sb.append(StringUtil.formatMaps(m));
        sb.append("\n");

        // -------------------------------
        // TRANSACTION TOTALS
        // -------------------------------
        Collection<String> txnNames = fr.getTransactionNames();
        Collection<String> clientNames = fr.getClientNames();
        int num_rows = txnNames.size() + (this.output_clients ? clientNames.size() + 1 : 0);
        Object rows[][] = new String[num_rows][COL_FORMATS.length];
        int row_idx = 0;
        
//        rows[row_idx++] = COL_HEADERS;
        for (String txnName : txnNames) {
            EntityResult er = fr.getTransactionResult(txnName);
            assert(er != null);
            int col_idx = 0;
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], txnName);
            
            // TXN COUNT
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnCount());
            
            // TXN PERCENTAGE
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPercentage());
            
            // TXN / MS
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerMilli());
            
            // AVG LATENCY
            String txnAvgLatency = "-";
            if (er.getTxnCount() > 0) {
                txnAvgLatency = String.format(RESULT_FORMAT, er.getTxnAvgLatency());
            }
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], txnAvgLatency);
            
            row_idx++;
        } // FOR

        // -------------------------------
        // CLIENT TOTALS
        // -------------------------------
        if (this.output_clients) {
            rows[row_idx][0] = "\nBreakdown by client:";
            for (int i = 1; i < COL_FORMATS.length; i++) {
                rows[row_idx][i] = "";
            } // FOR
            row_idx++;
            
            for (String clientName : clientNames) {
                EntityResult er = fr.getClientResult(clientName);
                assert(er != null);
                int col_idx = 0;
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], clientName);
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnCount());
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPercentage());
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerMilli());
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerSecond());
                row_idx++;
            } // FOR
        }
        
        sb.append(StringUtil.repeat("-", width)).append("\n");
        sb.append(TableUtil.table(rows));
        sb.append(String.format("\n%s\n", StringUtil.repeat("=", width)));
        
        // -------------------------------
        // TXNS PER PARTITION
        // -------------------------------
        if (this.output_basepartitions) {
            sb.append("Transaction Base Partitions:\n");
            Histogram<Integer> h = results.getBasePartitions();
            h.enablePercentages();
            Map<Integer, String> labels = new HashMap<Integer, String>();
            for (Integer p : h.values()) {
                labels.put(p, String.format("Partition %02d", p));
            } // FOR
            h.setDebugLabels(labels);
            sb.append(StringUtil.prefix(h.toString((int)(width * 0.5)), "   "));
            sb.append(String.format("\n%s\n", StringUtil.repeat("=", width)));
        }
        
        // -------------------------------
        // CLIENT RESPONSES
        // -------------------------------
        if (this.output_responses) {
            sb.append("Client Response Statuses:\n");
            Histogram<String> h = results.getResponseStatuses();
            h.enablePercentages();
            sb.append(StringUtil.prefix(h.toString((int)(width * 0.5)), "   "));
            sb.append(String.format("\n%s\n", StringUtil.repeat("=", width)));
        }
        
        return (sb.toString());
    }
    
    @Override
    public void benchmarkHasUpdated(BenchmarkResults results) {
        if (this.stop) return;
        if (this.output_interval == false) return;
        
        Pair<Long, Long> p = results.computeTotalAndDelta();
        assert(p != null);
        long totalTxnCount = p.getFirst();
        long txnDelta = p.getSecond();
        
        // INTERVAL LATENCY
        Histogram<Integer> lastLatencies = results.getLastLatencies();
        double intervalLatency = HistogramUtil.sum(lastLatencies) / (double)lastLatencies.getSampleCount();
        
        // TOTAL LATENCY
        Histogram<Integer> allLatencies = results.getAllLatencies();        
        double totalLatency = HistogramUtil.sum(allLatencies) / (double)allLatencies.getSampleCount();
        if (totalLatency < 0) {
            LOG.warn("Negative Total Latency: " + totalLatency);
            LOG.warn("# of All Latencies: " + allLatencies.getSampleCount());
            LOG.warn("All Latencies:\n" + allLatencies);
            assert(false);
        }

        int pollIndex = results.getCompletedIntervalCount();
        long duration = results.getTotalDuration();
        long pollCount = duration / results.getIntervalDuration();
        long currentTime = pollIndex * results.getIntervalDuration();

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("At time %d out of %d (%d%%):",
                                currentTime, duration, currentTime * 100 / duration));
        sb.append("\n" + SPACER);
        sb.append(String.format("In the past %d ms:",
                                duration / pollCount));
        sb.append("\n" + SPACER + SPACER);
        sb.append(String.format("Completed %d txns at a rate of " + RESULT_FORMAT + " txns/s",
                                txnDelta, txnDelta / (double)(results.getIntervalDuration()) * 1000d));
        sb.append(String.format(" with " + RESULT_FORMAT + " ms avg latency", intervalLatency));
        
        sb.append("\n" + SPACER);
        sb.append("Since the benchmark began:");
        sb.append("\n" + SPACER + SPACER);
        sb.append(String.format("Completed %d txns at a rate of " + RESULT_FORMAT + " txns/s",
                                totalTxnCount, totalTxnCount / (double)(pollIndex * results.getIntervalDuration()) * 1000d));
        sb.append(String.format(" with " + RESULT_FORMAT + " ms avg latency", totalLatency));
        
        System.out.println();
        if (LOG.isDebugEnabled()) LOG.debug("Printing result information for poll index " + pollIndex);
        System.out.println(sb);
        System.out.flush();
    }

    @Override
    public void markEvictionStart() {
        // TODO Auto-generated method stub
    }

    @Override
    public void markEvictionStop() {
        // TODO Auto-generated method stub
    }
}
