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

package edu.brown.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.utils.Pair;

import edu.brown.api.BenchmarkResults.EntityResult;
import edu.brown.api.BenchmarkResults.FinalResult;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;

public class ResultsPrinter implements BenchmarkController.BenchmarkInterest {

    private static final String COL_FORMATS[] = {
        "%23s:",
        "%10d total",
        "(%5.1f%%)",
        "%8.2f txn/s",
        "%10.2f txn/m",
    };
    
    private static final String RESULT_FORMAT = "%.2f";
    
    protected final boolean output_clients;
    protected final boolean output_basepartitions;
    protected final boolean output_responses;
    
    public ResultsPrinter(boolean output_clients, boolean output_basepartitions, boolean output_responses) {
        this.output_clients = output_clients;
        this.output_basepartitions = output_basepartitions;
        this.output_responses = output_responses;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        StringBuilder sb = new StringBuilder();
        FinalResult fr = new FinalResult(results);
        
        final int width = 100; 
        sb.append(String.format("\n%s\n", StringUtil.header("BENCHMARK RESULTS", "=", width)));
        
        // -------------------------------
        // GLOBAL TOTALS
        // -------------------------------
        StringBuilder inner = new StringBuilder();
        inner.append(String.format(RESULT_FORMAT + " txn/s", fr.getTotalTxnPerSecond()))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, fr.getMinTxnPerSecond()))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, fr.getMaxTxnPerSecond()))
             .append(" / ")
             .append(String.format("stddev:" + RESULT_FORMAT, fr.getStandardDeviationTxnPerSecond()))
             .append("]\n\n");
        
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Execution Time", String.format("%d ms", fr.getDuration()));
        m.put("Total Transactions", fr.getTotalTxnCount());
        m.put("Throughput", inner.toString()); 
        sb.append(StringUtil.formatMaps(m));

        // -------------------------------
        // TRANSACTION TOTALS
        // -------------------------------
        Collection<String> txnNames = fr.getTransactionNames();
        Collection<String> clientNames = fr.getClientNames();
        int num_rows = txnNames.size() + (this.output_clients ? clientNames.size() + 1 : 0);
        Object rows[][] = new String[num_rows][COL_FORMATS.length];
        int row_idx = 0;
        
        for (String txnName : txnNames) {
            EntityResult er = fr.getTransactionResult(txnName);
            assert(er != null);
            int col_idx = 0;
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], txnName);
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnCount());
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPercentage());
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerMilli());
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerSecond());
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
        Pair<Long, Long> p = results.computeTotalAndDelta();
        assert(p != null);
        long totalTxnCount = p.getFirst();
        long txnDelta = p.getSecond();

        int pollIndex = results.getCompletedIntervalCount();
        long duration = results.getTotalDuration();
        long pollCount = duration / results.getIntervalDuration();
        long currentTime = pollIndex * results.getIntervalDuration();

        System.out.printf("\nAt time %d out of %d (%d%%):\n", currentTime, duration, currentTime * 100 / duration);
        System.out.printf("  In the past %d ms:\n", duration / pollCount);
        System.out.printf("    Completed %d txns at a rate of " + RESULT_FORMAT + " txns/s\n",
                txnDelta,
                txnDelta / (double)(results.getIntervalDuration()) * 1000.0);
        System.out.printf("  Since the benchmark began:\n");
        System.out.printf("    Completed %d txns at a rate of " + RESULT_FORMAT + " txns/s\n",
                totalTxnCount,
                totalTxnCount / (double)(pollIndex * results.getIntervalDuration()) * 1000.0);


//        if ((pollIndex * results.getIntervalDuration()) >= duration) {
//            // print the final results
//            System.out.println(this.formatFinalResults(results));
//        }

        System.out.flush();
    }
}
