package edu.brown.api.results;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.api.BenchmarkInterest;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.HistogramUtil;

public class CSVResultsPrinter implements BenchmarkInterest {
    private static final Logger LOG = Logger.getLogger(CSVResultsPrinter.class);

    public static final int[] percentiles = { 50, 95, 99 };
    public static final ColumnInfo COLUMNS[] = {
        new ColumnInfo("INTERVAL", VoltType.INTEGER),
        new ColumnInfo("ELAPSED", VoltType.BIGINT),
        new ColumnInfo("TIMESTAMP", VoltType.BIGINT),
        new ColumnInfo("TRANSACTIONS", VoltType.BIGINT),
        new ColumnInfo("THROUGHPUT", VoltType.FLOAT),
        new ColumnInfo("LATENCY", VoltType.FLOAT),
        new ColumnInfo("EVICTING", VoltType.INTEGER),
        new ColumnInfo("LATENCY_50", VoltType.FLOAT),
        new ColumnInfo("LATENCY_95", VoltType.FLOAT),
        new ColumnInfo("LATENCY_99", VoltType.FLOAT),

    };

    private final List<Object[]> results = new ArrayList<Object[]>(); 
    private final File outputPath;
    private final AtomicBoolean evicting = new AtomicBoolean(false);
    private Object last_eviction_row[] = null;
    private long last_eviction_start = -1;
    private long last_eviction_stop = -1;
    private boolean stop = false;
    private int intervalCounter = 0;
    
    public CSVResultsPrinter(File outputPath) {
        this.outputPath = outputPath;
    }
    
    @Override
    public void stop() {
        this.stop = true;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults br) {
        if (this.stop) return (null);
        
        VoltTable vt = new VoltTable(COLUMNS);
        for (Object row[] : this.results) {
            vt.addRow(row);
        }
        
        try {
            FileWriter writer = new FileWriter(this.outputPath);
            VoltTableUtil.csv(writer, vt, true);
            writer.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        LOG.info("Wrote CSV results to '" + this.outputPath.getAbsolutePath() + "'");
        return (null);
    }
    
    @Override
    public void benchmarkHasUpdated(BenchmarkResults br) {
        if (this.stop) return;
        
        Pair<Long, Long> p = br.computeTotalAndDelta();
        assert(p != null);
        
        boolean new_eviction = this.evicting.compareAndSet(true, false);
        
        // INTERVAL THROUGHPUT
        long txnDelta = p.getSecond();
        double intervalThroughput = txnDelta / (double)(br.getIntervalDuration()) * 1000.0;
        
        // INTERVAL LATENCY
        Histogram<Integer> lastLatencies = br.getLastTotalLatencies();
        double intervalLatency = HistogramUtil.sum(lastLatencies) / (double)lastLatencies.getSampleCount();
        double[] latencies = HistogramUtil.percentile(lastLatencies, percentiles);
        double intervalLatency_50 = latencies[0];
        double intervalLatency_95 = latencies[1];
        double intervalLatency_99 = latencies[2];
        Object row[] = {
            this.intervalCounter++,
            br.getElapsedTime(),
            br.getLastTimestamp(),
            txnDelta,
            intervalThroughput,
            intervalLatency,
            0,
            intervalLatency_50,
            intervalLatency_95,
            intervalLatency_99
        };
        this.results.add(row);
        
        if (new_eviction) {
            synchronized (CSVResultsPrinter.class) {
                if (this.last_eviction_stop == -1) {
                    this.last_eviction_row = row;    
                } else {
                    this.updateLastEviction(row);
                    this.last_eviction_row = null;
                }
            } // SYNCH
        }
    }
    
    private void updateLastEviction(Object row[]) {
        row[4] = this.last_eviction_stop - this.last_eviction_start;
        this.last_eviction_start = -1;
        this.last_eviction_stop = -1;
    }
    
    @Override
    public void markEvictionStart() {
        this.evicting.set(true);
        this.last_eviction_start = System.currentTimeMillis();
        this.last_eviction_stop = -1;
    }

    @Override
    public void markEvictionStop() {
        this.last_eviction_stop = System.currentTimeMillis();
        synchronized (CSVResultsPrinter.class) {
            if (this.last_eviction_row != null) {
                Object row[] = this.last_eviction_row;
                this.last_eviction_row = null;
                this.updateLastEviction(row);
            }
        } // SYNCH
    }
}
