package edu.brown.api.results;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.api.BenchmarkController;

public class CSVResultsPrinter implements BenchmarkController.BenchmarkInterest {

    public static final ColumnInfo COLUMNS[] = {
        new ColumnInfo("INTERVAL", VoltType.INTEGER),
        new ColumnInfo("TRANSACTIONS", VoltType.BIGINT),
        new ColumnInfo("THROUGHPUT", VoltType.FLOAT),
        new ColumnInfo("LATENCY", VoltType.FLOAT),
        new ColumnInfo("EVICTING", VoltType.BOOLEAN),
        new ColumnInfo("CREATED", VoltType.BIGINT),
    };

    private final VoltTable results;
    private final File outputPath;
    private final AtomicBoolean evicting = new AtomicBoolean(false);
    
    public CSVResultsPrinter(File outputPath) {
        this.results = new VoltTable(COLUMNS);
        this.outputPath = outputPath;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults br) {
        try {
            FileWriter writer = new FileWriter(this.outputPath);
            VoltTableUtil.csv(writer, this.results, true);
            writer.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        String msg = "Wrote CSV results to '" + this.outputPath.getAbsolutePath() + "'";
        return (msg);
    }
    
    @Override
    public void benchmarkHasUpdated(BenchmarkResults br) {
        Pair<Long, Long> p = br.computeTotalAndDelta();
        assert(p != null);
        
        long txnDelta = p.getSecond();
        double tps = txnDelta / (double)(br.getIntervalDuration()) * 1000.0;
        
        this.results.addRow(
            br.getCompletedIntervalCount(),
            txnDelta,
            tps,
            0,
            this.evicting.compareAndSet(true, false),
            System.currentTimeMillis()/1000
        );
    }
    
    @Override
    public void markEvictionStart() {
        this.evicting.set(true);
    }

    @Override
    public void markEvictionStop() {
        // Do nothing...
    }
}
