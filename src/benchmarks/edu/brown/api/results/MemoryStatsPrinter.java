package edu.brown.api.results;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.MemoryStats;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.api.BenchmarkInterest;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.CollectionUtil;

/**
 * Poll the system at every interval to collect memory information 
 * @author pavlo
 *
 */
public class MemoryStatsPrinter implements BenchmarkInterest {
    private static final Logger LOG = Logger.getLogger(MemoryStatsPrinter.class);

    private final List<Object[]> results = new ArrayList<Object[]>(); 
    private final Client client;
    private final File outputPath;
    private boolean stop = false;
    private int intervalCounter = 0;
    private final ColumnInfo columns[];
    
    public MemoryStatsPrinter(Client client, File outputPath) {
        this.client = client;
        this.outputPath = outputPath;
        
        List<VoltTable.ColumnInfo> temp = new ArrayList<VoltTable.ColumnInfo>();
        temp.add(new ColumnInfo("INTERVAL", VoltType.INTEGER));
        temp.add(new ColumnInfo("ELAPSED", VoltType.BIGINT));
        temp.add(new ColumnInfo("TIMESTAMP", VoltType.BIGINT));
        CollectionUtil.addAll(temp, MemoryStats.COLUMNS);
        this.columns = temp.toArray(new ColumnInfo[0]);
    }
    
    @Override
    public void stop() {
        this.stop = true;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults br) {
        if (this.stop) return (null);
        
        VoltTable vt = new VoltTable(this.columns);
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
        String msg = "Wrote CSV memory stats to '" + this.outputPath.getAbsolutePath() + "'";
        return (msg);
    }
    
    @Override
    public void benchmarkHasUpdated(final BenchmarkResults br) {
        if (this.stop) return;
        
        final int interval = this.intervalCounter++;
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                if (clientResponse.getStatus() != Status.OK) {
                    LOG.warn("Failed to get memory stats", clientResponse.getException());
                    return;
                }
                
                // TOTAL STATS FROM ALL SITES 
                VoltTable vt = clientResponse.getResults()[0];
                LOG.info("Memory Stats Info:\n" + VoltTableUtil.format(vt));
                long totals[] = new long[vt.getColumnCount()];
                Arrays.fill(totals, 0);
                int colOffsets[] = null;
                
                while (vt.advanceRow()) {
                    if (colOffsets == null) {
                        colOffsets = new int[MemoryStats.COLUMNS.length];
                        for (int i = 0; i < MemoryStats.COLUMNS.length; i++) {
                            ColumnInfo col = MemoryStats.COLUMNS[i];
                            colOffsets[i] = vt.getColumnIndex(col.getName());
                        } // FOR
                    }
                    for (int offset : colOffsets) {
                        totals[offset] += vt.getLong(offset); 
                    } // FOR
                };

                // CALCULATE DELAY
                long timestamp = System.currentTimeMillis();
                long delay = timestamp - br.getLastTimestamp(); 
                
                // CONSTRUCT ROW
                Object row[] = new Object[columns.length];
                int idx = 0;
                row[idx++] = interval;
                row[idx++] = br.getElapsedTime() + delay;
                row[idx++] = timestamp;
                for (int offset : colOffsets) {
                    row[idx++] = totals[offset];
                } // FOR
                MemoryStatsPrinter.this.results.add(row);
            }
        };
        
        String procName = VoltSystemProcedure.procCallName(Statistics.class);
        Object params[] = { SysProcSelector.MEMORY.name(), 0 };
        try {
            LOG.info("Retrieving memory stats from cluster");
            boolean result = this.client.callProcedure(callback, procName, params);
            assert(result) : "Failed to queue " + procName + " request";
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Override
    public void markEvictionStart() {
        // NOTHING
    }

    @Override
    public void markEvictionStop() {
        // NOTHING
    }
}
