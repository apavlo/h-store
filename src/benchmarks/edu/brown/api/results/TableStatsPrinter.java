package edu.brown.api.results;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
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
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Poll the system at every interval to collect table information 
 *
 */
public class TableStatsPrinter implements BenchmarkInterest {
    private static final Logger LOG = Logger.getLogger(TableStatsPrinter.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    public static final ColumnInfo COLUMNS[] = {
        new ColumnInfo("INTERVAL", VoltType.INTEGER),
        new ColumnInfo("ELAPSED", VoltType.BIGINT),
        new ColumnInfo("TIMESTAMP", VoltType.BIGINT),     
        new ColumnInfo("HOST_ID", VoltType.INTEGER),
        new ColumnInfo("HOSTNAME", VoltType.STRING),       
        new ColumnInfo("SITE_ID", VoltType.INTEGER),       
        new ColumnInfo("PARTITION_ID", VoltType.INTEGER),       
        new ColumnInfo("TABLE_NAME", VoltType.STRING),       
        new ColumnInfo("TABLE_TYPE", VoltType.STRING),
        // ACTIVE
        new ColumnInfo("TUPLE_COUNT", VoltType.INTEGER),
        new ColumnInfo("TUPLE_ACCESSES", VoltType.BIGINT),        
        new ColumnInfo("TUPLE_ALLOCATED_MEMORY", VoltType.BIGINT),
        new ColumnInfo("TUPLE_DATA_MEMORY", VoltType.BIGINT),        
        new ColumnInfo("STRING_DATA_MEMORY", VoltType.BIGINT),
        new ColumnInfo("ANTICACHE_TUPLES_EVICTED", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BLOCKS_EVICTED", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BYTES_EVICTED", VoltType.BIGINT),
        // GLOBAL WRITTEN
        new ColumnInfo("ANTICACHE_TUPLES_WRITTEN", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BLOCKS_WRITTEN", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BYTES_WRITTEN", VoltType.BIGINT),
        // GLOBAL READ
        new ColumnInfo("ANTICACHE_TUPLES_READ", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BLOCKS_READ", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BYTES_READ", VoltType.BIGINT),
    };
    

    private final List<Object[]> results = new ArrayList<Object[]>(); 
    private final Client client;
    private final File outputPath;
    private boolean stop = false;
    private int intervalCounter = 0;
    
    public TableStatsPrinter(Client client, File outputPath) {
        this.client = client;
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
        LOG.info("Wrote CSV table stats periodically to '" + this.outputPath.getAbsolutePath() + "'");
        return (null);
    }
    
    @Override
    public void benchmarkHasUpdated(final BenchmarkResults br) {
        if (this.stop) return;
        final int interval = this.intervalCounter++;
        
        // HACK: Skip every other interval
        if (interval % 2 != 0) return;
        
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                if (clientResponse.getStatus() != Status.OK) {
                    LOG.warn("Failed to get table stats", clientResponse.getException());
                    return;
                }
                if (debug.val)
                    LOG.debug("Updating table stats information [interval=" + interval + "]");
                
                VoltTable vt = clientResponse.getResults()[0];
                    
                // CALCULATE DELAY
                long timestamp = System.currentTimeMillis();
                long delay = timestamp - br.getLastTimestamp(); 
                
                while (vt.advanceRow()) {
                	Object row[] = new Object[COLUMNS.length];
                    int idx = 0;
                    row[idx++] = interval;
                    row[idx++] = br.getElapsedTime() + delay;                    
                    row[idx++] = timestamp; 
                    
                    while(idx < COLUMNS.length) {                    	
                        row[idx] = vt.get(idx-2, COLUMNS[idx].getType());
                        idx++;
                    }
                    TableStatsPrinter.this.results.add(row);
                };
                
            }
        };
        
        String procName = VoltSystemProcedure.procCallName(Statistics.class);
        Object params[] = { SysProcSelector.TABLE.name(), 0 }; 
        try {
            if (debug.val) LOG.debug("Retrieving memory stats from cluster");
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
