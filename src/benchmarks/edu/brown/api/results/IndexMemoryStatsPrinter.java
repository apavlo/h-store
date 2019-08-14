package edu.brown.api.results;

import edu.brown.api.BenchmarkInterest;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Poll the system at every interval to collect Index memory information
 */
public class IndexMemoryStatsPrinter implements BenchmarkInterest {
    private static final Logger LOG = Logger.getLogger(IndexMemoryStatsPrinter.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    private final List<Object[]> results = new ArrayList<Object[]>();
    private final Client client;
    private final File outputPath;
    private boolean stop = false;
    private int intervalCounter = 0;
    private ColumnInfo columns[];
    private boolean colsInitialized = false;

    public IndexMemoryStatsPrinter(Client client, File outputPath) {
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
        LOG.info("Wrote CSV index memory stats to '" + this.outputPath.getAbsolutePath() + "'");
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
                    LOG.warn("Failed to get index memory stats", clientResponse.getException());
                    return;
                }
                if (debug.val)
                    LOG.debug("Updating index memory stats information [interval=" + interval + "]");

                // TOTAL STATS FROM ALL SITES
                VoltTable vt = clientResponse.getResults()[0];
                if (!IndexMemoryStatsPrinter.this.colsInitialized) {
                    List<VoltTable.ColumnInfo> temp = new ArrayList<VoltTable.ColumnInfo>();
                    temp.add(new ColumnInfo("INTERVAL", VoltType.INTEGER));
                    temp.add(new ColumnInfo("ELAPSED", VoltType.BIGINT));
                    temp.add(new ColumnInfo("TIMESTAMP_BENCHMARK", VoltType.BIGINT));
                    for (int i = 0; i < vt.getColumnCount(); i++) {
                        temp.add(new ColumnInfo(vt.getColumnName(i), vt.getColumnType(i)));
                    }
                    IndexMemoryStatsPrinter.this.columns = temp.toArray(new ColumnInfo[0]);
                    IndexMemoryStatsPrinter.this.colsInitialized = true;
                }


                Object totals[] = new Object[vt.getColumnCount()];
                Arrays.fill(totals, null);
                int colOffsets[] = null;

                while (vt.advanceRow()) {
                    if (colOffsets == null) {
                        String[] colNames = vt.getColumnNames();
                        colOffsets = new int[colNames.length];
                        for (int i = 0; i < colNames.length; i++) {
                            String col = colNames[i];
                            colOffsets[i] = vt.getColumnIndex(col);
                        } // FOR

                    }
                    for (int i = 0; i < vt.getColumnCount(); i++) {
                        int offset = colOffsets[i];
                        switch (vt.getColumnType(i)) {
                            case TINYINT:
                            case SMALLINT:
                            case INTEGER:
                            case BIGINT:
                                totals[offset] = vt.getLong(i);
                                break;
                            case FLOAT:
                                totals[offset] = vt.getDouble(i);
                                break;
                            case TIMESTAMP:
                                totals[offset] = vt.getTimestampAsTimestamp(i);
                                break;
                            case STRING:
                                totals[offset] = vt.getString(i);
                                break;
                            case DECIMAL:
                                totals[offset] = vt.getDecimalAsBigDecimal(i);
                                break;
                        }
                    } // FOR
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
                    IndexMemoryStatsPrinter.this.results.add(row);

                }
                ;

            }
        };

        String procName = VoltSystemProcedure.procCallName(Statistics.class);
        Object params[] = {SysProcSelector.INDEX.name(), 0};
        try {
            if (debug.val) LOG.debug("Retrieving index memory stats from cluster");
            boolean result = this.client.callProcedure(callback, procName, params);
            assert (result) : "Failed to queue " + procName + " request";
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
