package edu.brown.hstore.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.utils.EstTime;

import au.com.bytecode.opencsv.CSVWriter;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.TransactionProfiler;

/**
 * Quick utility method to dump out profile information about a txn
 * to string arrays so that we can write them to a CSV file
 * @author pavlo
 */
public class TransactionProfilerDumper {
    private static final Logger LOG = Logger.getLogger(TransactionProfilerDumper.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private File outputFile;
    private final CSVWriter writer;
    
    public TransactionProfilerDumper(File outputFile) {
        this.outputFile = outputFile;
        try {
            FileWriter out = new FileWriter(outputFile);
            this.writer = new CSVWriter(out);
        } catch (Exception ex) {
            LOG.fatal(String.format("Failed to create CSVWriter for '%s'", this.outputFile), ex);
            throw new RuntimeException(ex);
        }
        this.writeHeader();
    }
    
    private void flush() {
        try {
            this.writer.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public synchronized void close() throws IOException {
        this.writer.close();
    }
    
    public void writeHeader() {
        if (debug.val)
            LOG.debug(String.format("Writing %s header to '%s'",
                      this.getClass().getSimpleName(), this.outputFile));
        
        List<Object> row = new ArrayList<Object>();
        row.add("TXNID");
        row.add("PROCEDURE_NAME");
        row.add("BASE_PARTITION");
        row.add("DURATION");
        row.add("STATUS");
        row.add("RESTART_COUNTER");
        row.add("PREDICT_SINGLEPARTITION");
        row.add("PREDICT_ABORTABLE");
        row.add("PREDICT_READONLY");
        
        if (HStoreConf.singleton().site.txn_profiling) {
            TransactionProfiler profiler = new TransactionProfiler();
            for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
                row.add(pm.getName());
            }
        }
        
        String ret[] = new String[row.size()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = row.get(i).toString();
        }
        this.writer.writeNext(ret);
        this.flush();
    }
        
    public void writeRow(LocalTransaction ts) {
        if (debug.val) LOG.debug("Writing profile information for " + ts);
        
        List<Object> row = new ArrayList<Object>();
        
        // TxnId
        row.add(ts.getTransactionId());
        
        // Procedure Name
        row.add(ts.getProcedure().getName());
        
        // Base Partition
        row.add(ts.getBasePartition());
        
        // Duration
        row.add(EstTime.currentTimeMillis() - ts.getInitiateTime());
        
        // Status
        row.add(ts.getStatus());
        
        // Restart Counter
        row.add(ts.getRestartCounter());
        
        // Predict Flags
        row.add(ts.isPredictSinglePartition());
        row.add(ts.isPredictAbortable());
        row.add(ts.isPredictReadOnly());
        
        if (HStoreConf.singleton().site.txn_profiling &&
            ts.profiler != null) {
            for (ProfileMeasurement pm : ts.profiler.getProfileMeasurements()) {
                row.add(pm.getTotalThinkTimeMS());
            }
        }
        
        String ret[] = new String[row.size()];
        int i = 0;
        for (Object obj : row) {
            ret[i++] = (obj == null ? "null" : obj.toString());
        } // FOR
        
        synchronized (this) {
            this.writer.writeNext(ret);
        } // SYNCH
    }
    
}
