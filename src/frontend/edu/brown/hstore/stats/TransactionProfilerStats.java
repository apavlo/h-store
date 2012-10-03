package edu.brown.hstore.stats;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.TransactionProfiler;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

public class TransactionProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(TransactionProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static File DUMP_DIR = null;
    
    private final CatalogContext catalogContext;
    
    /**
     * Maintain a set of tuples for the transaction profile times
     */
    private final Map<Procedure, Queue<long[]>> profileQueues = Collections.synchronizedSortedMap(new TreeMap<Procedure, Queue<long[]>>());
    // private final Map<Procedure, long[]> profileTotals = Collections.synchronizedSortedMap(new TreeMap<Procedure, long[]>());
    private final Map<Procedure, List<Long>[]> profileValues = new HashMap<Procedure, List<Long>[]>();

    public TransactionProfilerStats(CatalogContext catalogContext) {
        super(SysProcSelector.TXNCOUNTER.name(), false);
        this.catalogContext = catalogContext;
    }
    
    /**
     * 
     * @param tp
     */
    public void addTxnProfile(Procedure catalog_proc, TransactionProfiler tp) {
        assert(catalog_proc != null);
        assert(tp.isStopped());
        if (trace.get()) LOG.info("Calculating TransactionProfile information");

        Queue<long[]> queue = this.profileQueues.get(catalog_proc);
        if (queue == null) {
            synchronized (this) {
                queue = this.profileQueues.get(catalog_proc);
                if (queue == null) {
                    queue = new ConcurrentLinkedQueue<long[]>();
                    this.profileQueues.put(catalog_proc, queue);
                }
            } // SYNCH
        }
        
        long tuple[] = tp.getTuple();
        assert(tuple != null);
        if (trace.get())
            LOG.trace(String.format("Appending TransactionProfile: %s", Arrays.toString(tuple)));
        queue.offer(tuple);
    }
    
    @SuppressWarnings("unchecked")
    private long[] calculateTxnProfileTotals(Procedure catalog_proc) {
        if (debug.get()) LOG.debug("Calculating profiling totals for " + catalog_proc.getName());
        long row[] = null; // this.profileTotals.get(catalog_proc); 
        long tuple[] = null;
        
        // Each offset in this array is one profile measurement type
        Queue<long[]> queue = this.profileQueues.get(catalog_proc);
        List<Long> stdevValues[] = this.profileValues.get(catalog_proc);
        while ((tuple = queue.poll()) != null) {
            if (row == null) {
                row = new long[tuple.length + 1];
                Arrays.fill(row, 0l);
                
                // row = new Object[tuple.length + (tuple.length/2) + 1];
                // this.profileTotals.put(catalog_proc, totals);
                
                stdevValues = (List<Long>[])new List<?>[row.length];
                this.profileValues.put(catalog_proc, stdevValues);
            }
            
            // Global total # of txns
            row[0] += 1;
            
            // ProfileMeasurement totals
            // There will be two numbers in each pair:
            //  (1) Total Time
            //  (2) Number of Invocations
            // We will want to keep track of the number of invocations so that
            // we can compute the standard deviation
            int offset = 1;
            for (int i = 0; i < tuple.length; i++) {
                row[offset] += tuple[i];
                
                // HACK!
                if (i % 2 == 0 && tuple[i] > 0 && tuple[i+1] > 0) {
                    if (stdevValues[offset] == null) {
                        stdevValues[offset] = new ArrayList<Long>();
                        if (trace.get()) LOG.trace(String.format("%s - Created stdevValues at offset %d",
                                                   catalog_proc.getName(), offset));
                    }
                    stdevValues[offset].add(tuple[i] / tuple[i+1]);
                }
                offset++;
            } // FOR
        } // FOR
        
        return (row);
    }
    

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        final Iterator<Procedure> it = this.profileQueues.keySet().iterator();
        return new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            @Override
            public Object next() {
                return it.next();
            }
            @Override
            public void remove() {
                it.remove();
            }
        };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("TRANSACTIONS", VoltType.BIGINT));
        
        // Construct a dummy TransactionProfiler so that we can get the fields
        TransactionProfiler profiler = new TransactionProfiler();
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            String name = pm.getType().toUpperCase();
            // We need two columns per ProfileMeasurement
            //  (1) The total think time in nanoseconds
            //  (2) The number of invocations
            //  (3) Standard Deviation
            // See AbstractProfiler.getTuple()
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
//            columns.add(new VoltTable.ColumnInfo(name+"_STDDEV", VoltType.FLOAT));
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        Procedure proc = (Procedure)rowKey;
        if (debug.get()) LOG.debug("Collecting txn profiling stats for " + proc.getName());
        
        final int offset = columnNameToIndex.get("PROCEDURE");
        rowValues[offset] = proc.getName();
        
        long row[] = this.calculateTxnProfileTotals(proc);
        for (int i = 0; i < row.length; i++) {
            rowValues[offset + i + 1] = row[i];    
        } // FOR
        super.updateStatsRow(rowKey, rowValues);
        
        // HACK: Dump values for stdev
        int i = columnNameToIndex.get("FIRST_REMOTE_QUERY") - offset - 1;
        List<Long> stdevValues[] = this.profileValues.get(proc);
        if (stdevValues != null && stdevValues[i] != null) {
            if (DUMP_DIR == null) {
                DUMP_DIR = FileUtil.getTempDirectory(this.catalogContext.database.getProject());
                LOG.info("Created " + this.getClass().getSimpleName() + " dump directory: " + DUMP_DIR);
            }
            
            File output = new File(String.format("%s/%s.csv", DUMP_DIR, proc.getName()));
            try {
                FileUtil.writeStringToFile(output, StringUtil.join("\n", stdevValues[i]) + "\n");
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            if (debug.get()) LOG.debug(String.format("Wrote %d %s FIRST_REMOTE_QUERY values to %s",
                                       stdevValues[i].size(), proc.getName(), output));
        } else if (trace.get()) {
            LOG.trace(String.format("Failed to find FIRST_REMOTE_QUERY entries for %s " +
                      "[orig=%d / offset=%d / i=%d] -> %s",
                      proc.getName(), columnNameToIndex.get("FIRST_REMOTE_QUERY"), offset, i, stdevValues[i]));
        }
    }
}
