package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.MathUtil;

public class TransactionProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(TransactionProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    @SuppressWarnings("unused")
    private final CatalogContext catalogContext;
    private int proc_offset;
    private int stdev_offset;
    
    private class ProcedureStats {
        /** Maintain a set of tuples for the transaction profile times **/
        final Queue<long[]> queue = new ConcurrentLinkedQueue<long[]>();
        final FastIntHistogram num_batches = new FastIntHistogram();
        final FastIntHistogram num_queries = new FastIntHistogram();
        final FastIntHistogram num_prefetch = new FastIntHistogram();        
    }

    private final Map<Procedure, ProcedureStats> procStats = Collections.synchronizedSortedMap(new TreeMap<Procedure, ProcedureStats>());
    
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
        if (trace.val) LOG.info("Calculating TransactionProfile information");

        ProcedureStats stats = this.procStats.get(catalog_proc);
        if (stats == null) {
            synchronized (this) {
                stats = this.procStats.get(catalog_proc);
                if (stats == null) {
                    stats = new ProcedureStats();
                    this.procStats.put(catalog_proc, stats);
                }
            } // SYNCH
        }
        
        long tuple[] = tp.getTuple();
        assert(tuple != null);
        if (trace.val)
            LOG.trace(String.format("Appending TransactionProfile: %s", Arrays.toString(tuple)));
        stats.queue.offer(tuple);
        synchronized (stats) {
            stats.num_batches.put(tp.getBatchCount());
            stats.num_queries.put(tp.getQueryCount());
            stats.num_prefetch.put(tp.getPrefetchCount());
        } // SYNCH
    }
    
    @SuppressWarnings("unchecked")
    private Object[] calculateTxnProfileTotals(Procedure catalog_proc) {
        if (debug.val) LOG.debug("Calculating profiling totals for " + catalog_proc.getName());
        Object row[] = null; 
        long tuple[] = null;
        int stdev_offset = this.stdev_offset - this.proc_offset - 1;
        
        // Each offset in this array is one profile measurement type
        ProcedureStats stats = this.procStats.get(catalog_proc);
        List<Long> stdevValues[] = null;
        while ((tuple = stats.queue.poll()) != null) {
            if (row == null) {
                row = new Object[tuple.length + 5];
                for (int i = 0; i < row.length; i++) { 
                    row[i] = 0l;
                }
                stdevValues = (List<Long>[])new List<?>[row.length];
            }
            
            // Global total # of txns
            row[0] = ((Long)row[0]) + 1;
            
            // ProfileMeasurement totals
            // There will be two numbers in each pair:
            //  (1) Total Time
            //  (2) Number of Invocations
            // We will want to keep track of the number of invocations so that
            // we can compute the standard deviation
            int offset = 4;
            for (int i = 0; i < tuple.length; i++) {
                row[offset] = ((Long)row[offset]) + tuple[i];
                
                // HACK!
                if (i % 2 == 0 && tuple[i] > 0 && tuple[i+1] > 0) {
                    if (stdevValues[offset] == null) {
                        stdevValues[offset] = new ArrayList<Long>();
                        if (trace.val) LOG.trace(String.format("%s - Created stdevValues at offset %d",
                                                   catalog_proc.getName(), offset));
                    }
                    stdevValues[offset].add(tuple[i] / tuple[i+1]);
                }
                offset++;
                if (offset == stdev_offset) offset++;
            } // FOR
        } // FOR
        row[1] = MathUtil.weightedMean(stats.num_batches);
        row[2] = MathUtil.weightedMean(stats.num_queries);
        row[3] = MathUtil.weightedMean(stats.num_prefetch);
        
        // HACK: Dump values for stdev
        int i = columnNameToIndex.get("FIRST_REMOTE_QUERY") - this.proc_offset - 1;
        if (stdevValues != null && stdevValues[i] != null) {
            double values[] = CollectionUtil.toDoubleArray(stdevValues[i]);
            row[stdev_offset] = MathUtil.stdev(values);
            if (trace.val) 
                LOG.trace(String.format("[%02d] %s => %f",
                          stdev_offset, catalog_proc.getName(), row[stdev_offset]));
        }
        
        return (row);
    }
    

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        final Iterator<Procedure> it = this.procStats.keySet().iterator();
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
        this.proc_offset = columns.size();
        columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("TRANSACTIONS", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("BATCHES_AVG", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("QUERIES_AVG", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("PREFETCH_AVG", VoltType.FLOAT));
        
        // Construct a dummy TransactionProfiler so that we can get the fields
        TransactionProfiler profiler = new TransactionProfiler();
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            String name = pm.getName().toUpperCase();
            // We need two columns per ProfileMeasurement
            //  (1) The total think time in nanoseconds
            //  (2) The number of invocations
            // See AbstractProfiler.getTuple()
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
            
            // Include the STDEV for FIRST_REMOTE_QUERY
            if (name.equalsIgnoreCase("FIRST_REMOTE_QUERY")) {
                this.stdev_offset = columns.size();
                columns.add(new VoltTable.ColumnInfo(name+"_STDEV", VoltType.FLOAT));        
            }
        } // FOR
        
        assert(this.proc_offset >= 0);
        assert(this.stdev_offset >= 0);
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        Procedure proc = (Procedure)rowKey;
        if (debug.val) LOG.debug("Collecting txn profiling stats for " + proc.getName());
        
        rowValues[this.proc_offset] = proc.getName();
        Object row[] = this.calculateTxnProfileTotals(proc);
        for (int i = 0; i < row.length; i++) {
            rowValues[this.proc_offset + i + 1] = row[i];    
        } // FOR
        super.updateStatsRow(rowKey, rowValues);
    }
}
