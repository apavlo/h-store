package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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

public class TransactionProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(TransactionProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Maintain a set of tuples for the transaction profile times
     */
    private final Map<Procedure, Queue<long[]>> profileQueues = new TreeMap<Procedure, Queue<long[]>>();
    private final Map<Procedure, long[]> profileTotals = Collections.synchronizedSortedMap(new TreeMap<Procedure, long[]>());

    public TransactionProfilerStats(CatalogContext catalogContext) {
        super(SysProcSelector.TXNCOUNTER.name(), false);
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
    
    private long[] calculateTxnProfileTotals(Procedure catalog_proc) {
        long totals[] = this.profileTotals.get(catalog_proc); 
        long tuple[] = null;
        Queue<long[]> queue = this.profileQueues.get(catalog_proc); 
        while ((tuple = queue.poll()) != null) {
            if (totals == null) {
                totals = new long[tuple.length+1];
                Arrays.fill(totals, 0l);
                this.profileTotals.put(catalog_proc, totals);
            }
            totals[0]++;
            for (int i = 0, cnt = tuple.length; i < cnt; i++) {
                totals[i+1] += tuple[i];
            } // FOR
        } // FOR
        return (totals);
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
            // See AbstractProfiler.getTuple()
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        Procedure proc = (Procedure)rowKey;
        
        long totals[] = this.calculateTxnProfileTotals(proc);
        int offset = columnNameToIndex.get("PROCEDURE");
        
        rowValues[offset++] = proc.getName();
        for (int i = 0; i < totals.length; i++) {
            rowValues[offset + i] = totals[i];
        } // FOR
        super.updateStatsRow(rowKey, rowValues);
    }
}
