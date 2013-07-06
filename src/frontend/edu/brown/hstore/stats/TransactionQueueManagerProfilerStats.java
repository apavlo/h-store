package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.PartitionLockQueue;
import edu.brown.hstore.TransactionQueueManager;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.PartitionLockQueueProfiler;
import edu.brown.profilers.TransactionQueueManagerProfiler;
import edu.brown.utils.MathUtil;

public class TransactionQueueManagerProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(PartitionExecutorProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final HStoreSite hstore_site;
    private final TransactionQueueManager queue_manager;

    public TransactionQueueManagerProfilerStats(HStoreSite hstore_site) {
        super(SysProcSelector.QUEUEPROFILER.name(), false);
        this.hstore_site = hstore_site;
        this.queue_manager = hstore_site.getTransactionQueueManager();
    }
    
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        final Iterator<Integer> it = hstore_site.getLocalPartitionIds().iterator();
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
        columns.add(new VoltTable.ColumnInfo("PARTITION", VoltType.INTEGER));
        
        // Make a dummy profiler just so that we can get the fields from it
        TransactionQueueManagerProfiler profiler = new TransactionQueueManagerProfiler();
        assert(profiler != null);
        
        columns.add(new VoltTable.ColumnInfo("AVG_CONCURRENT", VoltType.FLOAT));
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            String name = pm.getName().toUpperCase();
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
        } // FOR
        
        // ThrottlingQueue
        columns.add(new VoltTable.ColumnInfo("THROTTLED", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("THROTTLED_CNT", VoltType.BIGINT));
        
        // Add in PartitionLockQueueProfiler stats
        PartitionLockQueueProfiler initProfiler = new PartitionLockQueueProfiler();
        columns.add(new VoltTable.ColumnInfo("AVG_TXN_WAIT", VoltType.FLOAT));
        for (ProfileMeasurement pm : initProfiler.queueStates.values()) {
            String name = pm.getName().toUpperCase();
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        int partition = (Integer)rowKey;
        TransactionQueueManager.Debug dbg = this.queue_manager.getDebugContext();
        TransactionQueueManagerProfiler profiler = dbg.getProfiler(partition);
        PartitionLockQueue initQueue = this.queue_manager.getLockQueue(partition);
        PartitionLockQueueProfiler initProfiler = initQueue.getDebugContext().getProfiler();
        
        int offset = this.columnNameToIndex.get("PARTITION");
        rowValues[offset++] = partition;
        rowValues[offset++] = MathUtil.weightedMean(profiler.concurrent_dtxn);
        
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            rowValues[offset++] = pm.getTotalThinkTime();
            rowValues[offset++] = pm.getInvocations();
        } // FOR
        
        // ThrottlingQueue
        ProfileMeasurement throttlePM = initQueue.getThrottleTime();
        rowValues[offset++] = throttlePM.getTotalThinkTime();
        rowValues[offset++] = throttlePM.getInvocations();
        
        // PartitionLockQueue
        rowValues[offset++] = MathUtil.weightedMean(initProfiler.waitTimes);
        for (ProfileMeasurement pm : initProfiler.queueStates.values()) {
            rowValues[offset++] = pm.getTotalThinkTime();
            rowValues[offset++] = pm.getInvocations();
        } // FOR
        
        super.updateStatsRow(rowKey, rowValues);
    }
}
