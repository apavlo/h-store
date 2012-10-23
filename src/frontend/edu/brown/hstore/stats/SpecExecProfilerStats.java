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
import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.SpecExecProfiler;
import edu.brown.utils.MathUtil;

public class SpecExecProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(SpecExecProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final HStoreSite hstore_site;

    public SpecExecProfilerStats(HStoreSite hstore_site) {
        super(SysProcSelector.SPECEXECPROFILER.name(), false);
        this.hstore_site = hstore_site;
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
        columns.add(new VoltTable.ColumnInfo("QUEUE_SIZE_AVG", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("QUEUE_SIZE_STDEV", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("COMPARISONS_AVG", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("COMPARISONS_STDEV", VoltType.FLOAT));

        // Make a dummy profiler just so that we can get the fields from it
        SpecExecProfiler profiler = new SpecExecProfiler();
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            String name = pm.getType().toUpperCase();
            // We need two columns per ProfileMeasurement
            //  (1) The total think time in nanoseconds
            //  (2) The number of invocations
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        Integer partition = (Integer)rowKey;
        PartitionExecutor.Debug dbg = hstore_site.getPartitionExecutor(partition).getDebugContext();
        SpecExecProfiler profiler = dbg.getSpecExecScheduler().getProfiler();
        assert(profiler != null);
        
        int offset = columnNameToIndex.get("PARTITION");
        rowValues[offset++] = partition;
        rowValues[offset++] = MathUtil.weightedMean(profiler.queue_size);
        rowValues[offset++] = MathUtil.stdev(profiler.queue_size);
        rowValues[offset++] = MathUtil.weightedMean(profiler.num_comparisons);
        rowValues[offset++] = MathUtil.stdev(profiler.num_comparisons);
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            rowValues[offset++] = pm.getTotalThinkTime();
            rowValues[offset++] = pm.getInvocations();
        } // FOR
        super.updateStatsRow(rowKey, rowValues);
    }
}
