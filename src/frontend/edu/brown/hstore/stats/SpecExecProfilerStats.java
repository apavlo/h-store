package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.types.SpeculationType;
import org.voltdb.utils.Pair;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.SpecExecScheduler;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.SpecExecProfiler;
import edu.brown.statistics.HistogramUtil;
import edu.brown.utils.MathUtil;

/**
 * Stats Source for SpecExecProfiler
 * @author pavlo
 */
public class SpecExecProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(SpecExecProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    private final HStoreSite hstore_site;
    
    /**
     * PartitionId+SpeculationType
     */
    private Set<Pair<Integer, SpeculationType>> sortedPair = new TreeSet<Pair<Integer,SpeculationType>>();

    public SpecExecProfilerStats(HStoreSite hstore_site) {
        super(SysProcSelector.SPECEXECPROFILER.name(), false);
        this.hstore_site = hstore_site;
        
        for (int partition: hstore_site.getLocalPartitionIds().values()) {
        	for (SpeculationType type: SpeculationType.values()) {
        	    if (type != SpeculationType.NULL) {
            		Pair<Integer, SpeculationType> pair = new Pair<Integer, SpeculationType>(partition, type);
            		this.sortedPair.add(pair);
        	    }
        	} // FOR
        } // FOR
    }
    
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        //final Iterator<Integer> it = hstore_site.getLocalPartitionIds().iterator();
        final Iterator<Pair<Integer,SpeculationType>> it = sortedPair.iterator();
        
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
        columns.add(new VoltTable.ColumnInfo("SPECULATE_TYPE", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("SUCCESS_CNT", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("SUCCESS_RATE", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("INTERRUPT_CNT", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("INTERRUPT_RATE", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("QUEUE_SIZE_AVG", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("QUEUE_SIZE_STDEV", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("COMPARISONS_AVG", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("COMPARISONS_STDEV", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("MATCHES_AVG", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("MATCHES_STDEV", VoltType.FLOAT));
        columns.add(new VoltTable.ColumnInfo("EXECUTED_PER_TXN_AVG", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo("EXECUTED_PER_TXN_STDEV", VoltType.FLOAT));
        
        // Make a dummy profiler just so that we can get the fields from it
        SpecExecProfiler profiler = new SpecExecProfiler();
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            String name = pm.getName().toUpperCase();
            // We need two columns per ProfileMeasurement
            //  (1) The total think time in nanoseconds
            //  (2) The number of invocations
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_STDEV", VoltType.FLOAT));
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        @SuppressWarnings("unchecked")
        Pair<Integer, SpeculationType> rowKeyPair = (Pair<Integer, SpeculationType>) rowKey;
        Integer partition = rowKeyPair.getFirst();
        SpeculationType specType = rowKeyPair.getSecond();
        PartitionExecutor.Debug executorDebug = hstore_site.getPartitionExecutor(partition).getDebugContext();
        SpecExecScheduler.Debug specExecDebug = executorDebug.getSpecExecScheduler().getDebugContext(); 
        SpecExecProfiler profiler = specExecDebug.getProfiler(specType);
        assert(profiler != null);
        
        int offset = columnNameToIndex.get("PARTITION");
        double total = (double)profiler.total_time.getInvocations();
        rowValues[offset++] = partition;
        rowValues[offset++] = specType.toString();
        rowValues[offset++] = profiler.success;
        rowValues[offset++] = profiler.success / total;
        rowValues[offset++] = profiler.interrupts;
        rowValues[offset++] = profiler.interrupts / total;
        rowValues[offset++] = MathUtil.weightedMean(profiler.queue_size);
        rowValues[offset++] = HistogramUtil.stdev(profiler.queue_size);
        rowValues[offset++] = MathUtil.weightedMean(profiler.num_comparisons);
        rowValues[offset++] = HistogramUtil.stdev(profiler.num_comparisons);
        rowValues[offset++] = MathUtil.weightedMean(profiler.num_matches);
        rowValues[offset++] = HistogramUtil.stdev(profiler.num_matches);
        rowValues[offset++] = MathUtil.weightedMean(profiler.num_executed);
        rowValues[offset++] = HistogramUtil.stdev(profiler.num_executed);
        
        List<Long> history = new ArrayList<Long>();
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            rowValues[offset++] = pm.getTotalThinkTime();
            rowValues[offset++] = pm.getInvocations();
            rowValues[offset++] = MathUtil.stdev(pm.getHistory(history));
            history.clear();
        } // FOR
        super.updateStatsRow(rowKey, rowValues);
    }
}
