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
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.MarkovEstimatorProfiler;
import edu.brown.profilers.ProfileMeasurement;

public class MarkovEstimatorProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(MarkovEstimatorProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final HStoreSite hstore_site;

    public MarkovEstimatorProfilerStats(HStoreSite hstore_site) {
        super(SysProcSelector.MARKOVPROFILER.name(), false);
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
        
        // Make a dummy profiler just so that we can get the fields from it
        MarkovEstimatorProfiler profiler = new MarkovEstimatorProfiler();
        assert(profiler != null);
        
        columns.add(new VoltTable.ColumnInfo("PARTITION", VoltType.INTEGER));
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            String name = pm.getName().toUpperCase();
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
        TransactionEstimator est = hstore_site.getPartitionExecutor(partition).getTransactionEstimator();
        assert(est != null) : "Unexpected null TransactionEstimator for partition " + partition;
        MarkovEstimator.Debug dbg = ((MarkovEstimator)est).getDebugContext();
        assert(dbg != null);
        MarkovEstimatorProfiler profiler = dbg.getProfiler();
        assert(profiler != null);
        
        int offset = columnNameToIndex.get("PARTITION");
        rowValues[offset++] = partition;
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            rowValues[offset++] = pm.getTotalThinkTime();
            rowValues[offset++] = pm.getInvocations();
        } // FOR
        super.updateStatsRow(rowKey, rowValues);
    }
}
