package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.BatchPlannerProfiler;
import edu.brown.profilers.ProfileMeasurement;

public class BatchPlannerProfilerStats extends StatsSource {
    private static final Logger LOG = Logger.getLogger(BatchPlannerProfilerStats.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final HStoreSite hstore_site;
    private final CatalogContext catalogContext;

    public BatchPlannerProfilerStats(HStoreSite hstore_site, CatalogContext catalogContext) {
        super(SysProcSelector.PLANNERPROFILER.name(), false);
        this.hstore_site = hstore_site;
        this.catalogContext = catalogContext;
    }
    
    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        final Iterator<Procedure> it = this.catalogContext.procedures.iterator();
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
        
        BatchPlannerProfiler profiler = new BatchPlannerProfiler();
        for (ProfileMeasurement pm : profiler.getProfileMeasurements()) {
            String name = pm.getType().toUpperCase();
            columns.add(new VoltTable.ColumnInfo(name, VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo(name+"_CNT", VoltType.BIGINT));
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        Procedure proc = (Procedure)rowKey;
        if (debug.get()) LOG.debug("Collecting BatchPlanner stats for " + proc.getName());
        
        // We're going to create a new profiler that we can use
        // to add up all of the values from the individual BatchPlannerProfilers
        BatchPlannerProfiler total = new BatchPlannerProfiler();
        ProfileMeasurement totalPMs[] = total.getProfileMeasurements();
        
        // Find all of the BatchPlanners for each partition for our target procedure
        for (BatchPlanner planner : this.getBatchPlanners(proc)) {
            BatchPlannerProfiler profiler = planner.getProfiler();
            if (profiler == null) continue;
            
            ProfileMeasurement profilerPMs[] = profiler.getProfileMeasurements();
            assert(totalPMs.length == profilerPMs.length);
            for (int i = 0; i < totalPMs.length; i++) {
                totalPMs[i].appendTime(profilerPMs[i]);
            } // FOR
            
            total.transactions.addAndGet(profiler.transactions.get());
        } // FOR
        
        int offset = this.columnNameToIndex.get("PROCEDURE");
        rowValues[offset++] = proc.getName();
        rowValues[offset++] = total.transactions.get();
        for (ProfileMeasurement pm : totalPMs) {
            rowValues[offset++] = pm.getTotalThinkTime();
            rowValues[offset++] = pm.getInvocations();
        } // FOR

        super.updateStatsRow(rowKey, rowValues);
    }
    
    private Collection<BatchPlanner> getBatchPlanners(Procedure proc) {
        Set<BatchPlanner> planners = new HashSet<BatchPlanner>();
        for (int partition : hstore_site.getLocalPartitionIds().values()) {
            PartitionExecutor executor = hstore_site.getPartitionExecutor(partition);
            PartitionExecutor.Debug executorDebug = executor.getDebugContext();
            for (BatchPlanner planner : executorDebug.getBatchPlanners()) {
                if (planner.getProcedure().equals(proc)) {
                    planners.add(planner);
                }
            } // FOR
        } // FOR
        return (planners);
    }
}
