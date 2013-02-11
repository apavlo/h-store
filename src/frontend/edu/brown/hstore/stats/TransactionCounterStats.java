package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.voltdb.CatalogContext;
import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.util.TransactionCounter;

public class TransactionCounterStats extends StatsSource {
    
    private static final Set<TransactionCounter> COUNTER_EXCLUDE = new HashSet<TransactionCounter>();
    static {
        COUNTER_EXCLUDE.add(TransactionCounter.SYSPROCS);
//        COUNTER_EXCLUDE.add(TransactionCounter.BLOCKED_LOCAL);
//        COUNTER_EXCLUDE.add(TransactionCounter.BLOCKED_REMOTE);
    }
    
    private static class ProcedureRow {
        final Map<TransactionCounter, Long> data = new LinkedHashMap<TransactionCounter, Long>();
        boolean has_values = false;
        
        public ProcedureRow(Procedure catalog_proc) {
            for (TransactionCounter tc : TransactionCounter.values()) {
                if (COUNTER_EXCLUDE.contains(tc)) continue;
                Long cnt = tc.get(catalog_proc);
                if (cnt != null) {
                    this.data.put(tc, cnt);
                    this.has_values = true;
                } else {
                    this.data.put(tc, 0l);
                }
            } // FOR
        }
    }
    
    private final CatalogContext catalogContext;
    private final List<Procedure> procedures;

    public TransactionCounterStats(CatalogContext catalogContext) {
        super(SysProcSelector.TXNCOUNTER.name(), false);
        this.catalogContext = catalogContext;
        
        // We'll put the sysprocs first
        this.procedures = new ArrayList<Procedure>(this.catalogContext.getSysProcedures());
        for (Procedure proc : catalogContext.procedures) {
            if (proc.getSystemproc()) continue;
            this.procedures.add(proc);
        } // FOR
        
        // Additional stuff to exclude
//        HStoreConf hstore_conf = HStoreConf.singleton();
//        if (hstore_conf.site.anticache_enable == false) {
//            COUNTER_EXCLUDE.add(TransactionCounter.EVICTEDACCESS);
//        }
//        if (hstore_conf.site.exec_prefetch_queries == false) {
//            COUNTER_EXCLUDE.add(TransactionCounter.PREFETCH_LOCAL);
//            COUNTER_EXCLUDE.add(TransactionCounter.PREFETCH_REMOTE);
//        }
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        // HACK: Figure out what procedures actually have results
        List<Object> hasResults = new ArrayList<Object>(TransactionCounter.getAllProcedures(this.catalogContext));
        return hasResults.iterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
        for (TransactionCounter tc : TransactionCounter.values()) {
            if (COUNTER_EXCLUDE.contains(tc)) continue;
            columns.add(new VoltTable.ColumnInfo(tc.name().toUpperCase(), VoltType.INTEGER));
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        // sum up all of the site statistics
        Procedure proc = (Procedure)rowKey;
        ProcedureRow totals = new TransactionCounterStats.ProcedureRow(proc);
        
        if (totals.has_values) {
            rowValues[columnNameToIndex.get("PROCEDURE")] = proc.getName();
            for (Entry<TransactionCounter, Long> e : totals.data.entrySet()) {
                Long v = e.getValue();
                if (v == null) v = Long.valueOf(0);
                rowValues[columnNameToIndex.get(e.getKey().name().toUpperCase())] = v.intValue();
            } // FOR
            super.updateStatsRow(rowKey, rowValues);
        }
    }
}
