package edu.brown.hstore.specexec.checkers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;

/**
 * OCC Conflict Checker
 * This relies on the EE to generate the txn's read/write tracking sets 
 * @author pavlo
 */
public class OptimisticConflictChecker extends AbstractConflictChecker {
    private static final Logger LOG = Logger.getLogger(OptimisticConflictChecker.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected static final int READ = 0;
    protected static final int WRITE = 1;
    
    private final ExecutionEngine ee;
    
    public OptimisticConflictChecker(CatalogContext catalogContext, ExecutionEngine ee) {
        super(catalogContext);
        this.ee = ee;
    }

    @Override
    public boolean shouldIgnoreTransaction(AbstractTransaction ts) {
        if (ts instanceof LocalTransaction) {
            return (((LocalTransaction)ts).getRestartCounter() > 0);
        }
        return (false);
    }
    
    @Override
    public boolean hasConflictBefore(AbstractTransaction ts0, LocalTransaction ts1, int partitionId) {
        return (false);
    }

    @Override
    public boolean hasConflictAfter(AbstractTransaction ts0, LocalTransaction ts1, int partitionId) {
        assert(ts0.isInitialized()) :
            String.format("Uninitialized distributed transaction handle [%s]", ts0);
        assert(ts1.isInitialized()) :
            String.format("Uninitialized speculative transaction handle [%s]", ts1);
        
        // Get the READ/WRITE tracking sets from the EE
        VoltTable tsTracking0[] = this.getReadWriteSets(ts0);
        if (trace.val)
            LOG.trace(String.format("%s READ/WRITE SETS:\n%s", ts0, VoltTableUtil.format(tsTracking0)));
        VoltTable tsTracking1[] = this.getReadWriteSets(ts1);
        if (trace.val)
            LOG.trace(String.format("%s READ/WRITE SETS:\n%s", ts1, VoltTableUtil.format(tsTracking1)));
        
        // SPECIAL CASE
        // Either txn did not actually read or write anything at this partition, so we can just
        // say that everything kosher.
        if ((tsTracking0[READ] == null || tsTracking0[READ].getRowCount() == 0) && 
            (tsTracking0[WRITE] == null || tsTracking0[WRITE].getRowCount() == 0)) {
            // SANITY CHECK
            assert((tsTracking0[READ] == null || tsTracking0[READ].getRowCount() == 0) &&
                   (ts0.getTableIdsMarkedRead(partitionId).length == 0)) :
                String.format("%s READ Set: %s\nTables Read: %s",
                              ts0, tsTracking0[READ],
                              Arrays.toString(ts0.getTableIdsMarkedRead(partitionId)));
            assert((tsTracking0[WRITE] == null || tsTracking0[WRITE].getRowCount() == 0) &&
                   (ts0.getTableIdsMarkedWritten(partitionId).length == 0)) :
                 String.format("%s WRITE Set: %s\nTables Written: %s",
                               ts0, tsTracking0[WRITE],
                               Arrays.toString(ts0.getTableIdsMarkedWritten(partitionId)));
            return (false);
        }
        else if ((tsTracking1[READ] == null || tsTracking1[READ].getRowCount() == 0) && 
                (tsTracking1[WRITE] == null || tsTracking1[WRITE].getRowCount() == 0)) {
            // SANITY CHECK
            assert((tsTracking1[READ] == null || tsTracking1[READ].getRowCount() == 0) &&
                   (ts1.getTableIdsMarkedRead(partitionId).length == 0)) :
                 String.format("%s READ Set: %s\nTables Read: %s",
                               ts1, tsTracking1[READ],
                               Arrays.toString(ts1.getTableIdsMarkedRead(partitionId)));
             assert((tsTracking1[WRITE] == null || tsTracking1[WRITE].getRowCount() == 0) &&
                    (ts1.getTableIdsMarkedWritten(partitionId).length == 0)) :
                  String.format("%s WRITE Set: %s\nTables Written: %s",
                                ts1, tsTracking1[WRITE],
                                Arrays.toString(ts1.getTableIdsMarkedWritten(partitionId)));
            return (false);
        }

        int tableIds[] = null;
        
        // READ-WRITE CONFLICTS
        tableIds = ts0.getTableIdsMarkedRead(partitionId);
        if (this.hasTupleConflict(partitionId, tableIds, ts0, tsTracking0[READ], ts1, tsTracking1[WRITE])) {
            if (debug.val)
                LOG.debug(String.format("Found READ-WRITE conflict between %s and %s", ts0, ts1));
            return (true);
        }
        
        // WRITE-WRITE CONFLICTS
        tableIds = ts0.getTableIdsMarkedWritten(partitionId);
        if (this.hasTupleConflict(partitionId, tableIds, ts0, tsTracking0[WRITE], ts1, tsTracking1[WRITE])) {
            if (debug.val)
                LOG.debug(String.format("Found WRITE-WRITE conflict between %s and %s", ts0, ts1));
            return (true);
        }
    
        return (false);
    }
    
    /**
     * Returns true if there is a conflict between the two transactions for the
     * given list of tableIds based on their tracking sets.
     * @param partition
     * @param tableIds
     * @param ts0
     * @param tsTracking0
     * @param ts1
     * @param tsTracking1
     * @return
     */
    protected boolean hasTupleConflict(int partition, int tableIds[],
                                       AbstractTransaction ts0, VoltTable tsTracking0,
                                       AbstractTransaction ts1, VoltTable tsTracking1) {
        
        // Check whether the first transaction accessed the same tuple by the second txn
        Set<Integer> tupleIds0 = new HashSet<Integer>();
        Set<Integer> tupleIds1 = new HashSet<Integer>();
        
        if (trace.val)
            LOG.trace("\n"+
                      StringUtil.columns(ts0+"\n"+VoltTableUtil.format(tsTracking0),
                                         ts1+"\n"+VoltTableUtil.format(tsTracking1)));
        
        for (int tableId : tableIds) {
            Table targetTbl = catalogContext.getTableById(tableId);
            
            // Skip if we know that the other transaction hasn't done anything
            // to the table at this partition.
            if (ts0.isTableReadOrWritten(partition, targetTbl) == false) {
                continue;
            }
            
            tupleIds0.clear();
            this.getTupleIds(targetTbl.getName(), tsTracking0, tupleIds0);
            tupleIds1.clear();
            this.getTupleIds(targetTbl.getName(), tsTracking1, tupleIds1);
            
            if (debug.val) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                m.put(targetTbl.getName() + " TRACKING SETS", null);
                m.put(ts0.toString(), tupleIds0);
                m.put(ts1.toString(), tupleIds1);
                LOG.debug(StringUtil.formatMaps(m));
            }
            
            if (CollectionUtils.containsAny(tupleIds0, tupleIds1)) {
                if (debug.val)
                    LOG.debug(String.format("Found conflict in %s between %s and %s",
                              targetTbl, ts0, ts1));
                return (true);
            }
            
        } // FOR
        return (false);
    }
    
    protected void getTupleIds(String targetTableName, VoltTable tsTracking, Collection<Integer> tupleIds) {
        tsTracking.resetRowPosition();
        while (tsTracking.advanceRow()) {
            String tableName = tsTracking.getString(0);
            if (targetTableName.equalsIgnoreCase(tableName)) {
                tupleIds.add((int)tsTracking.getLong(1));
            }
        } // WHILE
    }
    
    protected VoltTable[] getReadWriteSets(AbstractTransaction ts) {
        VoltTable readSet, writeSet;
        try {
            readSet = this.ee.trackingReadSet(ts.getTransactionId());
            writeSet = this.ee.trackingWriteSet(ts.getTransactionId());
        } catch (Exception ex) {
            String msg = String.format("Failed to get read/write tracking set for %s", ts);
            throw new RuntimeException(msg, ex);
        }
        return (new VoltTable[]{ readSet, writeSet });
    }
    
    
}
