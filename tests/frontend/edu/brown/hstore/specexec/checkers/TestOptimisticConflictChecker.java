/**
 * 
 */
package edu.brown.hstore.specexec.checkers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.TestReadWriteTracking;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * OCC Conflict Checker
 * @author pavlo
 */
public class TestOptimisticConflictChecker extends BaseTestCase {

    private static final int NUM_PARTITONS = 2;
    private static final int BASE_PARTITION = 0;
    private static long NEXT_TXN_ID = 1000;

    private HStoreSite hstore_site;
    private OptimisticConflictChecker checker;
    private Map<AbstractTransaction, VoltTable[]> readWriteSets = new HashMap<AbstractTransaction, VoltTable[]>();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITONS);

        this.checker = new OptimisticConflictChecker(catalogContext, null) {
            @Override
            protected VoltTable[] getReadWriteSets(AbstractTransaction ts) {
                return readWriteSets.get(ts);
            }
        };
        this.hstore_site = new MockHStoreSite(0, catalogContext, HStoreConf.singleton());
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void updateTracking(AbstractTransaction ts, Table catalog_tbl, boolean isRead, int...tupleIds) {
        VoltTable vts[] = this.readWriteSets.get(ts);
        if (vts == null) {
            vts = new VoltTable[]{
                        new VoltTable(TestReadWriteTracking.RESULT_COLS),
                        new VoltTable(TestReadWriteTracking.RESULT_COLS)
            };
            this.readWriteSets.put(ts, vts);
        }

        VoltTable vt = null;
        if (isRead) {
            vt = vts[OptimisticConflictChecker.READ];
            ts.markTableRead(BASE_PARTITION, catalog_tbl);
        }
        else {
            vt = vts[OptimisticConflictChecker.WRITE];
            ts.markTableWritten(BASE_PARTITION, catalog_tbl);
        }
        for (int tupleId : tupleIds) {
            vt.addRow(catalog_tbl.getName(), tupleId);
        } // FOR
    }
    
    private void addReads(AbstractTransaction ts, Table tbl, int...tupleIds) {
        this.updateTracking(ts, tbl, true, tupleIds);
    }
    private void addWrites(AbstractTransaction ts, Table tbl, int...tupleIds) {
        this.updateTracking(ts, tbl, false, tupleIds);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testReadWriteConflicts
     */
    @Test
    public void testReadWriteConflicts() throws Exception {
        Procedure proc = this.getProcedure(neworder.class);
        Collection<Table> tables = CatalogUtil.getReferencedTables(proc);
        
        LocalTransaction ts0 = new LocalTransaction(this.hstore_site);
        ts0.testInit(NEXT_TXN_ID++,
                     BASE_PARTITION,
                     catalogContext.getPartitionSetSingleton(BASE_PARTITION),
                     proc,
                     new Object[0]);
        
        LocalTransaction ts1 = new LocalTransaction(this.hstore_site);
        ts1.testInit(NEXT_TXN_ID++,
                     BASE_PARTITION,
                     catalogContext.getPartitionSetSingleton(BASE_PARTITION),
                     proc,
                     new Object[0]);
        
        // Let both txns read all of the tables referenced in the procedure
        // The checker should say that there isn't a conflict
        for (Table tbl : tables) {
            this.addReads(ts0, tbl, 1111);
            this.addReads(ts1, tbl, 1111);
        }
        assertFalse(this.checker.hasConflictBefore(ts0, ts1, BASE_PARTITION));
        assertFalse(this.checker.hasConflictAfter(ts0, ts1, BASE_PARTITION));
        
        // Now if we have the second txn write to the tables, but target different
        // tupleIds, then it still should say that there isn't a conflict.
        for (Table tbl : tables) {
            this.addReads(ts1, tbl, 2222);
        }
        assertFalse(this.checker.hasConflictBefore(ts0, ts1, BASE_PARTITION));
        assertFalse(this.checker.hasConflictAfter(ts0, ts1, BASE_PARTITION));
        
        // Finally, we'll pick a random table to have the second txn write to.
        // That should get the checker to mark them as conflicting
        Table tbl = CollectionUtil.random(tables);
        assertNotNull(tbl);
        this.addWrites(ts1, tbl, 1111);
        assertFalse(this.checker.hasConflictBefore(ts0, ts1, BASE_PARTITION));
        assertTrue(this.checker.hasConflictAfter(ts0, ts1, BASE_PARTITION));
    }
    
    /**
     * testWriteWriteConflicts
     */
    @Test
    public void testWriteWriteConflicts() throws Exception {
        Procedure proc = this.getProcedure(neworder.class);
        Collection<Table> tables = CatalogUtil.getReferencedTables(proc);
        
        LocalTransaction ts0 = new LocalTransaction(this.hstore_site);
        ts0.testInit(NEXT_TXN_ID++,
                     BASE_PARTITION,
                     catalogContext.getPartitionSetSingleton(BASE_PARTITION),
                     proc,
                     new Object[0]);
        
        LocalTransaction ts1 = new LocalTransaction(this.hstore_site);
        ts1.testInit(NEXT_TXN_ID++,
                     BASE_PARTITION,
                     catalogContext.getPartitionSetSingleton(BASE_PARTITION),
                     proc,
                     new Object[0]);
        
        // Let both the txns write to different tuples at the same tables
        // The checker should say that there isn't a conflict
        for (Table tbl : tables) {
            this.addWrites(ts0, tbl, 1111);
            this.addWrites(ts1, tbl, 2222);
        }
        assertFalse(this.checker.hasConflictBefore(ts0, ts1, BASE_PARTITION));
        assertFalse(this.checker.hasConflictAfter(ts0, ts1, BASE_PARTITION));
        
        // Finally, we'll pick a random table to have the second txn write to.
        // That should get the checker to mark them as conflicting
        Table tbl = CollectionUtil.random(tables);
        assertNotNull(tbl);
        this.addWrites(ts1, tbl, 1111);
        assertFalse(this.checker.hasConflictBefore(ts0, ts1, BASE_PARTITION));
        assertTrue(this.checker.hasConflictAfter(ts0, ts1, BASE_PARTITION));
    }
    
}
