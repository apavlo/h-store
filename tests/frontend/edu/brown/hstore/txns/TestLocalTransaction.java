package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.ProjectType;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.MockPartitionExecutor;
import edu.brown.hstore.MockHStoreSite;

/**
 * TestLocalTransaction
 * @author pavlo
 */
public class TestLocalTransaction extends BaseTestCase {

    static final int NUM_PARTITIONS = 10;
    static final int BASE_PARTITION = 0;
    static final long UNDO_TOKEN = 99999;
    static final long TXN_ID = 10000;
    static final long CLIENT_HANDLE = Integer.MAX_VALUE;
    
    static final Class<? extends VoltProcedure> TARGET_PROC = neworder.class;
    static final String TARGET_STMTS[] = {
        "updateStock",
        "createOrderLine"
    };
    static final int TARGET_STMTS_W_IDS[][] = { // ParameterSet offset
        { 5 }, 
        { 2, 5 },
    };
    static final int TARGET_REPEAT = 14;
    
    MockHStoreSite hstore_site;
    MockPartitionExecutor executor;
    DependencyTracker depTracker;
    Procedure catalog_proc;
    LocalTransaction ts;
    AbstractTransaction.Debug tsDebug;
    SQLStmt batchStmts[];
    ParameterSet batchParams[];
    int batchCtrs[];
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        this.catalog_proc = this.getProcedure(TARGET_PROC);
        Statement catalog_stmts[] = new Statement[TARGET_STMTS.length];
        for (int i = 0; i < catalog_stmts.length; i++) {
            catalog_stmts[i] = this.getStatement(catalog_proc, TARGET_STMTS[i]);
        } // FOR
        
        this.batchStmts = new SQLStmt[TARGET_REPEAT * TARGET_STMTS.length];
        this.batchParams = new ParameterSet[this.batchStmts.length];
        this.batchCtrs = new int[this.batchStmts.length];
        for (int i = 0; i < this.batchStmts.length; ) {
            for (int j = 0; j < catalog_stmts.length; j++) {
                this.batchStmts[i] = new SQLStmt(catalog_stmts[j]);
                this.batchCtrs[i] = i;
                
                // Generate random input parameters for the Statement but make sure that
                // the W_IDs always point to our BASE_PARTITION
                Object params[] = this.randomStatementParameters(catalog_stmts[j]);
                for (int k = 0; k < TARGET_STMTS_W_IDS[j].length; k++) {
                    int idx = TARGET_STMTS_W_IDS[j][k];
                    params[idx] = BASE_PARTITION;
                } // FOR
                this.batchParams[i] = new ParameterSet(params);
                i += 1;
            } // FOR
        } // FOR
        
        for (int i = 0; i < this.batchStmts.length; i++) {
            assertNotNull(this.batchStmts[i]);
            assertNotNull(this.batchParams[i]);
        } // FOR
        
        this.hstore_site = new MockHStoreSite(0, catalogContext, HStoreConf.singleton());
        this.executor = (MockPartitionExecutor)this.hstore_site.getPartitionExecutor(BASE_PARTITION);
        this.depTracker = this.executor.getDependencyTracker();
        assertNotNull(this.depTracker);
        
        this.ts = new LocalTransaction(this.hstore_site);
        this.ts.testInit(TXN_ID,
                         BASE_PARTITION,
                         null,
                         catalogContext.getAllPartitionIds(),
                         this.catalog_proc);
        this.tsDebug = this.ts.getDebugContext();
        this.depTracker.addTransaction(this.ts);
    }
    
    /**
     * testStartRound
     */
    public void testStartRound() throws Exception {
        this.ts.markControlCodeExecuted();
        this.ts.initFirstRound(UNDO_TOKEN, this.batchStmts.length);
        
        // We need to get all of our WorkFragments for this batch
        BatchPlanner planner = new BatchPlanner(this.batchStmts, this.catalog_proc, p_estimator);
        BatchPlanner.BatchPlan plan = planner.plan(TXN_ID,
                                                   BASE_PARTITION,
                                                   ts.getPredictTouchedPartitions(),
                                                   ts.getTouchedPartitions(),
                                                   this.batchParams);
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
        
        List<WorkFragment.Builder> builders = new ArrayList<WorkFragment.Builder>();
        plan.getWorkFragmentsBuilders(TXN_ID, this.batchCtrs, builders);
        assertFalse(builders.isEmpty());
        
        List<WorkFragment.Builder> ready = new ArrayList<WorkFragment.Builder>();
        for (WorkFragment.Builder builder : builders) {
            boolean blocked = (this.depTracker.addWorkFragment(this.ts, builder, this.batchParams) == false);
            if (blocked == false) {
                assertFalse(builder.toString(), ready.contains(builder));
                ready.add(builder);
            }
        } // FOR
        assertFalse(ready.isEmpty());
        
        this.ts.startRound(BASE_PARTITION);
    }
    
    /**
     * testReadWriteSets
     */
    public void testReadWriteSets() throws Exception {
        this.ts.markControlCodeExecuted();
        this.ts.initFirstRound(UNDO_TOKEN, this.batchStmts.length);
        
        int tableIds[] = null;
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            this.tsDebug.clearReadWriteSets();
            for (PlanFragment catalog_frag : catalog_stmt.getFragments()) {
//                System.err.println(catalog_frag.fullName());
                
                tableIds = catalogContext.getReadTableIds(Long.valueOf(catalog_frag.getId()));
                if (tableIds != null) {
                    ts.markTableIdsRead(BASE_PARTITION, tableIds);
//                    System.err.printf("*** %s -- READ:%s\n",
//                                      catalog_frag, Arrays.toString(tableIds));
                }
                
                tableIds = catalogContext.getWriteTableIds(Long.valueOf(catalog_frag.getId()));
                if (tableIds != null) {
                    ts.markTableIdsWritten(BASE_PARTITION, tableIds);
//                    System.err.printf("*** %s -- WRITE:%s\n",
//                                      catalog_frag, Arrays.toString(tableIds));
                }
            } // FOR

            Set<Table> readTables = new HashSet<Table>();
            Set<Integer> readTableIds = new HashSet<Integer>();
            Set<Table> writeTables = new HashSet<Table>();
            Set<Integer> writeTableIds = new HashSet<Integer>();
            for (Table catalog_tbl : CatalogUtil.getReferencedTables(catalog_stmt)) {
                if (catalog_stmt.getReadonly()) {
                    readTables.add(catalog_tbl);
                    readTableIds.add(catalog_tbl.getRelativeIndex());
                    assertTrue(catalog_tbl.toString(), ts.isTableRead(BASE_PARTITION, catalog_tbl));
                    assertFalse(catalog_tbl.toString(), ts.isTableWritten(BASE_PARTITION, catalog_tbl));
                } else {
                    writeTables.add(catalog_tbl);
                    writeTableIds.add(catalog_tbl.getRelativeIndex());
                    assertFalse(catalog_tbl.toString(), ts.isTableRead(BASE_PARTITION, catalog_tbl));
                    assertTrue(catalog_tbl.toString(), ts.isTableWritten(BASE_PARTITION, catalog_tbl));
                }
            } // FOR
//            System.err.printf("%s -- READ:%s / WRITE:%s\n",
//                              catalog_stmt, readTableIds, writeTableIds);
            
            int readIds[] = ts.getTableIdsMarkedRead(BASE_PARTITION);
            assertEquals(readTables.size(), readIds.length);
            for (int tableId : readIds) {
                Table tbl = catalogContext.getTableById(tableId);
                assertNotNull(tbl);
                assertTrue(catalog_stmt.fullName()+"->"+tbl.toString(), readTables.contains(tbl));
            } // FOR
            
            int writeIds[] = ts.getTableIdsMarkedWritten(BASE_PARTITION);
            assertEquals(writeTables.size(), writeIds.length);
            for (int tableId : writeIds) {
                Table tbl = catalogContext.getTableById(tableId);
                assertNotNull(tbl);
                assertTrue(catalog_stmt.fullName()+"->"+tbl.toString(), writeTables.contains(tbl));
            } // FOR
            
//            System.err.println(StringUtil.repeat("-", 100));
        } // FOR (stmt)
    }
    
}
