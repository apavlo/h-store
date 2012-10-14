package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.List;

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
import edu.brown.hstore.txns.ExecutionState;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.PartitionSet;
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
    Procedure catalog_proc;
    LocalTransaction ts;
    SQLStmt batchStmts[];
    ParameterSet batchParams[];
    
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
        this.batchParams = new ParameterSet[TARGET_REPEAT * 2];
        for (int i = 0; i < this.batchStmts.length; ) {
            for (int j = 0; j < catalog_stmts.length; j++) {
                this.batchStmts[i] = new SQLStmt(catalog_stmts[j]);
                
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
        assertNotNull(this.executor);
        this.ts = new LocalTransaction(this.hstore_site);
    }
    
    /**
     * testStartRound
     */
    public void testStartRound() throws Exception {
        this.ts.testInit(TXN_ID, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), this.catalog_proc);
        ExecutionState state = new ExecutionState(this.executor);
        this.ts.setExecutionState(state);
        this.ts.initRound(BASE_PARTITION, UNDO_TOKEN);
        this.ts.setBatchSize(this.batchStmts.length);
        
        // We need to get all of our WorkFragments for this batch
        BatchPlanner planner = new BatchPlanner(this.batchStmts, this.catalog_proc, p_estimator);
        BatchPlanner.BatchPlan plan = planner.plan(TXN_ID,
                                                   CLIENT_HANDLE,
                                                   BASE_PARTITION,
                                                   ts.getPredictTouchedPartitions(),
                                                   false,
                                                   ts.getTouchedPartitions(),
                                                   this.batchParams);
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
        
        List<WorkFragment> fragments = new ArrayList<WorkFragment>();
        plan.getWorkFragments(TXN_ID, fragments);
        assertFalse(fragments.isEmpty());
        
        List<WorkFragment> ready = new ArrayList<WorkFragment>();
        for (WorkFragment pf : fragments) {
            boolean blocked = this.ts.addWorkFragment(pf);
            if (blocked == false) {
                assertFalse(pf.toString(), ready.contains(pf));
                ready.add(pf);
            }
        } // FOR
        assertFalse(ready.isEmpty());
        
        this.ts.startRound(BASE_PARTITION);
    }
    
    /**
     * testReadWriteSets
     */
    public void testReadWriteSets() throws Exception {
        ExecutionState state = new ExecutionState(this.executor);
        this.ts.setExecutionState(state);
        this.ts.fastInitRound(BASE_PARTITION, UNDO_TOKEN);
        
        int tableIds[] = null;
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            this.ts.clearReadWriteSets();
            for (PlanFragment catalog_frag : catalog_stmt.getFragments()) {
                tableIds = catalogContext.getReadTableIds(Long.valueOf(catalog_frag.getId()));
                ts.markTableIdsAsRead(BASE_PARTITION, tableIds);
                
                tableIds = catalogContext.getWriteTableIds(Long.valueOf(catalog_frag.getId()));
                ts.markTableIdsAsWritten(BASE_PARTITION, tableIds);
            } // FOR

            for (Table catalog_tbl : CatalogUtil.getAllTables(catalog_stmt)) {
                if (catalog_stmt.getReadonly()) {
                    assertTrue(catalog_tbl.toString(), ts.isTableRead(BASE_PARTITION, catalog_tbl));
                    assertFalse(catalog_tbl.toString(), ts.isTableWritten(BASE_PARTITION, catalog_tbl));
                } else {
                    assertFalse(catalog_tbl.toString(), ts.isTableRead(BASE_PARTITION, catalog_tbl));
                    assertTrue(catalog_tbl.toString(), ts.isTableWritten(BASE_PARTITION, catalog_tbl));
                }
            } // FOR
        } // FOR
    }
    
}
