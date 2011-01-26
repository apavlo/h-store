package org.voltdb;

import java.io.File;
import java.util.List;

import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.MultiPartitionTxnFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestBatchPlannerComplex extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 1;
    private static final int NUM_PARTITIONS = 50;
    private static final int INITIATOR_ID = -1;

    private static Procedure catalog_proc;
    private static Workload workload;

    private Statement catalog_stmt;
    private SQLStmt batch[];
    private ParameterSet args[];

    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);

        if (workload == null) {
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            
            File file = this.getWorkloadFile(ProjectType.TPCC, "100w.large");
            workload = new Workload(catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter to only include multi-partition txns
            // (3) Another limit to stop after allowing ### txns
            // Where is your god now???
            Workload.Filter filter = new ProcedureNameFilter()
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new MultiPartitionTxnFilter(p_estimator))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file.getAbsolutePath(), catalog_db, filter);
            assert(workload.getTransactionCount() > 0);
        }
        
        // Convert the first QueryTrace batch into a SQLStmt+ParameterSet batch
        TransactionTrace txn_trace = workload.getTransactions().get(0);
        assertNotNull(txn_trace);
        List<QueryTrace> query_batch = txn_trace.getQueryBatch(0);
        this.batch = new SQLStmt[query_batch.size()];
        this.args = new ParameterSet[query_batch.size()];
        for (int i = 0; i < this.batch.length; i++) {
            QueryTrace query_trace = query_batch.get(i);
            assertNotNull(query_trace);
            this.batch[i] = new SQLStmt(query_trace.getCatalogItem(catalog_db));
            this.args[i] = VoltProcedure.getCleanParams(this.batch[0], query_trace.getParams());
        } // FOR
    }
    
    /**
     * 
     * @throws Exception
     */
    public void testPlanMultiPartition() throws Exception {
        BatchPlanner batchPlan = new BatchPlanner(batch, this.catalog_proc, p_estimator, INITIATOR_ID);
        BatchPlanner.BatchPlan plan = batchPlan.plan(this.args, 0, false);
        
        
    }
    
}
