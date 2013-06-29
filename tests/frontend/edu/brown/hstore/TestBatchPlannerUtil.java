package edu.brown.hstore;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.voltdb.BackendTarget;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.MultiPartitionTxnFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestBatchPlannerUtil extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int TARGET_BATCH = 1;
    private static final int WORKLOAD_XACT_LIMIT = 1;
    private static final int NUM_PARTITIONS = 50;
    private static final int BASE_PARTITION = 0;
    private static final long TXN_ID = 123l;

    private static Procedure catalog_proc;
    private static Workload workload;
    private static PartitionSet all_partitions;

    private static SQLStmt batch[][];
    private static ParameterSet args[][];
    private static List<QueryTrace> query_batch[];
    private static TransactionTrace txn_trace;
    
    private MockPartitionExecutor executor;
    private FastIntHistogram touched_partitions = new FastIntHistogram(NUM_PARTITIONS);

    @Override
    @SuppressWarnings("unchecked")
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);

        if (isFirstSetup()) {
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            all_partitions = catalogContext.getAllPartitionIds();
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalogContext.catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter to only include multi-partition txns
            // (3) Another limit to stop after allowing ### txns
            // Where is your god now???
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new MultiPartitionTxnFilter(p_estimator, false))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file, catalogContext.database, filter);
            assert(workload.getTransactionCount() > 0);
            
            // Convert the first QueryTrace batch into a SQLStmt+ParameterSet batch
            txn_trace = CollectionUtil.first(workload.getTransactions());
            assertNotNull(txn_trace);
            int num_batches = txn_trace.getBatchCount();
            query_batch = (List<QueryTrace>[])new List<?>[num_batches];
            batch = new SQLStmt[num_batches][];
            args = new ParameterSet[num_batches][];
            
            for (int i = 0; i < query_batch.length; i++) {
                query_batch[i] = txn_trace.getBatchQueries(i);
                batch[i] = new SQLStmt[query_batch[i].size()];
                args[i] = new ParameterSet[query_batch[i].size()];
                for (int ii = 0; ii < batch[i].length; ii++) {
                    QueryTrace query_trace = query_batch[i].get(ii);
                    assertNotNull(query_trace);
                    batch[i][ii] = new SQLStmt(query_trace.getCatalogItem(catalogContext.database));
                    args[i][ii] = VoltProcedure.getCleanParams(batch[i][ii], query_trace.getParams());
                } // FOR
            } // FOR
        }
        
        VoltProcedure volt_proc = ClassUtil.newInstance(TARGET_PROCEDURE, new Object[0], new Class<?>[0]);
        assert(volt_proc != null);
        this.executor = new MockPartitionExecutor(BASE_PARTITION, catalogContext, p_estimator);
        volt_proc.init(this.executor, catalog_proc, BackendTarget.NONE);
    }
    
    /**
     * testBatchHashCode
     */
    public void testBatchHashCode() throws Exception {
        final List<SQLStmt> statements = new ArrayList<SQLStmt>();
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            statements.add(new SQLStmt(catalog_stmt));
            statements.add(new SQLStmt(catalog_stmt));
            statements.add(new SQLStmt(catalog_stmt));
        } // FOR
        int num_stmts = statements.size();
        assert(num_stmts > 0);

        Random rand = new Random();
        for (int x = 0; x < 100; x++) {
        Map<Integer, BatchPlanner> batchPlanners = new HashMap<Integer, BatchPlanner>(100);
        SQLStmt batches[][] = new SQLStmt[10][];
        int hashes[] = new int[batches.length];
        for (int i = 0; i < batches.length; i++) {
            int batch_size = i + 1; 
            batches[i] = new SQLStmt[batch_size];
            Collections.shuffle(statements, rand);
            for (int ii = 0; ii < batch_size; ii++) {
                batches[i][ii] = statements.get(ii);
            } // FOR
            hashes[i] = VoltProcedure.getBatchHashCode(batches[i], batch_size);
            batchPlanners.put(hashes[i], new BatchPlanner(batches[i], catalog_proc, p_estimator));
        } // FOR
        
        for (int i = 0; i < batches.length; i++) {
            for (int ii = i+1; ii < batches.length; ii++) {
                if (hashes[i] == hashes[ii]) {
                    for (SQLStmt s : batches[i])
                        System.err.println(s.getStatement().fullName());
                    System.err.println("---------------------------------------");
                    for (SQLStmt s : batches[ii])
                        System.err.println(s.getStatement().fullName());
                }
                assert(hashes[i] != hashes[ii]) : Arrays.toString(batches[i]) + " <-> " + Arrays.toString(batches[ii]);
            } // FOR

            // Just check to make sure that if we reduce the length of the 
            // batch size that the hash code changes. We can't check that we don't 
            // already have a BatchPlanner because there might be another batch
            // that only has the SQLStmt in our reduced batch
            int hash = VoltProcedure.getBatchHashCode(batches[i], batches[i].length-1);
            assert(hashes[i] != hash);
        } // FOR
        } // FOR
    }
    
    /**
     * testPlanMultiPartition
     */
    public void testPlanMultiPartition() throws Exception {
        BatchPlanner batchPlan = new BatchPlanner(batch[TARGET_BATCH], catalog_proc, p_estimator);
        BatchPlanner.BatchPlan plan = batchPlan.plan(TXN_ID, BASE_PARTITION, all_partitions, this.touched_partitions, args[TARGET_BATCH]);
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
    }
    
    /**
     * testMispredict
     */
    public void testMispredict() throws Exception {
        BatchPlanner batchPlan = new BatchPlanner(batch[TARGET_BATCH], catalog_proc, p_estimator);
        
        // Ask the planner to plan a multi-partition transaction where we have predicted it
        // as single-partitioned. It should throw a nice MispredictionException
        PartitionSet partitions = new PartitionSet();
        partitions.add(BASE_PARTITION);
        partitions.add(BASE_PARTITION+1);
        
        BatchPlanner.BatchPlan plan = batchPlan.plan(TXN_ID, BASE_PARTITION+1, partitions, this.touched_partitions, args[TARGET_BATCH]);
        assert(plan.hasMisprediction());
        if (plan != null) System.err.println(plan.toString());
    }
    
    /**
     * testMispredict
     */
    public void testMispredictPartitions() throws Exception {
        BatchPlanner batchPlan = new BatchPlanner(batch[TARGET_BATCH], catalog_proc, p_estimator);
        
        // Ask the planner to plan a multi-partition transaction where we have predicted it
        // as single-partitioned. It should throw a nice MispredictionException
        PartitionSet partitions = new PartitionSet();
        partitions.add(BASE_PARTITION);
//        partitions.add(BASE_PARTITION+1);
        
        BatchPlanner.BatchPlan plan = batchPlan.plan(TXN_ID, BASE_PARTITION+1, partitions, this.touched_partitions, args[TARGET_BATCH]);
        assert(plan.hasMisprediction());
        if (plan != null) System.err.println(plan.toString());
    }
    
    /**
     * testGetStatementPartitions
     */
    public void testGetStatementPartitions() throws Exception {
        for (int batch_idx = 0; batch_idx < query_batch.length; batch_idx++) {
            BatchPlanner batchPlan = new BatchPlanner(batch[batch_idx], catalog_proc, p_estimator);
            BatchPlanner.BatchPlan plan = batchPlan.plan(TXN_ID, BASE_PARTITION, all_partitions, this.touched_partitions, args[batch_idx]);
            assertNotNull(plan);
            assertFalse(plan.hasMisprediction());
            
            Statement catalog_stmts[] = batchPlan.getStatements();
            assertNotNull(catalog_stmts);
            assertEquals(query_batch[batch_idx].size(), catalog_stmts.length);
            
            PartitionSet partitions[] = plan.getStatementPartitions();
            assertNotNull(partitions);
            
            for (int i = 0; i < catalog_stmts.length; i++) {
                assertEquals(query_batch[batch_idx].get(i).getCatalogItem(catalogContext.database), catalog_stmts[i]);
                PartitionSet p = partitions[i];
                assertNotNull(p);
                assertFalse(p.isEmpty());
            } // FOR
//            System.err.println(plan);
        }
    }
    
    /**
     * testReplicatedTableTouchedPartitions
     */
    public void testReplicatedTableTouchedPartitions() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getItemInfo");
        
        SQLStmt batch[] = { new SQLStmt(catalog_stmt) };
        ParameterSet params[] = new ParameterSet[]{
                VoltProcedure.getCleanParams(batch[0], new Object[]{ new Long(1) })
        };
        PartitionSet partitions = new PartitionSet(0);
        
        // Check to make sure that if we do a SELECT on a replicated table, that
        // it doesn't get added to our touched partitions histogram
        BatchPlanner batchPlan = new BatchPlanner(batch, catalog_proc, p_estimator);
        BatchPlanner.BatchPlan plan = batchPlan.plan(TXN_ID, BASE_PARTITION, partitions, this.touched_partitions, params);
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
        
        assertEquals(0, this.touched_partitions.getValueCount());
        assertEquals(0, this.touched_partitions.getSampleCount());
    }
    
}
