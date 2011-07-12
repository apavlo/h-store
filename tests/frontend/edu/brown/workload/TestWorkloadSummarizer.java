package edu.brown.workload;

import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.utils.ProjectType;

public class TestWorkloadSummarizer extends BaseTestCase {

    private static final int NUM_PARTITIONS = 100;
    private static final int NUM_TRANSACTIONS = 10;
    private static final int NUM_QUERIES = 5;
    private static final Object PARAMS[] = { new Long(100), new String("ABC123") };
    
    private WorkloadSummarizer summarizer;
    private Workload workload;
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);

        summarizer = new WorkloadSummarizer(catalog_db, p_estimator, null, null); 
        catalog_proc = this.getProcedure(UpdateLocation.class);
        catalog_stmt = this.getStatement(catalog_proc, "update");
        
        // Construct a synthetic workload with duplicate TransactionTraces that each have
        // duplicate QueryTraces
        workload = new Workload(catalog);
        
        for (int i = 0; i < NUM_TRANSACTIONS; i++) {
            TransactionTrace txn_trace = new TransactionTrace(i, catalog_proc, PARAMS);
            for (int j = 0; j < NUM_QUERIES; j++) {
                QueryTrace query_trace = new QueryTrace(catalog_stmt, PARAMS, i % 3);
                txn_trace.addQuery(query_trace);
            } // FOR
            workload.addTransaction(catalog_proc, txn_trace);
        } // FOR
        assertEquals(NUM_TRANSACTIONS, workload.getTransactionCount());
    }
    
    /**
     * testRemoveDuplicateQueries
     */
    public void testRemoveDuplicateQueries() throws Exception {
        Workload pruned = this.summarizer.removeDuplicateQueries(workload);
        assertNotNull(pruned);
        assertEquals(NUM_TRANSACTIONS, pruned.getTransactionCount());
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
//            System.err.println(txn_trace.debug(catalog_db) + "\n");
            assertEquals(1, txn_trace.getQueryCount());
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertNotNull(query_trace);
                assert(query_trace.hasWeight());
                assertEquals(NUM_QUERIES, query_trace.getWeight().intValue());
            } // FOR
        } // FOR
        
        // Then add another transaction for another procedure that doesn't have
        // duplicate queries
        Procedure new_proc = this.getProcedure(GetAccessData.class);
        Statement new_stmt = this.getStatement(new_proc, "GetData");
        TransactionTrace new_txn_trace = new TransactionTrace(pruned.getTransactionCount() + 100, new_proc, new Object[] { new Long(1), new Long(2) });
        for (int i = 0; i < NUM_QUERIES; i++) {
            Object new_params[] = new Object[] { new Long(i), new Long(2) };
            QueryTrace new_query_trace = new QueryTrace(new_stmt, new_params, i % 3);
            new_txn_trace.addQuery(new_query_trace);
        } // FOR
        workload.addTransaction(new_proc, new_txn_trace);
        
        pruned = this.summarizer.removeDuplicateQueries(workload);
        assertNotNull(pruned);
        assertEquals(NUM_TRANSACTIONS+1, pruned.getTransactionCount());
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
            boolean is_new_proc = txn_trace.getCatalogItem(catalog_db).equals(new_proc);
//            System.err.println(txn_trace.debug(catalog_db) + "\n");
            if (is_new_proc) {
                assertEquals(NUM_QUERIES, txn_trace.getQueryCount());
            } else {
                assertEquals(1, txn_trace.getQueryCount());
            }
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertNotNull(query_trace);
                if (is_new_proc) {
                    assertFalse(query_trace.hasWeight());
                } else {
                    assert(query_trace.hasWeight());
                    assertEquals(NUM_QUERIES, query_trace.getWeight().intValue());
                }
            } // FOR
        } // FOR
    }
}
