package edu.brown.workload;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestWorkloadSummarizer extends BaseTestCase {

    private static final int NUM_PARTITIONS = 100;
    private static final int NUM_TRANSACTIONS = 10;
    private static final int NUM_QUERIES = 5;
    private static final Object PARAMS[] = { new Long(100), new String("ABC123") };
    
    private static ParameterMappingsSet mappings;
    private WorkloadSummarizer summarizer;
    private Workload workload;
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);

        if (mappings == null) {
            File f = this.getParameterMappingsFile(ProjectType.TM1);
            mappings = new ParameterMappingsSet();
            mappings.load(f, catalog_db);
        }
        
        summarizer = new WorkloadSummarizer(catalog_db, p_estimator, mappings); 
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
     * testBuildTargetParameters
     */
    public void testBuildTargetParameters() throws Exception {
        StmtParameter catalog_param = catalog_stmt.getParameters().get(1);
        assertNotNull(catalog_param);
        Column catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_param);
        assertNotNull(catalog_col);
        Collection<Column> columns = CollectionUtil.addAll(new HashSet<Column>(), catalog_col);
        summarizer = new WorkloadSummarizer(catalog_db, p_estimator, mappings, catalog_db.getProcedures(), columns);
        
        ProcParameter expected = this.getProcParameter(catalog_proc, 1);
        List<ProcParameter> proc_params = summarizer.getTargetParameters(catalog_proc);
        assertNotNull(proc_params);
        assertEquals(1, proc_params.size());
        assert(proc_params.contains(expected)) : proc_params;
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
                assertEquals(NUM_QUERIES, query_trace.getWeight());
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
                assertEquals(this.summarizer.getTransactionTraceSignature(new_proc, txn_trace, null), 1, txn_trace.getQueryCount());
            }
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertNotNull(query_trace);
                if (is_new_proc) {
                    assertEquals(1, query_trace.getWeight());
                } else {
                    // FIXME assertEquals(NUM_QUERIES, query_trace.getWeight());
                }
            } // FOR
        } // FOR
    }
    
    /**
     * testRemoveDuplicateQueriesWithCandidates
     */
    public void testRemoveDuplicateQueriesWithCandidates() throws Exception {
        // Make a new workload where the first parameter is different for each QueryTrace
        // The default WorkloadSummarizer should not identify any duplicate queries
        workload = new Workload(catalog);
        for (int i = 0; i < NUM_TRANSACTIONS; i++) {
            TransactionTrace txn_trace = new TransactionTrace(i, catalog_proc, PARAMS);
            for (int j = 0; j < NUM_QUERIES; j++) {
                Object query_params[] = { new Long(j), new String("H-STORE!") };
                QueryTrace query_trace = new QueryTrace(catalog_stmt, query_params, i % 3);
                txn_trace.addQuery(query_trace);
            } // FOR
            workload.addTransaction(catalog_proc, txn_trace);
        } // FOR
        assertEquals(NUM_TRANSACTIONS, workload.getTransactionCount());
        Workload pruned = this.summarizer.removeDuplicateQueries(workload);
        assertNotNull(pruned);
        assertEquals(NUM_TRANSACTIONS, pruned.getTransactionCount());
        assertEquals(NUM_TRANSACTIONS*NUM_QUERIES, pruned.getQueryCount());
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
            assertEquals(NUM_QUERIES, txn_trace.getQueryCount());
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertEquals(1, query_trace.getWeight());
            } // FOR (query)
//            System.err.println(txn_trace.debug(catalog_db));
//            System.err.println("----------------------------------------------");
        } // (txn)
//        System.err.println(StringUtil.repeat("+", 100));

        // We then tell the WorkloadSummarizer that only care about the column that is mapped to the second query
        // parameter. All of the queries should then be collapsed into a single weighted query
        StmtParameter catalog_param = catalog_stmt.getParameters().get(1);
        assertNotNull(catalog_param);
        Column catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_param);
        assertNotNull(catalog_col);
        Collection<Column> columns = CollectionUtil.addAll(new HashSet<Column>(), catalog_col);
        summarizer = new WorkloadSummarizer(catalog_db, p_estimator, mappings, catalog_db.getProcedures(), columns);
        pruned = this.summarizer.removeDuplicateQueries(workload);
        assertNotNull(pruned);
        assertEquals(NUM_TRANSACTIONS, pruned.getTransactionCount());
        assertEquals(NUM_TRANSACTIONS, pruned.getQueryCount());
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
            assertEquals(1, txn_trace.getQueryCount());
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertEquals(NUM_QUERIES, query_trace.getWeight());
            } // FOR (query)
//            System.err.println(txn_trace.debug(catalog_db));
//            System.err.println("----------------------------------------------");
        } // FOR (txn)
        
    }
    
    /**
     * testRemoveDuplicateTransactions
     */
    public void testRemoveDuplicateTransactions() throws Exception {
        // Make a new workload where the first parameter is different for each TransactionTrace
        // The default WorkloadSummarizer should not identify any duplicate transactions
        workload = new Workload(catalog);
        for (int i = 0; i < NUM_TRANSACTIONS; i++) {
            Object txn_params[] = { new Long(i), new String("EVAN GOT MARRIED?!?") };
            TransactionTrace txn_trace = new TransactionTrace(i, catalog_proc, txn_params);
            for (int j = 0; j < NUM_QUERIES; j++) {
                Object query_params[] = { new Long(j + 9999*i), "H-STORE" };
                QueryTrace query_trace = new QueryTrace(catalog_stmt, query_params, i % 3);
                txn_trace.addQuery(query_trace);
            } // FOR
            workload.addTransaction(catalog_proc, txn_trace);
        } // FOR
        assertEquals(NUM_TRANSACTIONS, workload.getTransactionCount());
        Workload pruned = this.summarizer.removeDuplicateTransactions(workload);
        assertNotNull(pruned);
        
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
            assertEquals(NUM_QUERIES, txn_trace.getQueryCount());
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertEquals(1, query_trace.getWeight());
            } // FOR (query)
//            System.err.println(summarizer.getTransactionTraceSignature(catalog_proc, txn_trace));
//            System.err.println("----------------------------------------------");
        } // (txn)
        assertEquals(NUM_TRANSACTIONS, pruned.getTransactionCount());
        assertEquals(NUM_TRANSACTIONS*NUM_QUERIES, pruned.getQueryCount());
//        System.err.println(StringUtil.repeat("+", 100));

        // We then tell the WorkloadSummarizer that only care about the column that is mapped to the second
        // parameter. All of the queries should then be collapsed into a single weighted query
        StmtParameter catalog_param = catalog_stmt.getParameters().get(1);
        assertNotNull(catalog_param);
        Column catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_param);
        assertNotNull(catalog_col);
        Collection<Column> columns = CollectionUtil.addAll(new HashSet<Column>(), catalog_col);
        summarizer = new WorkloadSummarizer(catalog_db, p_estimator, mappings, catalog_db.getProcedures(), columns);
        pruned = this.summarizer.removeDuplicateTransactions(workload);
        assertNotNull(pruned);
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
//            System.err.println(txn_trace.debug(catalog_db));
//            System.err.println(summarizer.getTransactionTraceSignature(catalog_proc, txn_trace));
//            System.err.println("----------------------------------------------");
            
            assertEquals(NUM_QUERIES, txn_trace.getQueryCount());
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertEquals(1, query_trace.getWeight());
            } // FOR (query)
        } // FOR (txn)
        assertEquals(1, pruned.getTransactionCount());
        assertEquals(NUM_QUERIES, pruned.getQueryCount());
    }
    
    /**
     * testProcess
     */
    public void testProcess() throws Exception {
        // Make a new workload where the first parameter is different for each TransactionTrace
        // The default WorkloadSummarizer should not identify any duplicate transactions
        workload = new Workload(catalog);
        for (int i = 0; i < NUM_TRANSACTIONS; i++) {
            Object txn_params[] = { new Long(i), new String("EVAN GOT MARRIED?!?") };
            TransactionTrace txn_trace = new TransactionTrace(i, catalog_proc, txn_params);
            for (int j = 0; j < NUM_QUERIES; j++) {
                Object query_params[] = { new Long(j + 9999*i), "H-STORE" };
                QueryTrace query_trace = new QueryTrace(catalog_stmt, query_params, i % 3);
                txn_trace.addQuery(query_trace);
            } // FOR
            workload.addTransaction(catalog_proc, txn_trace);
        } // FOR
        assertEquals(NUM_TRANSACTIONS, workload.getTransactionCount());
        Workload pruned = this.summarizer.process(workload);
        assertNotNull(pruned);
        
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
            assertEquals(NUM_QUERIES, txn_trace.getQueryCount());
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertEquals(1, query_trace.getWeight());
            } // FOR (query)
//            System.err.println(summarizer.getTransactionTraceSignature(catalog_proc, txn_trace));
//            System.err.println("----------------------------------------------");
        } // (txn)
        assertEquals(NUM_TRANSACTIONS, pruned.getTransactionCount());
        assertEquals(NUM_TRANSACTIONS*NUM_QUERIES, pruned.getQueryCount());
//        System.err.println(StringUtil.repeat("+", 100));

        // We then tell the WorkloadSummarizer that only care about the column that is mapped to the second
        // parameter. All of the queries and transactions should then be collapsed into a single weighted transaction
        // with a single weight query
        StmtParameter catalog_param = catalog_stmt.getParameters().get(1);
        assertNotNull(catalog_param);
        Column catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_param);
        assertNotNull(catalog_col);
        Collection<Column> columns = CollectionUtil.addAll(new HashSet<Column>(), catalog_col);
        summarizer = new WorkloadSummarizer(catalog_db, p_estimator, mappings, catalog_db.getProcedures(), columns);
        pruned = this.summarizer.process(workload);
        assertNotNull(pruned);
        for (TransactionTrace txn_trace : pruned) {
            assertNotNull(txn_trace);
//            System.err.println(txn_trace.debug(catalog_db));
//            System.err.println(summarizer.getTransactionTraceSignature(catalog_proc, txn_trace));
//            System.err.println("----------------------------------------------");

            assert(txn_trace.getWeight() > 1);
            assertEquals(1, txn_trace.getQueryCount());
            for (QueryTrace query_trace : txn_trace.getQueries()) {
                assertEquals(NUM_QUERIES * NUM_TRANSACTIONS, query_trace.getWeight() * txn_trace.getWeight());
            } // FOR (query)
        } // FOR (txn)
        assertEquals(1, pruned.getTransactionCount());
        assertEquals(1, pruned.getQueryCount());
    }
}
