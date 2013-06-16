package edu.brown.hstore.estimators.markov;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.*;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil.StatementWrapper;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.*;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.NoAbortFilter;
import edu.brown.workload.filters.ProcParameterArraySizeFilter;
import edu.brown.workload.filters.ProcParameterValueFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * 
 * @author pavlo
 *
 */
public class TestMarkovEstimator extends BaseTestCase {

    public static final Random rand = new Random();
    
    private static final int WORKLOAD_XACT_LIMIT = 50;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 5;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static AtomicLong XACT_ID = new AtomicLong(1000);

    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    private static TransactionTrace singlep_trace;
    private static TransactionTrace multip_trace;
    private static final List<MarkovVertex> multip_path = new ArrayList<MarkovVertex>();
    
    private MarkovEstimator t_estimator;
    private EstimationThresholds thresholds;
    private Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        HStoreConf hstore_conf = HStoreConf.singleton();
        hstore_conf.site.markov_path_caching = false;
        hstore_conf.site.markov_fast_path = false;
        hstore_conf.site.markov_force_traversal = true;
        hstore_conf.site.markov_learning_enable = false;
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        if (isFirstSetup()) {
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new NoAbortFilter())
                    .attach(new ProcParameterValueFilter().include(1, new Integer(5))) // D_ID
                    .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));

            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalogContext.catalog);
            ((Workload) workload).load(file, catalogContext.database, filter);
            assert(workload.getTransactionCount() > 0);
            
            // Generate MarkovGraphs
            markovs = MarkovGraphsContainerUtil.createBasePartitionMarkovGraphsContainer(catalogContext, workload, p_estimator);
            assertNotNull(markovs);
            
            // Find a single-partition and multi-partition trace
            PartitionSet multip_partitions = new PartitionSet();
            multip_partitions.add(BASE_PARTITION);
            for (TransactionTrace xact : workload.getTransactions()) {
                Object ol_supply_w_ids[] = (Object[])xact.getParam(5);
                assert(ol_supply_w_ids != null);
                boolean same_partition = true;
                for (Object i : ol_supply_w_ids) {
                    Integer partition = p_estimator.getHasher().hash(Integer.valueOf(i.toString()));
                    same_partition = same_partition && (partition == BASE_PARTITION);
                    if (same_partition == false && multip_trace == null) {
                        multip_partitions.add(partition);
                    }
                } // FOR
                if (same_partition && singlep_trace == null) singlep_trace = xact;
                if (same_partition == false && multip_trace == null) {
                    multip_trace = xact;
                    multip_path.addAll(markovs.get(BASE_PARTITION, this.catalog_proc).processTransaction(xact, p_estimator));
                }
                if (singlep_trace != null && multip_trace != null) break;
            } // FOR
            assert(multip_partitions.size() > 1);
        }
        assertNotNull(multip_trace);
        assertFalse(multip_path.isEmpty());
        assertNotNull(singlep_trace);
        
        // Setup
        this.t_estimator = new MarkovEstimator(catalogContext, p_estimator, markovs);
        this.thresholds = new EstimationThresholds();
    }

    /**
     * testUnknownPath
     */
    @Test
    public void testUnknownPath() throws Exception {
        // This is to test our ability to handle a transaction with
        // a path that we haven't seen before.
        
        // Find the S_W_ID that's different than the base partition, and change
        // all of the elements in the array to that value.
        TransactionTrace txn_trace = (TransactionTrace)multip_trace.clone();
        int w_id = (Integer)txn_trace.getParam(0);
        Short s_w_ids[] = (Short[])txn_trace.getParam(5);
        short remote_w_id = -1;
        assert(s_w_ids.length > 0);
        for (int i = 0; i < s_w_ids.length; i++) {
            if (w_id != s_w_ids[i]) {
                remote_w_id = s_w_ids[i];
                break;
            }
        } // FOR
        assert(remote_w_id >= 0);
        assertNotSame(w_id, remote_w_id);
        Arrays.fill(s_w_ids, remote_w_id);
//        System.err.println("S_W_ID: " + Arrays.toString(s_w_ids));
        txn_trace.setParam(5, s_w_ids);
        
        MarkovEstimatorState state = t_estimator.startTransaction(XACT_ID.getAndIncrement(),
                                                                  this.catalog_proc,
                                                                  txn_trace.getParams());
        assertNotNull(state);
            
        // Even though it won't correctly identify the exact path that we're going to
        // take (i.e., what partitions the getStockInfo queries will need), the path
        // should be complete (i.e., we should see each Statement in NewOrder executed
        // at least once)
        MarkovEstimate initialEst = state.getInitialEstimate();
//        System.err.println("FIRST ESTIMATE:\n" + initialEst);
        List<MarkovVertex> initialPath = initialEst.getMarkovPath();
        assertFalse(initialPath.isEmpty());
        Set<Statement> seenStmts = new HashSet<Statement>();
        for (MarkovVertex v : initialPath) {
            Statement stmt = v.getCatalogItem();
            if ((stmt instanceof StatementWrapper) == false) {
                seenStmts.add(stmt);
            }
        } // FOR
        Procedure proc = txn_trace.getCatalogItem(catalogContext.database);
        for (Statement stmt : proc.getStatements()) {
            assertTrue(stmt.fullName(), seenStmts.contains(stmt));
        }
//        System.err.println(proc + "\n" + StringUtil.join("\n", proc.getStatements()));
//        System.err.println("=======================================");
//        System.err.println("SEEN\n" + StringUtil.join("\n", seenStmts));
        assertEquals(proc.getStatements().size(), seenStmts.size());
    }
    
    /**
     * testMultipleStartTransaction
     */
    @Test
    public void testMultipleStartTransaction() throws Exception {
        Set<MarkovEstimatorState> all_states = new HashSet<MarkovEstimatorState>();
        int expected = 20;
        MarkovEstimate initialEst = null;
        for (int i = 0; i < expected; i++) {
            MarkovEstimatorState state = t_estimator.startTransaction(XACT_ID.getAndIncrement(),
                                                                      this.catalog_proc,
                                                                      multip_trace.getParams());
            assertNotNull(state);
            assertFalse(all_states.contains(state));
            all_states.add(state);
            
            if (initialEst == null) {
                initialEst = state.getInitialEstimate();
                // System.err.println("FIRST ESTIMATE:\n" + initialEst);
            } else {
                MarkovEstimate est = state.getInitialEstimate();
                // System.err.printf("ESTIMATE #%02d:\n%s", i, est);
                assertEquals(initialEst.getTouchedPartitions(thresholds), est.getTouchedPartitions(thresholds));
            }
            
        } // FOR
        assertEquals(expected, all_states.size());
    }
    
    /**
     * testStartTransaction
     */
    @Test
    public void testStartTransaction() throws Exception {
        long txn_id = XACT_ID.getAndIncrement();
        MarkovEstimatorState state = t_estimator.startTransaction(txn_id, this.catalog_proc, singlep_trace.getParams());
        assertNotNull(state);
        assertNotNull(state.getLastEstimate());
        
        MarkovEstimate est = state.getInitialEstimate();
        System.err.println(est);
        assertNotNull(est);
        assertTrue(est.toString(), est.isInitialized());
//        assertTrue(est.toString(), est.isSinglePartitionProbabilitySet());
        assertTrue(est.toString(), est.isAbortProbabilitySet());
        assertTrue(est.toString(), est.isConfidenceCoefficientSet());
        // assertTrue(est.toString(), est.getConfidenceCoefficient() >= 0f);
//        assertEquals(est.toString(), 1.0f, est.getSinglePartitionProbability());
        assertEquals(est.toString(), 1.0f, est.getConfidenceCoefficient());

        //        System.err.println(est.toString());
        
        MarkovGraph markov = state.getMarkovGraph();
        List<MarkovVertex> initial_path = est.getMarkovPath();
        assertNotNull(initial_path);
        assertFalse(initial_path.isEmpty());
        
        System.err.println("# of Vertices: " + markov.getVertexCount());
        System.err.println("# of Edges:    " + markov.getEdgeCount());
        System.err.println("Confidence:    " + String.format("%.4f", est.getConfidenceCoefficient()));
        System.err.println("\nINITIAL PATH:\n" + StringUtil.join("\n", initial_path));
//        System.err.println(multip_trace.debug(catalog_db));

        PartitionSet partitions = new PartitionSet();
        p_estimator.getAllPartitions(partitions, singlep_trace);
        assertNotNull(partitions);
//        assert(partitions.size() > 1) : partitions;
        System.err.println("partitions: " + partitions);
        
//        GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, false, null);
//        gv.highlightPath(markov.getPath(initial_path), "blue");
//        gv.writeToTempFile(this.catalog_proc, 0);
//
//        MarkovUtil.exportGraphviz(markov, false, markov.getPath(multip_path)).writeToTempFile(this.catalog_proc, 1);
        
        Collection<Integer> est_partitions = est.getTouchedPartitions(thresholds);
        assertNotNull(est_partitions);
        assertEquals(partitions.size(), est_partitions.size());
        assertEquals(partitions, est_partitions);
        
        assert(est.isSinglePartitioned(this.thresholds));
        assertTrue(est.isAbortable(this.thresholds));
        
        for (int partition : catalogContext.getAllPartitionIds()) {
            if (partitions.contains(partition)) { //  == BASE_PARTITION) {
                assertFalse("isFinishedPartition(" + partition + ")", est.isDonePartition(thresholds, partition));
                assertTrue("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition));
                assertTrue("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition));
            } else {
                assertTrue("isFinishedPartition(" + partition + ")", est.isDonePartition(thresholds, partition));
                assertFalse("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition));
                assertFalse("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition));
            }
        } // FOR
    }
    
    /**
     * testStartTransactionDtxn
     */
    @Test
    public void testStartTransactionDtxn() throws Exception {
        TransactionTrace txn_trace = multip_trace;
        long txn_id = XACT_ID.getAndIncrement();
        MarkovEstimatorState state = t_estimator.startTransaction(txn_id, this.catalog_proc, txn_trace.getParams());
        assertNotNull(state);
        assertNotNull(state.getLastEstimate());
        
        MarkovEstimate initialEst = state.getInitialEstimate();
        assertNotNull(initialEst);
        assertTrue(initialEst.toString(), initialEst.isInitialized());
//        assertTrue(initialEst.toString(), initialEst.isSinglePartitionProbabilitySet());
        assertTrue(initialEst.toString(), initialEst.isAbortProbabilitySet());
//        assertTrue(initialEst.toString(), initialEst.getSinglePartitionProbability() < 1.0f);
        assertTrue(initialEst.toString(), initialEst.isConfidenceCoefficientSet());
        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() >= 0f);
        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() <= 1f);
        assertFalse(initialEst.toString(), initialEst.isSinglePartitioned(this.thresholds));
        assertTrue(initialEst.toString(), initialEst.isAbortable(this.thresholds));
        
        MarkovGraph markov = state.getMarkovGraph();
        List<MarkovVertex> initial_path = initialEst.getMarkovPath();
        assertNotNull(initial_path);
        assertFalse(initial_path.isEmpty());
        
        System.err.println("# of Vertices: " + markov.getVertexCount());
        System.err.println("# of Edges:    " + markov.getEdgeCount());
        System.err.println("Confidence:    " + String.format("%.4f", initialEst.getConfidenceCoefficient()));
        System.err.println("\nINITIAL PATH:\n" + StringUtil.join("\n", initial_path));

        PartitionSet partitions = new PartitionSet();
        p_estimator.getAllPartitions(partitions, txn_trace);
        assertNotNull(partitions);
        assert(partitions.size() > 1) : partitions;
        
        Collection<Integer> est_partitions = initialEst.getTouchedPartitions(thresholds);
        assertNotNull(est_partitions);
        assertEquals(partitions.size(), est_partitions.size());
        assertEquals(partitions, est_partitions);
    }
    
    /**
     * testExecuteQueries
     */
    @Test
    public void testExecuteQueries() throws Exception {
        TransactionTrace txn_trace = multip_trace;
        long txn_id = XACT_ID.getAndIncrement();
        MarkovEstimatorState state = t_estimator.startTransaction(txn_id, this.catalog_proc, txn_trace.getParams());
        assertNotNull(state);
        assertNotNull(state.getLastEstimate());
        
        MarkovEstimate initialEst = state.getInitialEstimate();
        assertNotNull(initialEst);
        assertTrue(initialEst.toString(), initialEst.isInitialized());
//        assertTrue(initialEst.toString(), initialEst.isSinglePartitionProbabilitySet());
        assertTrue(initialEst.toString(), initialEst.isAbortProbabilitySet());
//        assertTrue(initialEst.toString(), initialEst.getSinglePartitionProbability() < 1.0f);
        assertTrue(initialEst.toString(), initialEst.isConfidenceCoefficientSet());
        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() >= 0f);
        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() <= 1f);

        // Get the list of partitions that we're going to touch in the beginning
        // We should never mark these as finished in subsequent estimates
        PartitionSet touched = initialEst.getTouchedPartitions(thresholds);
        assertFalse(touched.isEmpty());
        assertFalse(touched.contains(HStoreConstants.NULL_PARTITION_ID));
        System.err.println("TOUCHED: " + touched);
        assertFalse(touched.toString(), touched.size() == 1);
        
        // Execute a bunch of batches
        // All should say that the txn is not finished with the partitions until we 
        // get to the one that contains the updateStock queries, which should be the last.
        Statement lastBatchStmt = this.getStatement(this.catalog_proc, "updateStock");
        for (int i = 0, cnt = txn_trace.getBatchCount(); i < cnt; i++) {
            List<QueryTrace> queries = txn_trace.getBatchQueries(i);
            assertFalse(queries.isEmpty());
            boolean is_last = (i+1 == cnt);
            Statement stmts[] = new Statement[queries.size()];
            PartitionSet partitions[] = new PartitionSet[queries.size()];
            
            boolean found = false;
            int idx = 0;
            for (QueryTrace q : queries) {
                stmts[idx] = q.getCatalogItem(catalogContext.database);
                assertNotNull(stmts[idx]);
                partitions[idx] = new PartitionSet();
                p_estimator.getAllPartitions(partitions[idx], q, state.getBasePartition());
                assertFalse(partitions[idx].isEmpty());
                assertFalse(partitions[idx].contains(HStoreConstants.NULL_PARTITION_ID));
                
                found = found || stmts[idx].equals(lastBatchStmt);
                idx++;
            } // FOR
            if (is_last) assertTrue(StringUtil.join("\n", queries), found); 
                
            MarkovEstimate est = t_estimator.executeQueries(state, stmts, partitions);
            assertNotNull(est);
            
            for (int partition : catalogContext.getAllPartitionIds()) {
                String debug = String.format("Batch %02d / Partition %02d / isLast=%s\n%s",
                                             est.getBatchId(), partition, is_last, est.toString());
                assertTrue(debug, est.isDoneProbabilitySet(partition));
                assertTrue(debug, est.isWriteProbabilitySet(partition));
//                assertTrue(debug, est.isReadOnlyProbabilitySet(partition));
                
                if (touched.contains(partition) && is_last == false) { //  || partition == state.getBasePartition())) {
                    assertFalse(debug, est.isDonePartition(thresholds, partition));
                    assertTrue(debug, est.isWritePartition(thresholds, partition));
                } else {
                    assertTrue(debug, est.isDonePartition(thresholds, partition));
                }
            } // FOR
        } // FOR 
    }
    
    /**
     * testFastPath
     */
    @Test
    public void testFastPath() throws Exception {
        TransactionTrace txn_trace = multip_trace;
        int base_partition = p_estimator.getBasePartition(txn_trace);
        
        List<QueryTrace> queries = txn_trace.getBatchQueries(1);
        assertFalse(queries.isEmpty());
        Statement stmts[] = new Statement[queries.size()];
        PartitionSet partitions[] = new PartitionSet[queries.size()];
        int idx = 0;
        for (QueryTrace q : queries) {
            stmts[idx] = q.getCatalogItem(catalogContext.database);
            assertNotNull(stmts[idx]);
            partitions[idx] = new PartitionSet();
            p_estimator.getAllPartitions(partitions[idx], q, base_partition);
            idx++;
        } // FOR
        
        MarkovEstimatorState states[] = new MarkovEstimatorState[2];
        MarkovEstimate ests[] = new MarkovEstimate[states.length];
        for (int i = 0; i < states.length; i++) {
            HStoreConf.singleton().site.markov_fast_path = (i != 0);
            states[i] = t_estimator.startTransaction(XACT_ID.getAndIncrement(), this.catalog_proc, txn_trace.getParams());
            assertNotNull(states[i]);
            assertNotNull(states[i].getLastEstimate());
            ests[i] = t_estimator.executeQueries(states[i], stmts, partitions);
            assertNotNull(ests[i]);
        } // FOR

        // Now compare that estimates look reasonable the same
        for (int i = 0; i < states.length; i++) {
            for (int ii = i+1; ii < states.length; ii++) {
                assertEquals(states[i].getBasePartition(), states[ii].getBasePartition());
                assertEquals(ests[i].getTouchedPartitions(thresholds), ests[ii].getTouchedPartitions(thresholds));
                assertEquals(ests[i].getBatchId(), ests[ii].getBatchId());
//                assertEquals(ests[i].getSinglePartitionProbability(), ests[ii].getSinglePartitionProbability());
                assertEquals(ests[i].getAbortProbability(), ests[ii].getAbortProbability());
                
                for (Integer partition : catalogContext.getAllPartitionIds()) {
                    String debug = String.format("Batch %02d / Partition %02d\n%s",
                                                 ests[i].getBatchId(), partition, ests[i].toString());
                    
                    assertEquals(debug, ests[i].getTouchedCounter(partition), ests[ii].getTouchedCounter(partition));
                    assertEquals(debug, ests[i].isDoneProbabilitySet(partition), ests[ii].isDoneProbabilitySet(partition));
                    assertEquals(debug, ests[i].isWriteProbabilitySet(partition), ests[ii].isWriteProbabilitySet(partition));
//                    assertEquals(debug, ests[i].isReadOnlyProbabilitySet(partition), ests[ii].isReadOnlyProbabilitySet(partition));
                    assertEquals(debug, ests[i].isDonePartition(thresholds, partition), ests[ii].isDonePartition(thresholds, partition));
                    assertEquals(debug, ests[i].isWritePartition(thresholds, partition), ests[ii].isWritePartition(thresholds, partition));
                    assertEquals(debug, ests[i].isReadOnlyPartition(thresholds, partition), ests[ii].isReadOnlyPartition(thresholds, partition));
                } // FOR
            } // FOR        
        }
    }
    
    /**
     * testProcessTransactionTrace
     */
    @Test
    public void testProcessTransactionTrace() throws Exception {
        TransactionTrace txn_trace = singlep_trace;
        assertNotNull(txn_trace);
        MarkovEstimatorState s = this.t_estimator.processTransactionTrace(txn_trace);
        assertNotNull(s);
        
        MarkovEstimate initialEst = s.getInitialEstimate();
        assertNotNull(initialEst);
        assertTrue(initialEst.toString(), initialEst.isInitialized());
//        assertTrue(initialEst.toString(), initialEst.isSinglePartitionProbabilitySet());
        assertTrue(initialEst.toString(), initialEst.isAbortProbabilitySet());
//        assertTrue(initialEst.toString(), initialEst.getSinglePartitionProbability() < 1.0f);
        assertTrue(initialEst.toString(), initialEst.isConfidenceCoefficientSet());
        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() >= 0f);
        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() <= 1f);
        assertTrue(initialEst.toString(), initialEst.getMarkovPath().isEmpty() == false);
        
        // We should have an MarkovEstimate for each batch
        assertEquals(txn_trace.getBatchCount(), s.getEstimateCount());
        List<Estimate> estimates = s.getEstimates();
        for (int i = 0, cnt = txn_trace.getBatchCount(); i < cnt; i++) {
            List<QueryTrace> queries = txn_trace.getBatchQueries(i);
            assertFalse(queries.isEmpty());
            
            MarkovEstimate est = (MarkovEstimate)estimates.get(i);
            assertNotSame(initialEst, est);
            assertNotNull(est);
//            assertTrue(est.toString(), est.isSinglePartitionProbabilitySet());
            assertTrue(est.toString(), est.isAbortProbabilitySet());
            assertTrue(est.toString(), est.isSinglePartitioned(thresholds));
            assertTrue(est.toString(), est.isConfidenceCoefficientSet());
            assertTrue(est.toString(), est.getConfidenceCoefficient() >= 0f);
            assertTrue(est.toString(), est.getConfidenceCoefficient() <= 1f);
            assertTrue(est.toString(), est.getMarkovPath().isEmpty() == false);
            
            // The last vertex in each MarkovEstimate should correspond to the last query in each batch
            MarkovVertex last_v = est.getVertex();
            assertNotNull(last_v);
            System.err.println("LAST VERTEX: " + last_v);
            assertEquals(CollectionUtil.last(queries).getCatalogItem(catalogContext.database), last_v.getCatalogItem());
        } // FOR
    }
}
