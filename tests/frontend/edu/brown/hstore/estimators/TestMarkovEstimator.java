package edu.brown.hstore.estimators;

import java.io.File;
import java.util.*;

import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.*;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.estimators.MarkovEstimator;
import edu.brown.hstore.estimators.MarkovEstimatorState;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
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
    private static long XACT_ID = 1000;

    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    private static TransactionTrace singlep_trace;
    private static TransactionTrace multip_trace;
    private static final Set<Integer> multip_partitions = new HashSet<Integer>();
    private static final List<MarkovVertex> multip_path = new ArrayList<MarkovVertex>();
    
    private MarkovEstimator t_estimator;
    private EstimationThresholds thresholds;
    private Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        if (markovs == null) {
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new NoAbortFilter())
                    .attach(new ProcParameterValueFilter().include(1, new Integer(5))) // D_ID
                    .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));

            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            ((Workload) workload).load(file, catalog_db, filter);
            assert(workload.getTransactionCount() > 0);
            
            // Generate MarkovGraphs
            markovs = MarkovGraphContainersUtil.createBasePartitionMarkovGraphsContainer(catalog_db, workload, p_estimator);
            assertNotNull(markovs);
            
            // Find a single-partition and multi-partition trace
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
        }
        assertNotNull(multip_trace);
        assert(multip_partitions.size() > 1);
        assertFalse(multip_path.isEmpty());
//        assertNotNull(singlep_trace);
        
        // Setup
        this.t_estimator = new MarkovEstimator(catalogContext, p_estimator, markovs);
        this.thresholds = new EstimationThresholds();
    }

//    /**
//     * testMultipleStartTransaction
//     */
//    @Test
//    public void testMultipleStartTransaction() throws Exception {
//        Set<MarkovEstimatorState> all_states = new HashSet<MarkovEstimatorState>();
//        
//        for (int i = 0; i < 20; i++) {
//            MarkovEstimatorState state = (MarkovEstimatorState)t_estimator.startTransaction(XACT_ID++, this.catalog_proc, multip_trace.getParams());
//            assertNotNull(state);
//            assertFalse(all_states.contains(state));
//            all_states.add(state);
//        } // FOR
//    }
//    
//    /**
//     * testStartTransaction
//     */
//    @Test
//    public void testStartTransaction() throws Exception {
//        long txn_id = XACT_ID++;
//        MarkovEstimatorState state = (MarkovEstimatorState)t_estimator.startTransaction(txn_id, this.catalog_proc, singlep_trace.getParams());
//        assertNotNull(state);
//        assertNotNull(state.getLastEstimate());
//        
//        MarkovEstimate est = state.getInitialEstimate();
//        assertNotNull(est);
//        assertTrue(est.toString(), est.isInitialized());
//        assertTrue(est.toString(), est.isSinglePartitionProbabilitySet());
//        assertTrue(est.toString(), est.isAbortProbabilitySet());
//        assertTrue(est.toString(), est.getSinglePartitionProbability() < 1.0f);
//        assertTrue(est.toString(), est.isConfidenceCoefficientSet());
//        assertTrue(est.toString(), est.getConfidenceCoefficient() >= 0f);
//        assertTrue(est.toString(), est.getConfidenceCoefficient() <= 1f);
//
//        //        System.err.println(est.toString());
//        
//        MarkovGraph markov = state.getMarkovGraph();
//        List<MarkovVertex> initial_path = est.getMarkovPath();
//        assertNotNull(initial_path);
//        assertFalse(initial_path.isEmpty());
//        
//        System.err.println("# of Vertices: " + markov.getVertexCount());
//        System.err.println("# of Edges:    " + markov.getEdgeCount());
//        System.err.println("Confidence:    " + String.format("%.4f", est.getConfidenceCoefficient()));
//        System.err.println("\nINITIAL PATH:\n" + StringUtil.join("\n", initial_path));
////        System.err.println(multip_trace.debug(catalog_db));
//
//        PartitionSet partitions = new PartitionSet();
//        p_estimator.getAllPartitions(partitions, singlep_trace);
//        assertNotNull(partitions);
////        assert(partitions.size() > 1) : partitions;
//        System.err.println("partitions: " + partitions);
//        
////        GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, false, null);
////        gv.highlightPath(markov.getPath(initial_path), "blue");
////        gv.writeToTempFile(this.catalog_proc, 0);
////
////        MarkovUtil.exportGraphviz(markov, false, markov.getPath(multip_path)).writeToTempFile(this.catalog_proc, 1);
//        
//        Collection<Integer> est_partitions = est.getTouchedPartitions(thresholds);
//        assertNotNull(est_partitions);
//        assertEquals(partitions.size(), est_partitions.size());
//        assertEquals(partitions, est_partitions);
//        
//        assert(est.isSinglePartitioned(this.thresholds));
//        assertTrue(est.isAbortable(this.thresholds));
//        
//        for (Integer partition : catalogContext.getAllPartitionIds()) {
//            if (partitions.contains(partition)) { //  == BASE_PARTITION) {
//                assertFalse("isFinishedPartition(" + partition + ")", est.isFinishPartition(thresholds, partition));
//                assertTrue("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition));
//                assertTrue("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition));
//            } else {
//                assertTrue("isFinishedPartition(" + partition + ")", est.isFinishPartition(thresholds, partition));
//                assertFalse("isWritePartition(" + partition + ")", est.isWritePartition(thresholds, partition));
//                assertFalse("isTargetPartition(" + partition + ")", est.isTargetPartition(thresholds, partition));
//            }
//        } // FOR
//    }
    
    /**
     * testStartTransactionDtxn
     */
    @Test
    public void testStartTransactionDtxn() throws Exception {
        TransactionTrace txn_trace = multip_trace;
        long txn_id = XACT_ID++;
        MarkovEstimatorState state = t_estimator.startTransaction(txn_id, this.catalog_proc, txn_trace.getParams());
        assertNotNull(state);
        assertNotNull(state.getLastEstimate());
        
        MarkovEstimate initialEst = state.getInitialEstimate();
        assertNotNull(initialEst);
        assertTrue(initialEst.toString(), initialEst.isInitialized());
        assertTrue(initialEst.toString(), initialEst.isSinglePartitionProbabilitySet());
        assertTrue(initialEst.toString(), initialEst.isAbortProbabilitySet());
        assertTrue(initialEst.toString(), initialEst.getSinglePartitionProbability() < 1.0f);
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
//        System.err.println(multip_trace.debug(catalog_db));

        PartitionSet partitions = new PartitionSet();
        p_estimator.getAllPartitions(partitions, txn_trace);
        assertNotNull(partitions);
//        assert(partitions.size() > 1) : partitions;
        System.err.println("partitions: " + partitions);
        
        Collection<Integer> est_partitions = initialEst.getTouchedPartitions(thresholds);
        assertNotNull(est_partitions);
        assertEquals(partitions.size(), est_partitions.size());
        assertEquals(partitions, est_partitions);
        
//        for (Integer partition : catalogContext.getAllPartitionIds()) {
//            if (partitions.contains(partition)) { //  == BASE_PARTITION) {
//                assertFalse("isFinishedPartition(" + partition + ")", initialEst.isFinishPartition(thresholds, partition));
//                assertTrue("isWritePartition(" + partition + ")", initialEst.isWritePartition(thresholds, partition));
//                assertTrue("isTargetPartition(" + partition + ")", initialEst.isTargetPartition(thresholds, partition));
//            } else {
//                assertTrue("isFinishedPartition(" + partition + ")", initialEst.isFinishPartition(thresholds, partition));
//                assertFalse("isWritePartition(" + partition + ")", initialEst.isWritePartition(thresholds, partition));
//                assertFalse("isTargetPartition(" + partition + ")", initialEst.isTargetPartition(thresholds, partition));
//            }
//        } // FOR
    }
    
//    /**
//     * testExecuteQueries
//     */
//    @Test
//    public void testExecuteQueries() throws Exception {
//        TransactionTrace txn_trace = multip_trace;
//        long txn_id = XACT_ID++;
//        MarkovEstimatorState state = t_estimator.startTransaction(txn_id, this.catalog_proc, txn_trace.getParams());
//        assertNotNull(state);
//        assertNotNull(state.getLastEstimate());
//        
//        MarkovEstimate initialEst = state.getInitialEstimate();
//        assertNotNull(initialEst);
//        assertTrue(initialEst.toString(), initialEst.isInitialized());
//        assertTrue(initialEst.toString(), initialEst.isSinglePartitionProbabilitySet());
//        assertTrue(initialEst.toString(), initialEst.isAbortProbabilitySet());
//        assertTrue(initialEst.toString(), initialEst.getSinglePartitionProbability() < 1.0f);
//        assertTrue(initialEst.toString(), initialEst.isConfidenceCoefficientSet());
//        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() >= 0f);
//        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() <= 1f);
//
//        // Get the list of partitions that we're going to touch in the beginning
//        // We should never mark these as finished in subsequent estimates
//        PartitionSet touched = initialEst.getTouchedPartitions(thresholds);
//        assertFalse(touched.isEmpty());
//        assertFalse(touched.contains(HStoreConstants.NULL_PARTITION_ID));
//        System.err.println("TOUCHED: " + touched);
//        assertFalse(touched.toString(), touched.size() == 1);
//        
//        // Execute a bunch of batches
//        // All should say that the txn is not finished with the partitions until we 
//        // get to the one that contains the updateStock queries, which should be the last.
//        Statement lastBatchStmt = this.getStatement(this.catalog_proc, "updateStock");
//        for (int i = 0, cnt = txn_trace.getBatchCount(); i < cnt; i++) {
//            List<QueryTrace> queries = txn_trace.getBatchQueries(i);
//            assertFalse(queries.isEmpty());
//            boolean is_last = (i+1 == cnt);
//            Statement stmts[] = new Statement[queries.size()];
//            PartitionSet partitions[] = new PartitionSet[queries.size()];
//            
//            boolean found = false;
//            int idx = 0;
//            for (QueryTrace q : queries) {
//                stmts[idx] = q.getCatalogItem(catalogContext.database);
//                assertNotNull(stmts[idx]);
//                partitions[idx] = new PartitionSet();
//                p_estimator.getAllPartitions(partitions[idx], q, state.getBasePartition());
//                assertFalse(partitions[idx].isEmpty());
//                assertFalse(partitions[idx].contains(HStoreConstants.NULL_PARTITION_ID));
//                
//                found = found || stmts[idx].equals(lastBatchStmt);
//                idx++;
//            } // FOR
//            if (is_last) assertTrue(StringUtil.join("\n", queries), found); 
//                
//            MarkovEstimate est = t_estimator.executeQueries(state, stmts, partitions, true);
//            assertNotNull(est);
//            
//            for (Integer partition : catalogContext.getAllPartitionIds()) {
//                String debug = String.format("Batch %02d / Partition %02d\n%s",
//                                             est.getBatchId(), partition, est.toString());
//                assertTrue(debug, est.isFinishProbabilitySet(partition));
//                assertTrue(debug, est.isWriteProbabilitySet(partition));
//                assertTrue(debug, est.isReadOnlyProbabilitySet(partition));
//                
//                if (touched.contains(partition)) {
//                    assertFalse(debug, est.isFinishPartition(thresholds, partition));
//                    assertTrue(debug, est.isWritePartition(thresholds, partition));
//                } else {
//                    assertTrue(debug, est.isFinishPartition(thresholds, partition));
//                }
//            } // FOR
//        } // FOR 
//    }
//    
//    /**
//     * testProcessTransactionTrace
//     */
//    @Test
//    public void testProcessTransactionTrace() throws Exception {
//        TransactionTrace txn_trace = CollectionUtil.first(workload.getTransactions());
//        assertNotNull(txn_trace);
//        MarkovEstimatorState s = this.t_estimator.processTransactionTrace(txn_trace);
//        assertNotNull(s);
//        
//        MarkovEstimate initialEst = s.getInitialEstimate();
//        assertNotNull(initialEst);
//        assertTrue(initialEst.toString(), initialEst.isInitialized());
//        assertTrue(initialEst.toString(), initialEst.isSinglePartitionProbabilitySet());
//        assertTrue(initialEst.toString(), initialEst.isAbortProbabilitySet());
//        assertTrue(initialEst.toString(), initialEst.getSinglePartitionProbability() < 1.0f);
//        assertTrue(initialEst.toString(), initialEst.isConfidenceCoefficientSet());
//        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() >= 0f);
//        assertTrue(initialEst.toString(), initialEst.getConfidenceCoefficient() <= 1f);
//        assertTrue(initialEst.toString(), initialEst.getMarkovPath().isEmpty() == false);
//        
//        // We should have an MarkovEstimate for each batch
//        assertEquals(txn_trace.getBatchCount(), s.getEstimateCount());
//        List<TransactionEstimate> estimates = s.getEstimates();
//        for (int i = 0, cnt = txn_trace.getBatchCount(); i < cnt; i++) {
//            List<QueryTrace> queries = txn_trace.getBatchQueries(i);
//            assertFalse(queries.isEmpty());
//            
//            MarkovEstimate est = (MarkovEstimate)estimates.get(i);
//            assertNotSame(initialEst, est);
//            assertNotNull(est);
//            assertTrue(est.toString(), est.isSinglePartitionProbabilitySet());
//            assertTrue(est.toString(), est.isAbortProbabilitySet());
//            assertTrue(est.toString(), est.getSinglePartitionProbability() < 1.0f);
//            assertTrue(est.toString(), est.isConfidenceCoefficientSet());
//            assertTrue(est.toString(), est.getConfidenceCoefficient() >= 0f);
//            assertTrue(est.toString(), est.getConfidenceCoefficient() <= 1f);
//            assertTrue(est.toString(), est.getMarkovPath().isEmpty() == false);
//            
//            // The last vertex in each MarkovEstimate should correspond to the last query in each batch
//            MarkovVertex last_v = est.getVertex();
//            assertNotNull(last_v);
//            System.err.println("LAST VERTEX: " + last_v);
//            assertEquals(CollectionUtil.last(queries).getCatalogItem(catalog_db), last_v.getCatalogItem());
//        } // FOR
//    }
}
