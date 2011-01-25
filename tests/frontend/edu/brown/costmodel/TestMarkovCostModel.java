package edu.brown.costmodel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovGraphsContainer;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.Vertex;
import edu.brown.markov.TransactionEstimator.State;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.MultiPartitionTxnFilter;
import edu.brown.workload.filters.ProcParameterArraySizeFilter;
import edu.brown.workload.filters.ProcParameterValueFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovCostModel extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 1000;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 5;

    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    private static ParameterCorrelations correlations;
    private static MarkovCostModel costmodel;
    private static Procedure catalog_proc;
    private final Random rand = new Random();
    
    private TransactionTrace txn_trace;
    private State txn_state;
    private List<Vertex> estimated_path;
    private List<Vertex> actual_path;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);

        if (markovs == null) {
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            
            File file = this.getCorrelationsFile(ProjectType.TPCC);
            correlations = new ParameterCorrelations();
            correlations.load(file.getAbsolutePath(), catalog_db);

            file = this.getWorkloadFile(ProjectType.TPCC, "100w.large");
            workload = new Workload(catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            Workload.Filter filter = new ProcedureNameFilter()
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new ProcParameterValueFilter().include(1, new Long(5))) // D_ID
                    .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new MultiPartitionTxnFilter(p_estimator))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file.getAbsolutePath(), catalog_db, filter);
            // assertEquals(WORKLOAD_XACT_LIMIT, workload.getTransactionCount());

            // for (TransactionTrace xact : workload.getTransactions()) {
            // System.err.println(xact + ": " + p_estimator.getAllPartitions(xact));
            // }

            // Generate MarkovGraphs per base partition
//            file = this.getMarkovFile(ProjectType.TPCC);
//            markovs = MarkovUtil.load(catalog_db, file.getAbsolutePath());
            markovs = MarkovUtil.createBasePartitionGraphs(catalog_db, workload, p_estimator);
            assertNotNull(markovs);
            
            // And then populate the MarkovCostModel
            costmodel = new MarkovCostModel(catalog_db, p_estimator);
            for (Entry<Integer, Map<Procedure, MarkovGraph>> e : markovs.entrySet()) {
                TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, correlations);
                t_estimator.addMarkovGraphs(e.getValue());
                costmodel.addTransactionEstimator(e.getKey(), t_estimator);
            } // FOR
        }
        
        // Take a TransactionTrace and throw it at the estimator to get our path info
        this.txn_trace = workload.getTransactions().get(0);
        assertNotNull(this.txn_trace);
        int base_partition = p_estimator.getBasePartition(txn_trace);
        
        TransactionEstimator t_estimator = costmodel.getTransactionEstimator(base_partition);
        assertNotNull(t_estimator);
        this.txn_state = t_estimator.processTransactionTrace(txn_trace);
        assertNotNull(this.txn_state);
        
        this.estimated_path = this.txn_state.getEstimatedPath();
        assertNotNull(this.estimated_path);
        assert(this.estimated_path.isEmpty() == false);
        this.actual_path = this.txn_state.getActualPath();
        assertNotNull(this.actual_path);
        assert(this.actual_path.isEmpty() == false);
    }

    /**
     * testCompareSamePaths
     */
    @Test
    public void testCompareSamePaths() throws Exception {
        // We should always get a zero cost if we throw the same path at the cost model
        double cost = costmodel.comparePaths(this.actual_path, this.actual_path);
        assertEquals(0.0d, cost);
    }
    
    /**
     * testCompareDifferentPartitions
     */
    @Test
    public void testCompareDifferentPartitions() throws Exception {
        // Now let's add some random partitions into a new path
        List<Vertex> tester = new ArrayList<Vertex>(this.actual_path);
        for (int i = 0, cnt = tester.size(); i < cnt; i++) {
            if (rand.nextBoolean()) {
                Vertex v = new Vertex(tester.get(i));
                v.partitions.add(rand.nextInt(NUM_PARTITIONS));
                tester.set(i, v);
            }
        } // FOR
        double cost = costmodel.comparePaths(tester, this.actual_path);
        assert(cost > 0);
    }
    
    /**
     * testCompareIncompletePath
     */
    @Test
    public void testCompareIncompletePath() throws Exception {
        // Then make sure that our cost model can handle paths where the estimated path isn't complete
        List<Vertex> tester = new ArrayList<Vertex>(this.actual_path);
        tester.removeAll(this.actual_path.subList(this.actual_path.size() - 5, this.actual_path.size()));
        double cost = costmodel.comparePaths(tester, this.actual_path);
        // assert(cost > 0);
     }
}