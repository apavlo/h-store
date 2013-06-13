package edu.brown.costmodel;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.costmodel.MarkovCostModel.Penalty;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.MarkovVertex.Type;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.MultiPartitionTxnFilter;
import edu.brown.workload.filters.ProcParameterValueFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovCostModel extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 500;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 5;
    private static final EstimationThresholds thresholds = new EstimationThresholds();

    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    private static Procedure catalog_proc;
    private static MarkovEstimator t_estimator;

    private MarkovCostModel costmodel;
    private MarkovGraph markov;
    private TransactionTrace txn_trace;
    private MarkovEstimatorState txn_state;
    private MarkovEstimate initialEst; 
    private List<MarkovVertex> estimated_path;
    private List<MarkovVertex> actual_path;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);

        if (isFirstSetup()) {
            catalog_proc = this.getProcedure(TARGET_PROCEDURE);
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalogContext.catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new ProcParameterValueFilter().include(1, new Integer(5))) // D_ID
                    // .attach(new ProcParameterArraySizeFilter(CatalogUtil.getArrayProcParameters(catalog_proc).get(0), 10, ExpressionType.COMPARE_EQUAL))
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new MultiPartitionTxnFilter(p_estimator, false))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file, catalogContext.database, filter);
            
            // Make a copy that doesn't have the first TransactionTrace
            Workload clone = new Workload(catalogContext.catalog, new Filter() {
                private boolean first = true;
                @Override
                protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
                    if (element instanceof TransactionTrace && first) {
                        this.first = false;
                        return (FilterResult.SKIP);
                    }
                    return FilterResult.ALLOW;
                }
                @Override
                public String debugImpl() { return null; }
                @Override
                protected void resetImpl() { }
            }, workload);
            TransactionTrace txn0 = CollectionUtil.first(workload.getTransactions());
            assertNotNull(txn0);
            TransactionTrace txn1 = CollectionUtil.first(clone.getTransactions());
            assertNotNull(txn1);
            assert(txn0.getTransactionId() != txn1.getTransactionId());
            
            // assertEquals(WORKLOAD_XACT_LIMIT, workload.getTransactionCount());

            // for (TransactionTrace xact : workload.getTransactions()) {
            // System.err.println(xact + ": " + p_estimator.getAllPartitions(xact));
            // }

            // Generate MarkovGraphs per base partition
//            file = this.getMarkovFile(ProjectType.TPCC);
//            markovs = MarkovUtil.load(catalogContext.database, file.getAbsolutePath());
            markovs = MarkovGraphsContainerUtil.createBasePartitionMarkovGraphsContainer(catalogContext, clone, p_estimator);
            assertNotNull(markovs);
            
            // And then populate the MarkovCostModel
            t_estimator = new MarkovEstimator(catalogContext, p_estimator, markovs);
        }
        
        this.costmodel = new MarkovCostModel(catalogContext, p_estimator, t_estimator, thresholds);
        
        // Take a TransactionTrace and throw it at the estimator to get our path info
        this.txn_trace = CollectionUtil.first(workload.getTransactions());
        assertNotNull(this.txn_trace);
        
        this.txn_state = t_estimator.processTransactionTrace(txn_trace);
        assertNotNull(this.txn_state);
        this.markov = markovs.get(BASE_PARTITION, catalog_proc);
        assertNotNull(this.markov);
        
        this.initialEst = this.txn_state.getInitialEstimate();
        assertNotNull(this.initialEst);
        this.estimated_path = this.initialEst.getMarkovPath();
        assertNotNull(this.estimated_path);
        assert(this.estimated_path.isEmpty() == false);
        this.actual_path = this.txn_state.getActualPath();
        assertNotNull(this.actual_path);
        assert(this.actual_path.isEmpty() == false);
    }

    /**
     * testComparePathsFast
     */
    @Test
    public void testComparePathsFast() throws Exception {
        List<MarkovVertex> clone = new ArrayList<MarkovVertex>(this.actual_path);
        
        // At the very least the exact clone should be equal
        assert(costmodel.comparePathsFast(clone, this.actual_path));
        
        // Test to make sure that it catches when one path aborts while the other commits
        this.actual_path.add(markov.getCommitVertex());
        clone.add(markov.getCommitVertex());
        assert(costmodel.comparePathsFast(clone, this.actual_path));
        clone.set(clone.size()-1, markov.getAbortVertex());
        assertFalse(costmodel.comparePathsFast(clone, this.actual_path));
        this.actual_path.remove(this.actual_path.size()-1);
        
        // Now check to make sure that it catches when the read/write differ
        clone.get(1).getPartitions().add(Integer.MAX_VALUE);
        assertEquals(false, costmodel.comparePathsFast(clone, this.actual_path));
        clone.get(1).getPartitions().remove(Integer.MAX_VALUE);
    }
    
    /**
     * testComparePathsFull_Penalty1
     */
    @Test
    public void testComparePathsFull_Penalty1() throws Exception {
        // Grab the actual path in the State and cut it off after the first
        // invocation of neworder.getCustomer().
        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getCustomer");
        List<MarkovVertex> actual = this.txn_state.getActualPath();
        List<MarkovVertex> orig = new ArrayList<MarkovVertex>(actual);
        actual.clear();
        for (MarkovVertex mv : orig) {
            actual.add(mv);
            if (mv.getCatalogItemName().equalsIgnoreCase(catalog_stmt.getName())) {
                break;
            }
        } // FOR
        assertNotSame(orig.size(), actual.size());
        
        MarkovVertex abort_v = markov.getAbortVertex();
        assertEquals(Type.ABORT, abort_v.getType());
        actual.add(abort_v);
        
        // We have to call comparePathsFast first to setup some sets
        // We don't care what the outcome is here...
        this.costmodel.comparePathsFast(this.estimated_path, actual);
        
        // System.err.println(StringUtil.join("\n", actual));
        double cost = costmodel.comparePathsFull(this.txn_state);
        
        List<Penalty> penalties = costmodel.getLastPenalties();
        assertNotNull(penalties);
        System.err.println(String.format("COST=%.03f PENALTIES=%s", cost, penalties));
//        assert(penalties.contains(Penalty.UNUSED_READ_PARTITION_MULTI) ||
//               penalties.contains(Penalty.UNUSED_WRITE_PARTITION_MULTI)); 
    }
    
    /**
     * testComparePathsFull_Penalty2
     */
    @Test
    public void testComparePathsFull_Penalty2() throws Exception {
        // We have to call comparePathsFast first to setup some sets
        // We don't care what the outcome is here...
        this.costmodel.comparePathsFast(this.estimated_path, this.txn_state.getActualPath());
        
        // Remove all of the estimated read partitions except for one
        PartitionSet e_read_partitions = this.costmodel.getLastEstimatedReadPartitions();
        assertNotNull(e_read_partitions);
        Set<Integer> retain = (Set<Integer>)CollectionUtil.addAll(new HashSet<Integer>(), CollectionUtil.first(e_read_partitions)); 
        e_read_partitions.retainAll(retain);
        
        // Then add all of our partitions to the actual read partitions
        PartitionSet a_read_partitions = this.costmodel.getLastActualReadPartitions();
        a_read_partitions.addAll(catalogContext.getAllPartitionIds());
        
        double cost = this.costmodel.comparePathsFull(this.txn_state);
        
        List<Penalty> penalties = this.costmodel.getLastPenalties();
        assertNotNull(penalties);
        System.err.println(String.format("COST=%.03f PENALTIES=%s", cost, penalties));
        assert(penalties.contains(Penalty.MISSED_READ_PARTITION)); 
    }
    
    /**
     * testCompareIncompletePath
     */
    @Test
    public void testCompareIncompletePath() throws Exception {
        // Then make sure that our cost model can handle paths where the estimated path isn't complete
        List<MarkovVertex> tester = new ArrayList<MarkovVertex>(this.actual_path);
        tester.removeAll(this.actual_path.subList(this.actual_path.size() - 5, this.actual_path.size()));
        /* FIXME
        double cost = costmodel.comparePathsFast(tester, this.actual_path);
        */
        // assert(cost > 0);
     }
}