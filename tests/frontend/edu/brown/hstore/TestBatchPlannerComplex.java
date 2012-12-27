package edu.brown.hstore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.BackendTarget;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.seats.SEATSProjectBuilder;
import edu.brown.benchmark.seats.procedures.DeleteReservation;
import edu.brown.benchmark.seats.procedures.LoadConfig;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.ClassUtil;

/**
 * 
 * @author pavlo
 */
public class TestBatchPlannerComplex extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = LoadConfig.class;
    private static final int NUM_PARTITIONS = 4;
    private static final int BASE_PARTITION = 0;
    private static final long TXN_ID = 123l;
    private static final long CLIENT_HANDLE = Long.MAX_VALUE;

    private SQLStmt batch[];
    private ParameterSet args[];
    
    private MockPartitionExecutor executor;
    private FastIntHistogram touched_partitions = new FastIntHistogram();
    private Procedure catalog_proc;
    private BatchPlanner planner;

    private final AbstractProjectBuilder builder = new SEATSProjectBuilder() {
        {
            this.addProcedure(BatchPlannerConflictProc.class);
            this.addAllDefaults();
        }
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(this.builder);
        this.addPartitions(NUM_PARTITIONS);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        this.batch = new SQLStmt[this.catalog_proc.getStatements().size()];
        this.args = new ParameterSet[this.batch.length];
        int i = 0;
        for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
            this.batch[i] = new SQLStmt(catalog_stmt);
            this.args[i] = ParameterSet.EMPTY;
            i++;
        } // FOR

        VoltProcedure volt_proc = ClassUtil.newInstance(TARGET_PROCEDURE, new Object[0], new Class<?>[0]);
        assert(volt_proc != null);
        this.executor = new MockPartitionExecutor(BASE_PARTITION, catalog, p_estimator);
        volt_proc.globalInit(this.executor, catalog_proc, BackendTarget.NONE, null, p_estimator);
        
        this.planner = new BatchPlanner(this.batch, this.catalog_proc, p_estimator);
    }
    
    private BatchPlan getPlan() {
        this.touched_partitions.clear();
        BatchPlan plan = planner.plan(
                            TXN_ID,
                            CLIENT_HANDLE,
                            0,
                            catalogContext.getAllPartitionIds(),
                            false,
                            this.touched_partitions,
                            this.args);
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
        return (plan);
    }

    /**
     * testGetPlanGraph
     */
    public void testGetPlanGraph() throws Exception {
        BatchPlanner.PlanGraph graph = getPlan().getPlanGraph();
        assertNotNull(graph);
        
        // Make sure that only PlanVertexs with input dependencies have a child in the graph
        for (BatchPlanner.PlanVertex v : graph.getVertices()) {
            assertNotNull(v);
            if (v.input_dependency_id == HStoreConstants.NULL_DEPENDENCY_ID) {
                assertEquals(0, graph.getSuccessorCount(v));
            } else {
                assertEquals(1, graph.getSuccessorCount(v));
            }
        } // FOR
        
//        GraphVisualizationPanel.createFrame(graph, GraphVisualizationPanel.makeVertexObserver(graph)).setVisible(true);
//        ThreadUtil.sleep(1000000);
    }
    
    /**
     * testFragmentIds
     */
    public void testFragmentIds() throws Exception {
        catalog_proc = this.getProcedure(DeleteReservation.class);
        
        // Make sure that PlanFragment ids in each WorkFragment only
        // belong to the Procedure
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            batch = new SQLStmt[] { new SQLStmt(catalog_stmt) };
            args = new ParameterSet[] {
                    new ParameterSet(this.randomStatementParameters(catalog_stmt))
            };
            this.planner = new BatchPlanner(this.batch, this.catalog_proc, p_estimator);
            this.touched_partitions.clear();
            BatchPlan plan = this.getPlan();
        
            List<WorkFragment.Builder> builders = new ArrayList<WorkFragment.Builder>();
            plan.getWorkFragmentsBuilders(TXN_ID, builders);
            assertFalse(builders.isEmpty());
        
            for (WorkFragment.Builder builder : builders) {
                assertNotNull(builder);
                for (int frag_id : builder.getFragmentIdList()) {
                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalog_proc, frag_id);
                    assertNotNull(catalog_frag);
                    assertEquals(catalog_frag.fullName(), catalog_stmt, catalog_frag.getParent());
                } // FOR
    //            System.err.println(pf);
            } // FOR
        } // FOR
    }
    
    /**
     * testFragmentOrder
     */
    public void testFragmentOrder() throws Exception {
        Procedure catalog_proc = this.getProcedure(BatchPlannerConflictProc.class);
        
        // Create a big batch and make sure that the fragments are in the correct order
        Statement stmts[] = new Statement[]{
            catalog_proc.getStatements().getIgnoreCase("ReplicatedInsert"),
            catalog_proc.getStatements().getIgnoreCase("ReplicatedSelect")
        };
        SQLStmt batch[] = new SQLStmt[stmts.length];
        ParameterSet params[] = new ParameterSet[stmts.length];
        for (int i = 0; i < stmts.length; i++) {
            batch[i] = new SQLStmt(stmts[i]);
            params[i] = new ParameterSet(this.randomStatementParameters(stmts[i]));
        } // FOR
        
        BatchPlanner planner = new BatchPlanner(batch, catalog_proc, p_estimator);
        this.touched_partitions.clear();
        BatchPlan plan = planner.plan(TXN_ID,
                                      CLIENT_HANDLE,
                                      BASE_PARTITION,
                                      catalogContext.getAllPartitionIds(),
                                      false,
                                      this.touched_partitions,
                                      params);
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
        
        List<WorkFragment.Builder> builders = new ArrayList<WorkFragment.Builder>();
        plan.getWorkFragmentsBuilders(TXN_ID, builders);
        assertFalse(builders.isEmpty());

        List<Statement> batchStmtOrder = new ArrayList<Statement>();
        boolean first = true;
        Statement last = null;
        for (WorkFragment.Builder builder : builders) {
            assertNotNull(builder);
            for (int frag_id : builder.getFragmentIdList()) {
                PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalog_proc, frag_id);
                assertNotNull(catalog_frag);
                Statement current = catalog_frag.getParent();
                if (last == null || last.equals(current) == false) {
                    batchStmtOrder.add(current);
                }
                last = current;
                
                // Make sure that the select doesn't appear before we execute the inserts
                if (first) assertNotSame(stmts[1], current);
                first = false;
            } // FOR
        } // FOR
    }
    
    /**
     * testBuildWorkFragments
     */
    public void testBuildWorkFragments() throws Exception {
        List<WorkFragment.Builder> builders = new ArrayList<WorkFragment.Builder>();
        BatchPlan plan = this.getPlan();
        plan.getWorkFragmentsBuilders(TXN_ID, builders);
        assertFalse(builders.isEmpty());
        
        for (WorkFragment.Builder builder : builders) {
            assertNotNull(builder);
//            System.err.println(pf);
            
            // If this WorkFragment is not for the base partition, then
            // we should make sure that it only has distributed queries...
            if (builder.getPartitionId() != BASE_PARTITION) {
                for (int frag_id : builder.getFragmentIdList()) {
                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalog, frag_id);
                    assertNotNull(catalog_frag);
                    Statement catalog_stmt = catalog_frag.getParent();
                    assertNotNull(catalog_stmt);
                    assert(catalog_stmt.getMs_fragments().contains(catalog_frag));
                } // FOR
            }
            
            // The InputDepId for all WorkFragments should always be the same
            Set<Integer> all_ids = new HashSet<Integer>(builder.getInputDepIdList());
            assertEquals(builder.toString(), 1, all_ids.size());
            
//            System.err.println(StringUtil.SINGLE_LINE);
        } // FOR
    } // FOR
}