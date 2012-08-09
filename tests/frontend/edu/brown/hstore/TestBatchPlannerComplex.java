package edu.brown.hstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.BackendTarget;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
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
import edu.brown.statistics.Histogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.conf.HStoreConf;

public class TestBatchPlannerComplex extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = LoadConfig.class;
    private static final int NUM_PARTITIONS = 4;
    private static final int BASE_PARTITION = 0;
    private static final long TXN_ID = 123l;
    private static final long CLIENT_HANDLE = Long.MAX_VALUE;

    private SQLStmt batch[];
    private ParameterSet args[];
    
    private HStoreConf hstore_conf;
    private MockPartitionExecutor executor;
    private Histogram<Integer> touched_partitions;
    private Procedure catalog_proc;
    private BatchPlanner planner;

    public static class ConflictProc extends VoltProcedure {
        public final SQLStmt ReplicatedInsert = new SQLStmt("INSERT INTO COUNTRY VALUES (?, ?, ?, ?);");
        public final SQLStmt ReplicatedSelect = new SQLStmt("SELECT * FROM COUNTRY");
        
        public VoltTable[] run(long id, String name, String code2, String code3) {
            voltQueueSQL(ReplicatedInsert, id, name, code2, code3);
            voltQueueSQL(ReplicatedSelect);
            return voltExecuteSQL(true);
        }
    }
    
    private final AbstractProjectBuilder builder = new SEATSProjectBuilder() {
        {
            this.addAllDefaults();
            this.addProcedure(TestBatchPlannerComplex.ConflictProc.class);
        }
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(this.builder);
        this.addPartitions(NUM_PARTITIONS);
        this.touched_partitions = new Histogram<Integer>();
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.hstore_conf = HStoreConf.singleton();
        
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

//    /**
//     * testGetPlanGraph
//     */
//    public void testGetPlanGraph() throws Exception {
//        BatchPlanner.PlanGraph graph = getPlan().getPlanGraph();
//        assertNotNull(graph);
//        
//        // Make sure that only PlanVertexs with input dependencies have a child in the graph
//        for (BatchPlanner.PlanVertex v : graph.getVertices()) {
//            assertNotNull(v);
//            if (v.input_dependency_id == HStoreConstants.NULL_DEPENDENCY_ID) {
//                assertEquals(0, graph.getSuccessorCount(v));
//            } else {
//                assertEquals(1, graph.getSuccessorCount(v));
//            }
//        } // FOR
//        
////        GraphVisualizationPanel.createFrame(graph, GraphVisualizationPanel.makeVertexObserver(graph)).setVisible(true);
////        ThreadUtil.sleep(1000000);
//    }
//    
//    /**
//     * testFragmentIds
//     */
//    public void testFragmentIds() throws Exception {
//        catalog_proc = this.getProcedure(DeleteReservation.class);
//        
//        // Make sure that PlanFragment ids in each WorkFragment only
//        // belong to the Procedure
//        for (Statement catalog_stmt : catalog_proc.getStatements()) {
//            batch = new SQLStmt[] { new SQLStmt(catalog_stmt) };
//            args = new ParameterSet[] {
//                    new ParameterSet(this.makeRandomStatementParameters(catalog_stmt))
//            };
//            this.planner = new BatchPlanner(this.batch, this.catalog_proc, p_estimator);
//            this.touched_partitions.clear();
//            BatchPlan plan = this.getPlan();
//        
//            List<WorkFragment> fragments = new ArrayList<WorkFragment>();
//            plan.getWorkFragments(TXN_ID, fragments);
//            assertFalse(fragments.isEmpty());
//        
//            for (WorkFragment pf : fragments) {
//                assertNotNull(pf);
//                for (int frag_id : pf.getFragmentIdList()) {
//                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalog_proc, frag_id);
//                    assertNotNull(catalog_frag);
//                    assertEquals(catalog_frag.fullName(), catalog_stmt, catalog_frag.getParent());
//                } // FOR
//    //            System.err.println(pf);
//            } // FOR
//        } // FOR
//    }
    
    /**
     * testFragmentOrder
     */
    public void testFragmentOrder() throws Exception {
        Procedure catalog_proc = this.getProcedure(DeleteReservation.class);
        Map<Statement, SQLStmt> sqlStmts = new HashMap<Statement, SQLStmt>();
        
        // Create a big batch and make sure that the fragments are in the correct order
//        SQLStmt batch[] = new SQLStmt[hstore_conf.site.planner_max_batch_size];
        SQLStmt batch[] = new SQLStmt[5];
        ParameterSet params[] = new ParameterSet[batch.length];
        for (int i = 0; i < batch.length; i++) {
            Statement catalog_stmt = CollectionUtil.random(catalog_proc.getStatements());
            batch[i] = sqlStmts.get(catalog_stmt);
            if (batch[i] == null) {
                batch[i] = new SQLStmt(catalog_stmt);
                sqlStmts.put(catalog_stmt, batch[i]);
            }
            params[i] = new ParameterSet(this.makeRandomStatementParameters(catalog_stmt));
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
        
        List<WorkFragment> fragments = new ArrayList<WorkFragment>();
        plan.getWorkFragments(TXN_ID, fragments);
        assertFalse(fragments.isEmpty());

        List<Statement> batchStmtOrder = new ArrayList<Statement>();
        Statement last = null;
        for (WorkFragment pf : fragments) {
            assertNotNull(pf);
            for (int frag_id : pf.getFragmentIdList()) {
                PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalog_proc, frag_id);
                assertNotNull(catalog_frag);
                Statement current = catalog_frag.getParent();
                if (last == null || last.equals(current) == false) {
                    batchStmtOrder.add(current);
                }
                last = current;
            } // FOR
        } // FOR
        assertEquals(batch.length, batchStmtOrder.size());
        for (int i = 0; i < batch.length; i++) {
            assertEquals(batch[i], batchStmtOrder.get(i));
        } // FOR
    }
    
//    /**
//     * testBuildWorkFragments
//     */
//    public void testBuildWorkFragments() throws Exception {
//        List<WorkFragment> fragments = new ArrayList<WorkFragment>();
//        BatchPlan plan = this.getPlan();
//        plan.getWorkFragments(TXN_ID, fragments);
//        assertFalse(fragments.isEmpty());
//        
//        for (WorkFragment pf : fragments) {
//            assertNotNull(pf);
////            System.err.println(pf);
//            
//            // If this WorkFragment is not for the base partition, then
//            // we should make sure that it only has distributed queries...
//            if (pf.getPartitionId() != BASE_PARTITION) {
//                for (int frag_id : pf.getFragmentIdList()) {
//                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalog, frag_id);
//                    assertNotNull(catalog_frag);
//                    Statement catalog_stmt = catalog_frag.getParent();
//                    assertNotNull(catalog_stmt);
//                    assert(catalog_stmt.getMs_fragments().contains(catalog_frag));
//                } // FOR
//            }
//            
//            // The InputDepId for all WorkFragments should always be the same
//            Set<Integer> all_ids = new HashSet<Integer>();
//            for (WorkFragment.InputDependency input_dep_ids : pf.getInputDepIdList()) {
//                all_ids.addAll(input_dep_ids.getIdsList());
//            } // FOR
//            assertEquals(pf.toString(), 1, all_ids.size());
//            
////            System.err.println(StringUtil.SINGLE_LINE);
//        } // FOR
//    } // FOR
}
