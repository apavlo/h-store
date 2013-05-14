package edu.brown.hstore;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.mapreduce.procedures.MockMapReduce;
import edu.brown.hashing.DefaultHasher;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public class TestBatchPlannerMapReduce extends BaseTestCase {

    private static final Class<? extends VoltProcedure> MULTISITE_PROCEDURE = MockMapReduce.class;
    private static final String MULTISITE_STATEMENT = "mapInputQuery";
    private static final Object MULTISITE_PROCEDURE_ARGS[] = {
        1
    };
    
    private static final Long TXN_ID = 1000l;
    private static final int LOCAL_PARTITION = 0;
    private static final int NUM_PARTITIONS = 10;
    
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    private SQLStmt batch[];
    private ParameterSet args[];
    private int stmtCounters[];
    private FastIntHistogram touched_partitions = new FastIntHistogram();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.MAPREDUCE);
        this.addPartitions(NUM_PARTITIONS);
        p_estimator = new PartitionEstimator(catalogContext, new DefaultHasher(catalogContext, NUM_PARTITIONS));
    }
 
    private void init(Class<? extends VoltProcedure> volt_proc, String stmt_name, Object raw_args[]) {
        this.catalog_proc = this.getProcedure(volt_proc);
        assertNotNull(this.catalog_proc);
        this.catalog_stmt = this.catalog_proc.getStatements().get(stmt_name);
        assertNotNull(this.catalog_stmt);
        assertTrue(this.catalog_stmt.fullName(), this.catalog_stmt.getHas_singlesited());
        CatalogMap<PlanFragment> fragments = this.catalog_stmt.getFragments();

        // Create a SQLStmt batch
        this.batch = new SQLStmt[] { new SQLStmt(this.catalog_stmt, fragments) };
        this.args = new ParameterSet[] { VoltProcedure.getCleanParams(this.batch[0], raw_args) };
        this.stmtCounters = new int[]{ 0 };
    }
    
    /**
     * testForceSinglePartitionPlan
     */
    public void testForceSinglePartitionPlan() throws Exception {
        this.init(MULTISITE_PROCEDURE, MULTISITE_STATEMENT, MULTISITE_PROCEDURE_ARGS);
        BatchPlanner batchPlan = new BatchPlanner(batch, this.catalog_proc, p_estimator, true);
        BatchPlanner.BatchPlan plan = batchPlan.plan(TXN_ID, LOCAL_PARTITION, PartitionSet.singleton(LOCAL_PARTITION), this.touched_partitions, this.args);
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
        
        assertNotNull(plan);
        assertFalse(plan.hasMisprediction());
        
        List<WorkFragment.Builder> tasks = new ArrayList<WorkFragment.Builder>(); 
        plan.getWorkFragmentsBuilders(TXN_ID, this.stmtCounters, tasks);
        int local_frags = TestBatchPlanner.getLocalFragmentCount(tasks, LOCAL_PARTITION);
        int remote_frags = TestBatchPlanner.getRemoteFragmentCount(tasks, LOCAL_PARTITION);
        
        System.err.println(plan);
        System.err.println("Fragments: " + tasks);
        
        assertTrue(plan.isLocal());
        assertTrue(plan.isSingleSited());
        assertEquals(1, local_frags);
        assertEquals(0, remote_frags);
    }
    
}
