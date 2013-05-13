/**
 * 
 */
package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.MockPartitionExecutor;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class TestDependencyTracker extends BaseTestCase {

    private static final Long TXN_ID = 1000l;
    private static final int NUM_PARTITIONS = 2;
    private static final int BASE_PARTITION = 0;
    
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final String TARGET_STATEMENT = "getStockInfo";
    

    private HStoreSite hstore_site;
    private PartitionExecutor executor;
    private DependencyTracker depTracker;
    private DependencyTracker.Debug depTrackerDbg;
    
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    private LocalTransaction ts;
    private final List<WorkFragment.Builder> ftasks = new ArrayList<WorkFragment.Builder>();
    private final FastIntHistogram touchedPartitions = new FastIntHistogram();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        PartitionEstimator p_estimator = new PartitionEstimator(catalogContext);
        this.executor = new MockPartitionExecutor(0, catalogContext.catalog, p_estimator);
        assertNotNull(this.executor);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.catalog_stmt = this.getStatement(catalog_proc, TARGET_STATEMENT);

        Partition catalog_part = catalogContext.getPartitionById(BASE_PARTITION);
        assertNotNull(catalog_part);
        this.hstore_site = HStore.initialize(catalogContext, ((Site)catalog_part.getParent()).getId(), HStoreConf.singleton());
        this.hstore_site.addPartitionExecutor(BASE_PARTITION, executor);
        this.depTracker = hstore_site.getDependencyTracker(BASE_PARTITION);
        this.depTrackerDbg = this.depTracker.getDebugContext();
        
        this.ts = new LocalTransaction(hstore_site);
        this.ts.testInit(TXN_ID,
                         BASE_PARTITION,
                         null,
                         catalogContext.getAllPartitionIds(), this.getProcedure(TARGET_PROCEDURE));
        this.ts.initializePrefetch();
        this.depTracker.addTransaction(ts);
        assertNull(this.ts.getCurrentRoundState(BASE_PARTITION));
    }
    
    @Override
    protected void tearDown() throws Exception {
        this.depTracker.removeTransaction(this.ts);
    }
    
    /**
     * testPrefetch
     */
    @Test
    public void testPrefetch() throws Exception {
        final ParameterSet params[] = { new ParameterSet(10001, BASE_PARTITION+1) };
        final SQLStmt batch[] = { new SQLStmt(catalog_stmt) };
        final long undoToken = 1000;

        this.ftasks.clear();
        BatchPlanner planner = new BatchPlanner(batch, catalog_proc, p_estimator);
        planner.setPrefetchFlag(true);
        BatchPlan plan = planner.plan(TXN_ID,
                                      BASE_PARTITION,
                                      catalogContext.getAllPartitionIds(),
                                      false,
                                      this.touchedPartitions,
                                      params);
        plan.getWorkFragmentsBuilders(TXN_ID, this.ftasks);
        
        for (WorkFragment.Builder fragment : this.ftasks) {
            assertTrue(fragment.getPrefetch());
            this.depTracker.addPrefetchWorkFragment(this.ts, fragment);
        } // FOR
        assertEquals(this.ftasks.size(), this.depTrackerDbg.getPrefetchCounter(this.ts));
        
        
        this.ts.initRound(BASE_PARTITION, undoToken);
        assertEquals(AbstractTransaction.RoundState.INITIALIZED, this.ts.getCurrentRoundState(BASE_PARTITION));
        assertNotNull(this.ts.getLastUndoToken(BASE_PARTITION));
        assertEquals(undoToken, this.ts.getLastUndoToken(BASE_PARTITION));
        //System.err.println(this.ts);
    }
    

}