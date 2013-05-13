/**
 * 
 */
package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.MockPartitionExecutor;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class TestDependencyTrackerPrefetch extends BaseTestCase {

    private static final Long TXN_ID = 1000l;
    private static final int NUM_PARTITIONS = 2;
    private static final int BASE_PARTITION = 0;
    private static final int REMOTE_PARTITION = BASE_PARTITION + 1;
    
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final String TARGET_STATEMENT = "getStockInfo";
    

    private HStoreSite hstore_site;
    private PartitionExecutor executor;
    private DependencyTracker depTracker;
    private DependencyTracker.Debug depTrackerDbg;
    
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    private LocalTransaction ts;
    private BatchPlan plan;
    private VoltTable prefetchResult;
    
    private WorkFragment.Builder prefetchFragment;
    private final FastIntHistogram touchedPartitions = new FastIntHistogram();
    private final ParameterSet prefetchParams[] = { new ParameterSet(10001, BASE_PARTITION+1) };
    private final int prefetchParamsHash[] = new int[this.prefetchParams.length];
    private final SQLStmt prefetchBatch[] = new SQLStmt[this.prefetchParams.length];
    private final int prefetchStmtIndex = 1;
    private final long undoToken = 1000;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        this.executor = new MockPartitionExecutor(0, catalogContext, p_estimator);
        assertNotNull(this.executor);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.catalog_proc.setPrefetchable(true);
        this.catalog_stmt = this.getStatement(catalog_proc, TARGET_STATEMENT);
        this.catalog_stmt.setPrefetchable(true);
        
        Collection<Column> outputCols = PlanNodeUtil.getOutputColumnsForStatement(this.catalog_stmt);
        this.prefetchResult = CatalogUtil.getVoltTable(outputCols);
        this.prefetchResult.addRow(VoltTableUtil.getRandomRow(this.prefetchResult));
        for (int i = 0; i < this.prefetchParamsHash.length; i++) {
            this.prefetchBatch[i] = new SQLStmt(this.catalog_stmt);
            this.prefetchParamsHash[i] = this.prefetchParams[i].hashCode();
        } // FOR

        Partition catalog_part = catalogContext.getPartitionById(BASE_PARTITION);
        assertNotNull(catalog_part);
        this.hstore_site = HStore.initialize(catalogContext, ((Site)catalog_part.getParent()).getId(), HStoreConf.singleton());
        this.hstore_site.addPartitionExecutor(BASE_PARTITION, executor);
        this.depTracker = hstore_site.getDependencyTracker(BASE_PARTITION);
        this.depTrackerDbg = this.depTracker.getDebugContext();
        
        // Create a BatchPlan for our batch
        BatchPlanner planner = new BatchPlanner(this.prefetchBatch, this.catalog_proc, p_estimator);
        planner.setPrefetchFlag(true);
        this.plan = planner.plan(TXN_ID,
                                 BASE_PARTITION,
                                 catalogContext.getAllPartitionIds(),
                                 this.touchedPartitions,
                                 this.prefetchParams);
        List<WorkFragment.Builder> ftasks = new ArrayList<WorkFragment.Builder>();
        this.plan.getWorkFragmentsBuilders(TXN_ID, ftasks);
        this.prefetchFragment = CollectionUtil.first(ftasks);
        this.prefetchFragment.setStmtIndex(0, this.prefetchStmtIndex);
        assert(this.prefetchFragment.getFragmentIdCount() > 0);
        assertTrue(this.prefetchFragment.getPrefetch());
        assertEquals(REMOTE_PARTITION, this.prefetchFragment.getPartitionId());
        
        this.ts = new LocalTransaction(hstore_site);
        this.ts.testInit(TXN_ID,
                         BASE_PARTITION,
                         null,
                         catalogContext.getAllPartitionIds(),
                         this.catalog_proc);
        this.ts.initializePrefetch();
        this.depTracker.addTransaction(ts);
        assertNull(this.ts.getCurrentRoundState(BASE_PARTITION));
    }
    
    /**
     * testAddPrefetchResultBefore
     */
    @Test
    public void testAddPrefetchResultBefore() throws Exception {
        // This tests when the result for the prefetch queries arrive in
        // the DependencyTracker *before* the txn actually requests it.
        // When the query does get added into the tracker, it should get
        // immediately released.
        
        // Tell the DependencyTracker that we're going to prefetch all of the WorkFragments
        this.depTracker.addPrefetchWorkFragment(this.ts, this.prefetchFragment);
        assertEquals(1, this.depTrackerDbg.getPrefetchCounter(this.ts));
        
        // Then add the result into the DependencyTracker
        this.depTracker.addPrefetchResult(this.ts,
                                          this.prefetchStmtIndex,
                                          this.prefetchFragment.getFragmentId(0),
                                          REMOTE_PARTITION,
                                          this.prefetchParamsHash[0],
                                          this.prefetchResult);
        
        // Now if we add in the same query again, it should automatically pick up the result
        SQLStmt nextBatch[] = {
            new SQLStmt(this.getStatement(this.catalog_proc, "getItemInfo")),
            new SQLStmt(this.catalog_stmt)
        };
        ParameterSet nextParams[] = {
            new ParameterSet(12345l),
            new ParameterSet(this.prefetchParams[0].toArray())
        };
        
        // Initialize the txn to simulate that it has started
        this.ts.initFirstRound(undoToken, nextBatch.length);
        assertEquals(AbstractTransaction.RoundState.INITIALIZED, this.ts.getCurrentRoundState(BASE_PARTITION));
        assertNotNull(this.ts.getLastUndoToken(BASE_PARTITION));
        assertEquals(undoToken, this.ts.getLastUndoToken(BASE_PARTITION));
        
        BatchPlanner nextPlanner = new BatchPlanner(nextBatch, this.catalog_proc, p_estimator);
        BatchPlan nextPlan = nextPlanner.plan(TXN_ID,
                                              BASE_PARTITION,
                                              catalogContext.getAllPartitionIds(),
                                              this.touchedPartitions,
                                              nextParams);
        List<WorkFragment.Builder> ftasks = new ArrayList<WorkFragment.Builder>();
        nextPlan.getWorkFragmentsBuilders(TXN_ID, ftasks);
        for (WorkFragment.Builder fragment : ftasks) {
            this.depTracker.addWorkFragment(this.ts, fragment);
        } // FOR
        
        // We only need to add the query result for the first query 
        // and then we should get immediately unblocked
        this.ts.startRound(BASE_PARTITION);
        CountDownLatch latch = this.depTracker.getDependencyLatch(this.ts);
        assertTrue(latch.getCount() > 0);
 
        WorkFragment.Builder fragment = CollectionUtil.first(ftasks);
        Collection<Column> outputCols = PlanNodeUtil.getOutputColumnsForStatement(nextBatch[0].getStatement());
        VoltTable result = CatalogUtil.getVoltTable(outputCols);
        result.addRow(VoltTableUtil.getRandomRow(result));
        this.depTracker.addResult(this.ts,
                                  fragment.getPartitionId(),
                                  fragment.getOutputDepId(0),
                                  result);
        assertEquals(0, latch.getCount());
        
        VoltTable results[] = this.depTracker.getResults(this.ts);
        assertEquals(nextBatch.length, results.length);
        assertEquals(this.prefetchResult, CollectionUtil.last(results));
    }
    
    /**
     * testAddPrefetchResultAfter
     */
    @Test
    public void testAddPrefetchResultAfter() throws Exception {
        // This tests when the result for the prefetch queries arrive in
        // the DependencyTracker *after* the txn actually requests it.
        // When the query does get added into the tracker, it should get
        // immediately released.
        
        // Tell the DependencyTracker that we're going to prefetch all of the WorkFragments
        this.depTracker.addPrefetchWorkFragment(this.ts, this.prefetchFragment);
        assertEquals(1, this.depTrackerDbg.getPrefetchCounter(this.ts));
        
        // Now if we add in the same query again, it should automatically pick up the result
        SQLStmt nextBatch[] = {
            new SQLStmt(this.getStatement(this.catalog_proc, "getItemInfo")),
            new SQLStmt(this.catalog_stmt)
        };
        ParameterSet nextParams[] = {
            new ParameterSet(12345l),
            new ParameterSet(this.prefetchParams[0].toArray())
        };
        
        // Initialize the txn to simulate that it has started
        this.ts.initFirstRound(undoToken, nextBatch.length);
        assertEquals(AbstractTransaction.RoundState.INITIALIZED, this.ts.getCurrentRoundState(BASE_PARTITION));
        assertNotNull(this.ts.getLastUndoToken(BASE_PARTITION));
        assertEquals(undoToken, this.ts.getLastUndoToken(BASE_PARTITION));
        
        // And invoke the first batch
        BatchPlanner nextPlanner = new BatchPlanner(nextBatch, this.catalog_proc, p_estimator);
        BatchPlan nextPlan = nextPlanner.plan(TXN_ID,
                                              BASE_PARTITION,
                                              catalogContext.getAllPartitionIds(),
                                              this.touchedPartitions,
                                              nextParams);
        List<WorkFragment.Builder> ftasks = new ArrayList<WorkFragment.Builder>();
        nextPlan.getWorkFragmentsBuilders(TXN_ID, ftasks);
        for (WorkFragment.Builder fragment : ftasks) {
            this.depTracker.addWorkFragment(this.ts, fragment);
        } // FOR
        
        // We only need to add the query result for the first query 
        // and then we should get immediately unblocked
        this.ts.startRound(BASE_PARTITION);
        CountDownLatch latch = this.depTracker.getDependencyLatch(this.ts);
        assertEquals(nextBatch.length, latch.getCount());
        WorkFragment.Builder fragment = CollectionUtil.first(ftasks);
        Collection<Column> outputCols = PlanNodeUtil.getOutputColumnsForStatement(nextBatch[0].getStatement());
        VoltTable result = CatalogUtil.getVoltTable(outputCols);
        result.addRow(VoltTableUtil.getRandomRow(result));
        this.depTracker.addResult(this.ts,
                                  fragment.getPartitionId(),
                                  fragment.getOutputDepId(0),
                                  result);
        assertEquals(nextBatch.length-1, latch.getCount());
        
        // Now add in the prefetch result
        // This should cause use to get unblocked now
        this.depTracker.addPrefetchResult(this.ts,
                                          this.prefetchStmtIndex,
                                          this.prefetchFragment.getFragmentId(0),
                                          REMOTE_PARTITION,
                                          this.prefetchParamsHash[0],
                                          this.prefetchResult);
        assertEquals(0, latch.getCount());
        
        VoltTable results[] = this.depTracker.getResults(this.ts);
        assertEquals(nextBatch.length, results.length);
        assertEquals(this.prefetchResult, CollectionUtil.last(results));
    }
    

}