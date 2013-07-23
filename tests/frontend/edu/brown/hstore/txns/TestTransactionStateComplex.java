/**
 * 
 */
package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.auctionmark.procedures.GetUserInfo;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.MockPartitionExecutor;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class TestTransactionStateComplex extends BaseTestCase {
    private static final long TXN_ID = 1000l;
    private static final long UNDO_TOKEN = 10l;
    
    private static final String TARGET_PROCEDURE = GetUserInfo.class.getSimpleName();
    private static final String TARGET_STATEMENT = "getWatchedItems";
    private static final int NUM_DUPLICATE_STATEMENTS = 1;
    
    private static final int NUM_PARTITIONS = 10;
    private static final int LOCAL_PARTITION = 1;

    private static final VoltTable.ColumnInfo FAKE_RESULTS_COLUMNS[] = new VoltTable.ColumnInfo[] {
        new VoltTable.ColumnInfo("ID", VoltType.INTEGER),
        new VoltTable.ColumnInfo("VAL", VoltType.STRING),
    };
    private static final VoltTable FAKE_RESULT = new VoltTable(FAKE_RESULTS_COLUMNS);
    
    private HStoreSite hstore_site;
    private PartitionExecutor executor;
    private BatchPlan plan;
    private List<WorkFragment.Builder> ftasks = new ArrayList<WorkFragment.Builder>();
    private DependencyTracker depTracker;
    private DependencyTracker.Debug depTrackerDbg;
    
    private FastIntHistogram touched_partitions = new FastIntHistogram();
    private LocalTransaction ts;
    private ListOrderedSet<Integer> dependency_ids = new ListOrderedSet<Integer>();
    private List<Integer> internal_dependency_ids = new ArrayList<Integer>();
    private List<Integer> output_dependency_ids = new ArrayList<Integer>();
    private List<WorkFragment.Builder> first_tasks = new ArrayList<WorkFragment.Builder>();
    private Map<Integer, Set<Integer>> dependency_partitions = new HashMap<Integer, Set<Integer>>();
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    
    SQLStmt batch[] = new SQLStmt[NUM_DUPLICATE_STATEMENTS];
    ParameterSet args[] = new ParameterSet[batch.length];
    int stmtCounters[] = new int[batch.length];

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
        this.addPartitions(NUM_PARTITIONS);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.catalog_stmt = this.getStatement(catalog_proc, TARGET_STATEMENT);
        
        this.executor = new MockPartitionExecutor(LOCAL_PARTITION, catalogContext, p_estimator);
        assertNotNull(executor);
        
        // Create a SQLStmt batch
        for (int i = 0; i < batch.length; i++) {
            Object raw_args[] = new Object[] {
                new Long(i + 1),    // U_ID
            };
            batch[i] = new SQLStmt(this.catalog_stmt, this.catalog_stmt.getMs_fragments());
            args[i] = VoltProcedure.getCleanParams(batch[i], raw_args);
            stmtCounters[i] = i;
        } // FOR
        
        Partition catalog_part = catalogContext.getPartitionById(LOCAL_PARTITION);
        hstore_site = HStore.initialize(catalogContext, ((Site)catalog_part.getParent()).getId(), HStoreConf.singleton());
        hstore_site.addPartitionExecutor(LOCAL_PARTITION, executor);
        this.depTracker = hstore_site.getDependencyTracker(LOCAL_PARTITION);
        this.depTrackerDbg = this.depTracker.getDebugContext();
        
        BatchPlanner batchPlan = new BatchPlanner(batch, catalog_proc, p_estimator);
        this.plan = batchPlan.plan(TXN_ID,
                                   LOCAL_PARTITION,
                                   catalogContext.getAllPartitionIds(),
                                   this.touched_partitions,
                                   args);
        this.plan.getWorkFragmentsBuilders(TXN_ID, stmtCounters, this.ftasks);
        assertFalse(ftasks.isEmpty());
        
        this.ts = new LocalTransaction(hstore_site);
        this.ts.testInit(TXN_ID,
                         LOCAL_PARTITION,
                         null,
                         catalogContext.getAllPartitionIds(),
                         this.catalog_proc);
        this.ts.markControlCodeExecuted();
        this.depTracker.addTransaction(this.ts);
    }

    /**
     * Add all of the FragmentTaskMessages from our BatchPlanner into the TransactionState
     * We will also populate our list of dependency ids
     */
    private void addFragments() {
        for (WorkFragment.Builder ftask : ftasks) {
//            System.err.println(ftask);
//            System.err.println("+++++++++++++++++++++++++++++++++++");
            this.depTracker.addWorkFragment(this.ts, ftask, this.args);
            for (int i = 0, cnt = ftask.getFragmentIdCount(); i < cnt; i++) {
                int dep_id = ftask.getOutputDepId(i);
                this.dependency_ids.add(dep_id);
                
                if (this.dependency_partitions.containsKey(dep_id) == false) {
                    this.dependency_partitions.put(dep_id, new HashSet<Integer>());
                }
                this.dependency_partitions.get(dep_id).add(ftask.getPartitionId());
                
                int input_dep_id = ftask.getInputDepId(i); 
                if (input_dep_id == HStoreConstants.NULL_DEPENDENCY_ID) {
                    this.first_tasks.add(ftask);
                } else {
                    this.internal_dependency_ids.add(input_dep_id);
                }
            } // FOR
        } // FOR
        for (int d_id : this.dependency_ids) {
            if (!this.internal_dependency_ids.contains(d_id)) this.output_dependency_ids.add(d_id);
        } // FOR
        assertFalse(this.output_dependency_ids.isEmpty());
        assertFalse(this.internal_dependency_ids.isEmpty());
    }
    
    /**
     * testTwoRoundQueryPlan
     */
    public void testTwoRoundQueryPlan() throws Exception {
        this.ts.initFirstRound(UNDO_TOKEN, NUM_DUPLICATE_STATEMENTS);
        this.addFragments();
        this.ts.startRound(LOCAL_PARTITION);
        
        // We want to add results for just one of the duplicated statements and make sure that
        // we only unblock one of them. First we need to find an internal dependency that has blocked tasks 
        Integer internal_d_id = this.internal_dependency_ids.get(0);
        assertNotNull(internal_d_id);
        DependencyInfo internal_dinfo = this.depTrackerDbg.getDependencyInfo(this.ts, internal_d_id);
        assertNotNull(internal_dinfo);
        
        // System.err.println(this.ts);
        
        // So for this test the query plan is a diamond, so we are going to add results in waves 
        // and make sure that the things get unblocked at the right time
        // (1) Add a result for the first output dependency
        assertEquals(1, this.first_tasks.size());
        WorkFragment.Builder first_ftask = CollectionUtil.first(this.first_tasks);
        assertNotNull(first_ftask);
        int partition = first_ftask.getPartitionId();
        int first_output_dependency_id = first_ftask.getOutputDepId(0);
        DependencyInfo first_dinfo = this.depTrackerDbg.getDependencyInfo(this.ts, first_output_dependency_id);
        assertNotNull(first_dinfo);
        assertEquals(NUM_PARTITIONS, first_dinfo.getBlockedWorkFragments().size());
        this.depTracker.addResult(this.ts, partition, first_output_dependency_id, FAKE_RESULT);
        assert(first_dinfo.hasTasksReleased());

        // (2) Now add outputs for each of the tasks that became unblocked in the previous step
        first_ftask = CollectionUtil.first(first_dinfo.getBlockedWorkFragments());
        DependencyInfo second_dinfo = this.depTrackerDbg.getDependencyInfo(this.ts, first_ftask.getOutputDepId(0));
        for (WorkFragment.Builder ftask : first_dinfo.getBlockedWorkFragments()) {
            assertFalse(second_dinfo.hasTasksReady());
            partition = ftask.getPartitionId();
            for (int i = 0, cnt = ftask.getFragmentIdCount(); i < cnt; i++) {
                int output_dependency_id = ftask.getOutputDepId(i);
                this.depTracker.addResult(this.ts, partition, output_dependency_id, FAKE_RESULT);
            } // FOR
        } // FOR
        assert(second_dinfo.hasTasksReleased());
    }
    
    
    /**
     * testAddResultsBeforeStart
     */
    public void testAddResultsBeforeStart() throws Exception {
        this.ts.initFirstRound(UNDO_TOKEN, NUM_DUPLICATE_STATEMENTS);
        this.addFragments();
        
        // We need to test to make sure that we don't get a CountDownLatch with the wrong count
        // if we start the round *after* a bunch of results have arrived.
        // Add a bunch of fake results
        Long marker = 1000l;
        List<Long> markers = new ArrayList<Long>();
        for (int dependency_id : this.dependency_ids) {
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                // If this dependency is meant to go back to the VoltProcedure, then
                // we want to add a row so that we can figure out whether we are getting
                // the results back in the right order
                if (!this.internal_dependency_ids.contains(dependency_id)) {
                    // Skip anything that isn't our local partition
                    if (partition != LOCAL_PARTITION) continue;
                    VoltTable copy = new VoltTable(FAKE_RESULTS_COLUMNS);
                    copy.addRow(marker, "XXXX");
                    this.depTracker.addResult(this.ts, partition, dependency_id, copy);
                    markers.add(marker++);
                // Otherwise just stuff in our fake result (if they actually need it)
                } else if (this.dependency_partitions.get(dependency_id).contains(partition)) {
                    this.depTracker.addResult(this.ts, partition, dependency_id, FAKE_RESULT);
                }
            } // FOR (partition)
        } // FOR (dependency ids)
        assertEquals(NUM_DUPLICATE_STATEMENTS, markers.size());

        this.ts.startRound(LOCAL_PARTITION);
        CountDownLatch latch = this.depTracker.getDependencyLatch(this.ts); 
        assertNotNull(latch);
        assertEquals(0, latch.getCount());
    }
    
    /**
     * testGetResults
     */
    public void testGetResults() throws Exception {
        this.ts.initFirstRound(UNDO_TOKEN, NUM_DUPLICATE_STATEMENTS);
        this.addFragments();
        this.ts.startRound(LOCAL_PARTITION);
//        System.err.println(this.ts);
//        System.err.println(this.internal_dependency_ids);

        // Add a bunch of fake results
        Long marker = 1000l;
        List<Long> markers = new ArrayList<Long>();
        for (int dependency_id : this.dependency_ids) {
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                
                // If this dependency is meant to go back to the VoltProcedure, then
                // we want to add a row so that we can figure out whether we are getting
                // the results back in the right order
                if (!this.internal_dependency_ids.contains(dependency_id)) {
                    // Skip anything that isn't our local partition
                    if (partition != LOCAL_PARTITION) {
                        continue;
                    }
                    
                    VoltTable copy = new VoltTable(FAKE_RESULTS_COLUMNS);
                    copy.addRow(marker, "XXXX");
                    this.depTracker.addResult(this.ts, partition, dependency_id, copy);
                    markers.add(marker++);
                    
                // Otherwise just stuff in our fake result (if they actually need it)
                } else if (this.dependency_partitions.get(dependency_id).contains(partition)) {
                    this.depTracker.addResult(this.ts, partition, dependency_id, FAKE_RESULT);
                }
            } // FOR (partition)
        } // FOR (dependency ids)
        assertEquals(NUM_DUPLICATE_STATEMENTS, markers.size());
        assert(this.ts instanceof LocalTransaction);
        System.err.println(this.ts.toString());

        VoltTable results[] = this.depTracker.getResults(this.ts);
        assertNotNull(results);
        assertEquals(NUM_DUPLICATE_STATEMENTS, results.length);
        
        for (int i = 0; i < results.length; i++) {
            marker = markers.get(i);
            assertNotNull(marker);
            results[i].resetRowPosition();
            assert(results[i].advanceRow());
            assertEquals(marker.longValue(), results[i].getLong(0));
            // System.err.println(results[i]);
            // System.err.println(StringUtil.DOUBLE_LINE);
        } // FOR
        // System.err.println(this.ts);
    }

}
