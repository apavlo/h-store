/**
 * 
 */
package edu.brown.hstore.dtxn;

import java.util.ArrayList;
import java.util.Collections;
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
import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.DefaultHasher;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockPartitionExecutor;

/**
 * @author pavlo
 */
public class TestTransactionStateComplex extends BaseTestCase {
    private static final long TXN_ID = 1000l;
    private static final long CLIENT_HANDLE = 99999l;
    private static final boolean SINGLE_PARTITIONED = false;
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
    
    private static HStoreSite hstore_site;
    private static PartitionExecutor executor;
    private static BatchPlan plan;
    private static List<WorkFragment> ftasks = new ArrayList<WorkFragment>();
    private Histogram<Integer> touched_partitions = new Histogram<Integer>();
    
    private LocalTransaction ts;
    private ExecutionState execState;
    private ListOrderedSet<Integer> dependency_ids = new ListOrderedSet<Integer>();
    private List<Integer> internal_dependency_ids = new ArrayList<Integer>();
    private List<Integer> output_dependency_ids = new ArrayList<Integer>();
    private List<WorkFragment> first_tasks = new ArrayList<WorkFragment>();
    private Map<Integer, Set<Integer>> dependency_partitions = new HashMap<Integer, Set<Integer>>();
    private Procedure catalog_proc;
    private Statement catalog_stmt;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
        this.addPartitions(NUM_PARTITIONS);
        
        catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        catalog_stmt = this.getStatement(catalog_proc, TARGET_STATEMENT);
        
        if (executor == null) {
            PartitionEstimator p_estimator = new PartitionEstimator(catalog_db);
            executor = new MockPartitionExecutor(LOCAL_PARTITION, catalog, p_estimator);
            p_estimator = new PartitionEstimator(catalog_db, new DefaultHasher(catalog_db, NUM_PARTITIONS));
            
            // Setup a BatchPlanner for ourselves here


            // Create a SQLStmt batch
            SQLStmt batch[] = new SQLStmt[NUM_DUPLICATE_STATEMENTS];
            ParameterSet args[] = new ParameterSet[NUM_DUPLICATE_STATEMENTS];
            
            for (int i = 0; i < batch.length; i++) {
                Object raw_args[] = new Object[] {
                    new Long(i + 1),    // U_ID
                };
                batch[i] = new SQLStmt(catalog_stmt, catalog_stmt.getMs_fragments());
                args[i] = VoltProcedure.getCleanParams(batch[i], raw_args); 
            } // FOR
            
            Partition catalog_part = CatalogUtil.getPartitionById(catalog_db, LOCAL_PARTITION);
            hstore_site = HStore.initialize((Site)catalog_part.getParent(), HStoreConf.singleton());
            hstore_site.addPartitionExecutor(LOCAL_PARTITION, executor);
            
            BatchPlanner batchPlan = new BatchPlanner(batch, catalog_proc, p_estimator);
            plan = batchPlan.plan(TXN_ID, CLIENT_HANDLE, LOCAL_PARTITION, Collections.singleton(LOCAL_PARTITION), SINGLE_PARTITIONED, this.touched_partitions, args);
            assertNotNull(plan);
            plan.getWorkFragments(TXN_ID, ftasks);
            assertFalse(ftasks.isEmpty());
        }
        assertNotNull(executor);
        
        this.execState = new ExecutionState(executor);
        this.ts = new LocalTransaction(hstore_site).testInit(TXN_ID, LOCAL_PARTITION, Collections.singleton(LOCAL_PARTITION), catalog_proc);
        this.ts.setExecutionState(this.execState);
    }

    /**
     * Add all of the FragmentTaskMessages from our BatchPlanner into the TransactionState
     * We will also populate our list of dependency ids
     */
    private void addFragments() {
        this.ts.setBatchSize(NUM_DUPLICATE_STATEMENTS);
        for (WorkFragment ftask : ftasks) {
            assertNotNull(ftask);
//            System.err.println(ftask);
//            System.err.println("+++++++++++++++++++++++++++++++++++");
            this.ts.addWorkFragment(ftask);
            for (int i = 0, cnt = ftask.getFragmentIdCount(); i < cnt; i++) {
                int dep_id = ftask.getOutputDepId(i);
                this.dependency_ids.add(dep_id);
                
                if (this.dependency_partitions.containsKey(dep_id) == false) {
                    this.dependency_partitions.put(dep_id, new HashSet<Integer>());
                }
                this.dependency_partitions.get(dep_id).add(ftask.getPartitionId());
                
                WorkFragment.InputDependency input_dep_ids = ftask.getInputDepId(i); 
                if (input_dep_ids.getIdsCount() == 1 && input_dep_ids.getIds(0) == HStoreConstants.NULL_DEPENDENCY_ID) {
                    this.first_tasks.add(ftask);
                } else {
                    for (Integer input_dep_id : input_dep_ids.getIdsList()) {
                        if (input_dep_id != HStoreConstants.NULL_DEPENDENCY_ID) this.internal_dependency_ids.add(input_dep_id);
                    } // FOR
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
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
        this.addFragments();
        this.ts.startRound(LOCAL_PARTITION);
        
        // We want to add results for just one of the duplicated statements and make sure that
        // we only unblock one of them. First we need to find an internal dependency that has blocked tasks 
        Integer internal_d_id = this.internal_dependency_ids.get(0);
        assertNotNull(internal_d_id);
        DependencyInfo internal_dinfo = this.ts.getDependencyInfo(internal_d_id);
        assertNotNull(internal_dinfo);
        
        // System.err.println(this.ts);
        
        // So for this test the query plan is a diamond, so we are going to add results in waves 
        // and make sure that the things get unblocked at the right time
        // (1) Add a result for the first output dependency
        assertEquals(1, this.first_tasks.size());
        WorkFragment first_ftask = CollectionUtil.first(this.first_tasks);
        assertNotNull(first_ftask);
        int partition = first_ftask.getPartitionId();
        int first_output_dependency_id = first_ftask.getOutputDepId(0);
        DependencyInfo first_dinfo = this.ts.getDependencyInfo(first_output_dependency_id);
        assertNotNull(first_dinfo);
        assertEquals(NUM_PARTITIONS, first_dinfo.getBlockedWorkFragments().size());
        this.ts.addResult(partition, first_output_dependency_id, FAKE_RESULT);
        assert(first_dinfo.hasTasksReleased());

        // (2) Now add outputs for each of the tasks that became unblocked in the previous step
        first_ftask = CollectionUtil.first(first_dinfo.getBlockedWorkFragments());
        DependencyInfo second_dinfo = this.ts.getDependencyInfo(first_ftask.getOutputDepId(0));
        for (WorkFragment ftask : first_dinfo.getBlockedWorkFragments()) {
            assertFalse(second_dinfo.hasTasksReady());
            partition = ftask.getPartitionId();
            for (int i = 0, cnt = ftask.getFragmentIdCount(); i < cnt; i++) {
                int output_dependency_id = ftask.getOutputDepId(i);
                this.ts.addResult(partition, output_dependency_id, FAKE_RESULT);
            } // FOR
        } // FOR
        assert(second_dinfo.hasTasksReleased());
    }
    
    
    /**
     * testAddResultsBeforeStart
     */
    public void testAddResultsBeforeStart() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
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
                    this.ts.addResult(partition, dependency_id, copy);
                    markers.add(marker++);
                // Otherwise just stuff in our fake result (if they actually need it)
                } else if (this.dependency_partitions.get(dependency_id).contains(partition)) {
                    this.ts.addResult(partition, dependency_id, FAKE_RESULT);
                }
            } // FOR (partition)
        } // FOR (dependency ids)
        assertEquals(NUM_DUPLICATE_STATEMENTS, markers.size());

        this.ts.startRound(LOCAL_PARTITION);
        CountDownLatch latch = this.ts.getDependencyLatch(); 
        assertNotNull(latch);
        assertEquals(0, latch.getCount());
    }
    
    /**
     * testGetResults
     */
    public void testGetResults() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
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
                    this.ts.addResult(partition, dependency_id, copy);
                    markers.add(marker++);
                    
                // Otherwise just stuff in our fake result (if they actually need it)
                } else if (this.dependency_partitions.get(dependency_id).contains(partition)) {
                    this.ts.addResult(partition, dependency_id, FAKE_RESULT);
                }
            } // FOR (partition)
        } // FOR (dependency ids)
        assertEquals(NUM_DUPLICATE_STATEMENTS, markers.size());
        assert(this.ts instanceof LocalTransaction);
        System.err.println(this.ts.toString());

        VoltTable results[] = this.ts.getResults();
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
