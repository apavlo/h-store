/**
 * 
 */
package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.junit.Test;
import edu.brown.hstore.PartitionExecutor;
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
import edu.brown.hashing.DefaultHasher;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.DependencyInfo;
import edu.brown.hstore.txns.ExecutionState;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.statistics.Histogram;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockPartitionExecutor;

/**
 * @author pavlo
 */
public class TestTransactionState extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestTransactionState.class);

    private static final Long TXN_ID = 1000l;
    private static final long CLIENT_HANDLE = 99999l;
    private static final boolean SINGLE_PARTITIONED = false;
    private static final long UNDO_TOKEN = 10l;
    
    private static final String TARGET_PROCEDURE = "UpdateLocation";
    private static final String TARGET_STATEMENT = "update";
    private static final int NUM_DUPLICATE_STATEMENTS = 3;
    
    private static final int NUM_PARTITIONS = 4;
    private static final int NUM_EXPECTED_DEPENDENCIES = (NUM_PARTITIONS + 1) * NUM_DUPLICATE_STATEMENTS;
    private static final int LOCAL_PARTITION = 0;

    private static final VoltTable.ColumnInfo FAKE_RESULTS_COLUMNS[] = new VoltTable.ColumnInfo[] {
        new VoltTable.ColumnInfo("ID", VoltType.INTEGER),
        new VoltTable.ColumnInfo("VAL", VoltType.STRING),
    };
    private static final VoltTable FAKE_RESULT = new VoltTable(FAKE_RESULTS_COLUMNS);
    
    private static HStoreSite hstore_site;
    private static PartitionExecutor executor;
    private static BatchPlan plan;
    private static List<WorkFragment> ftasks = new ArrayList<WorkFragment>();
    
    private LocalTransaction ts;
    private ExecutionState execState;
    private ListOrderedSet<Integer> dependency_ids = new ListOrderedSet<Integer>();
    private List<Integer> internal_dependency_ids = new ArrayList<Integer>();
    private List<Integer> output_dependency_ids = new ArrayList<Integer>();
    private Histogram<Integer> touched_partitions = new Histogram<Integer>();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        
        if (executor == null) {
            PartitionEstimator p_estimator = new PartitionEstimator(catalogContext);
            executor = new MockPartitionExecutor(LOCAL_PARTITION, catalog, p_estimator);
            p_estimator = new PartitionEstimator(catalogContext, new DefaultHasher(catalog_db, NUM_PARTITIONS));
            
            // Setup a BatchPlanner for ourselves here
            Procedure catalog_proc = catalog_db.getProcedures().get(TARGET_PROCEDURE);
            assertNotNull(catalog_proc);
            Statement catalog_stmt = catalog_proc.getStatements().get(TARGET_STATEMENT);
            assertNotNull(catalog_stmt);

            // Create a SQLStmt batch
            SQLStmt batch[] = new SQLStmt[NUM_DUPLICATE_STATEMENTS];
            ParameterSet args[] = new ParameterSet[NUM_DUPLICATE_STATEMENTS];
            
            for (int i = 0; i < batch.length; i++) {
                Object raw_args[] = new Object[] {
                    new Long(i + 1),    // VLR_LOCATION
                    new String("XXX"),  // SUB_NBR
                };
                batch[i] = new SQLStmt(catalog_stmt, catalog_stmt.getMs_fragments());
                args[i] = VoltProcedure.getCleanParams(batch[i], raw_args); 
            } // FOR
         
            Partition catalog_part = catalogContext.getPartitionById(LOCAL_PARTITION);
            assertNotNull(catalog_part);
            hstore_site = HStore.initialize(catalogContext, ((Site)catalog_part.getParent()).getId(), HStoreConf.singleton());
            hstore_site.addPartitionExecutor(LOCAL_PARTITION, executor);
            
            BatchPlanner planner = new BatchPlanner(batch, catalog_proc, p_estimator);
            plan = planner.plan(TXN_ID, CLIENT_HANDLE, LOCAL_PARTITION, Collections.singleton(LOCAL_PARTITION), SINGLE_PARTITIONED, this.touched_partitions, args);
            assertNotNull(plan);
            plan.getWorkFragments(TXN_ID, ftasks);
//            System.err.println("FTASKS: " + ftasks);
            assertFalse(ftasks.isEmpty());
        }
        assertNotNull(ftasks);
        assertNotNull(executor);
        
        this.execState = new ExecutionState(executor);
        this.ts = new LocalTransaction(hstore_site);
        this.ts.testInit(TXN_ID,
                         LOCAL_PARTITION,
                         null,
                         catalogContext.getAllPartitionIds(), this.getProcedure(TARGET_PROCEDURE));
        this.ts.setExecutionState(this.execState);
        assertNull(this.ts.getCurrentRoundState(LOCAL_PARTITION));
    }

    /**
     * Add all of the FragmentTaskMessages from our BatchPlanner into the TransactionState
     * We will also populate our list of dependency ids
     */
    private void addFragments() {
        this.ts.setBatchSize(NUM_DUPLICATE_STATEMENTS);
        for (WorkFragment ftask : ftasks) {
            assertNotNull(ftask);
            this.ts.addWorkFragment(ftask);
            for (int i = 0, cnt = ftask.getFragmentIdCount(); i < cnt; i++) {
                this.dependency_ids.add(ftask.getOutputDepId(i));
                WorkFragment.InputDependency input_dep_ids = ftask.getInputDepId(i);
                for (Integer input_dep_id : input_dep_ids.getIdsList()) {
                    if (input_dep_id != HStoreConstants.NULL_DEPENDENCY_ID) {
                        this.internal_dependency_ids.add(input_dep_id);
                    }
                } // FOR
            } // FOR
        } // FOR
        for (int d_id : this.dependency_ids) {
            if (!this.internal_dependency_ids.contains(d_id)) this.output_dependency_ids.add(d_id);
        } // FOR
        Collections.sort(this.internal_dependency_ids);
        Collections.sort(this.output_dependency_ids);
        
        assertFalse(this.output_dependency_ids.isEmpty());
        assertFalse(this.internal_dependency_ids.isEmpty());
//        System.err.println(ftasks);
//        System.err.println("OUTPUT:   " + this.output_dependency_ids);
//        System.err.println("INTERNAL: " + this.internal_dependency_ids);
    }
    
    /**
     * testInitRound
     */
    @Test
    public void testInitRound() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
        assertEquals(AbstractTransaction.RoundState.INITIALIZED, this.ts.getCurrentRoundState(LOCAL_PARTITION));
        assertNotNull(this.ts.getLastUndoToken(LOCAL_PARTITION));
        assertEquals(UNDO_TOKEN, this.ts.getLastUndoToken(LOCAL_PARTITION));
        //System.err.println(this.ts);
    }
    
    /**
     * testStartRound
     */
    @Test
    public void testStartRound() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
        assertEquals(AbstractTransaction.RoundState.INITIALIZED, this.ts.getCurrentRoundState(LOCAL_PARTITION));
        this.addFragments();
        this.ts.startRound(LOCAL_PARTITION);
        CountDownLatch latch = this.execState.getDependencyLatch();
        assertNotNull(latch);
        
//        System.err.println(this.ts.toString());
        assertEquals(NUM_EXPECTED_DEPENDENCIES, latch.getCount());
        assertEquals(NUM_DUPLICATE_STATEMENTS, this.execState.getOutputOrder().size());
        
        // Although there will be a single blocked FragmentTaskMessage, it will contain
        // the same number of PlanFragments as we have duplicate Statements
        System.err.println(this.execState.getBlockedWorkFragments());
        assertEquals(NUM_DUPLICATE_STATEMENTS, this.execState.getBlockedWorkFragments().size());
        
        // We now need to make sure that our output order is correct
        // We should be getting back the same number of results as how
        // many Statements that we queued up
        for (int i = 0; i < NUM_DUPLICATE_STATEMENTS; i++) {
            Integer dependency_id = this.execState.getOutputOrder().get(i);
            assertNotNull(dependency_id);
            assert(this.output_dependency_ids.contains(dependency_id));
            assertNotNull(this.ts.getDependencyInfo(dependency_id));
        } // FOR
        //System.err.println(this.ts);
    }
    
    /**
     * testAddFragmentTaskMessage
     */
    @Test
    public void testAddFragmentTaskMessage() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
        this.addFragments();
        
        assertEquals(NUM_EXPECTED_DEPENDENCIES, this.execState.getDependencyCount());
        assertEquals(NUM_DUPLICATE_STATEMENTS, this.execState.getStatementCount());
        
        // For each Statement that we have queued, make sure that they have the proper
        // partitions setup for the dependencies that we expect to show up
        for (int i = 0; i < NUM_DUPLICATE_STATEMENTS; i++) {
            Map<Integer, DependencyInfo> stmt_dinfos = this.execState.getStatementDependencies(i);
            assertNotNull(stmt_dinfos);
            assertFalse(stmt_dinfos.isEmpty());
            
            for (Integer d_id : stmt_dinfos.keySet()) {
                DependencyInfo dinfo = stmt_dinfos.get(d_id);
                assertNotNull(dinfo);
                
                if (this.internal_dependency_ids.contains(d_id)) {
                    // This fragment should have been broadcast to all partitions
                    assertEquals(NUM_PARTITIONS, dinfo.getPartitions().size());
                    // But never out to VoltProcedure
                    assertFalse(this.output_dependency_ids.contains(d_id));
                    // And we should have a task blocked waiting for this dependency
                    assertFalse(dinfo.getBlockedWorkFragments().isEmpty());
                } else {
                    assertEquals(1, dinfo.getPartitions().size());
                    assertEquals(LOCAL_PARTITION, dinfo.getPartitions().get(0).intValue());
                }
            } // FOR
            
        } // FOR
        //System.err.println(this.ts);
    }
    
    /**
     * testAddResult
     */
    @Test
    public void testAddResult() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
        this.addFragments();
        this.ts.startRound(LOCAL_PARTITION);
        assertEquals(AbstractTransaction.RoundState.STARTED, this.ts.getCurrentRoundState(LOCAL_PARTITION));
        
        // We want to add results for just one of the duplicated statements and make sure that
        // we only unblock one of them. First we need to find an internal dependency that has blocked tasks 
        Integer internal_d_id = null;
        DependencyInfo internal_dinfo = null;
        for (Integer d_id : this.internal_dependency_ids) {
            internal_d_id = d_id;
            internal_dinfo = this.ts.getDependencyInfo(d_id);
            if (internal_dinfo != null) break;
        } // FOR
        assertNotNull(internal_d_id);
        assertNotNull(internal_dinfo);
        
        // Now shove a fake result for each partition at the TransactionState object. This should
        // cause the blocked FragmentTaskMessage to get moved into the "ready" queue
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            assertFalse(internal_dinfo.hasTasksReady());
            // If this is the first partition, then add one for each of the duplicate Statements
            // We want to make sure that they don't unblock
            for (int stmt_index = 0; stmt_index < NUM_DUPLICATE_STATEMENTS; stmt_index++) {
                if (partition != LOCAL_PARTITION && stmt_index > 0) break;
                int dependency_id = this.internal_dependency_ids.get(stmt_index);
                LOG.debug("Adding result for [partition=" + partition + ", dependency_id=" + dependency_id + ", stmt_index=" + stmt_index + "]");
                this.ts.addResult(partition, dependency_id, FAKE_RESULT);
            } // FOR
        } // FOR
        if (NUM_PARTITIONS != internal_dinfo.getResults().size()) {
            LOG.info(this.ts);
            LOG.info("----------------------------------");
            LOG.info(internal_dinfo);
        }
        assertEquals(internal_dinfo.getResults().toString(), NUM_PARTITIONS, internal_dinfo.getResults().size());
        assert(internal_dinfo.hasTasksReleased());
        
        // Make sure that all other Statements didn't accidently unblock their FragmentTaskMessages...
        for (int stmt_index = 1; stmt_index < NUM_DUPLICATE_STATEMENTS; stmt_index++) {
            DependencyInfo other = this.ts.getDependencyInfo(this.internal_dependency_ids.get(stmt_index));
            assertNotNull(other);
            assertFalse(other.hasTasksReady());
            assertFalse(other.hasTasksReleased());
            assertEquals(1, other.getResults().size());
        } // FOR
        // System.err.println(this.ts);
    }
    
    /**
     * testAddResultsBeforeStart
     */
    @Test
    public void testAddResultsBeforeStart() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
        this.addFragments();
        assertEquals(AbstractTransaction.RoundState.INITIALIZED, this.ts.getCurrentRoundState(LOCAL_PARTITION));
        
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
                // Otherwise just stuff in our fake result
                } else {
                    this.ts.addResult(partition, dependency_id, FAKE_RESULT);
                }
            } // FOR (partition)
        } // FOR (dependency ids)
        assertEquals(NUM_DUPLICATE_STATEMENTS, markers.size());

        this.ts.startRound(LOCAL_PARTITION);
        CountDownLatch latch = this.execState.getDependencyLatch();
        assertNotNull(latch);
        assertEquals(0, latch.getCount());
        assertEquals(AbstractTransaction.RoundState.STARTED, this.ts.getCurrentRoundState(LOCAL_PARTITION));
    }
    
    /**
     * testGetResults
     */
    @Test
    public void testGetResults() throws Exception {
        this.ts.initRound(LOCAL_PARTITION, UNDO_TOKEN);
        this.addFragments();
        this.ts.startRound(LOCAL_PARTITION);

        // Add a bunch of fake results
        Long marker = 1000l;
        List<Long> markers = new ArrayList<Long>();
        List<Integer> sorted_dependency_ids = new ArrayList<Integer>(this.dependency_ids.asList());
        Collections.sort(sorted_dependency_ids);
        
        for (int dependency_id : sorted_dependency_ids) {
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
                // Otherwise just stuff in our fake result
                } else {
                    this.ts.addResult(partition, dependency_id, FAKE_RESULT);
                    LOG.debug("Adding result for [partition=" + partition + ", dependency_id=" + dependency_id + "]");
                }
            } // FOR (partition)
        } // FOR (dependency ids)
        assertEquals(NUM_DUPLICATE_STATEMENTS, markers.size());

        VoltTable results[] = this.ts.getResults();
        assertNotNull(results);
        assertEquals(NUM_DUPLICATE_STATEMENTS, results.length);

//        System.err.println("MARKERS: " + markers);
//        System.err.println(StringUtil.join("---------\n", results));
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