package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.voltdb.TransactionIdManager;
import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.SpecExecSchedulerPolicyType;
import org.voltdb.types.SpeculationType;
import org.voltdb.utils.EstTimeUpdater;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.MockEstimate;
import edu.brown.hstore.specexec.checkers.AbstractConflictChecker;
import edu.brown.hstore.specexec.checkers.TableConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.profilers.SpecExecProfiler;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * SpecExecScheduler Test Cases
 * @author pavlo
 */
public class TestSpecExecScheduler extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 5;
    private static final int BASE_PARTITION = 1;
    private static final int WINDOW_SIZE = 5;
    
    private MockHStoreSite hstore_site;
    private TransactionIdManager idManager;
    private TransactionQueueManager queueManager;
    private PartitionLockQueue work_queue;
    private SpecExecScheduler scheduler;
    private SpecExecScheduler.Debug schedulerDebug;
    private AbstractConflictChecker checker;
    private LocalTransaction dtxn;
    private AbstractTransaction.Debug dtxnDebug;
    private List<LocalTransaction> addedTxns = new ArrayList<LocalTransaction>();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.initializeCatalog(2, 2, NUM_PARTITIONS);
        // if (isFirstSetup()) System.err.println(CatalogInfo.getInfo(catalog, null));
        
        HStoreConf hstore_conf = HStoreConf.singleton();
        hstore_conf.site.specexec_profiling = true;
        hstore_conf.site.specexec_profiling_sample = 1.0;
        
        this.checker = new TableConflictChecker(catalogContext);
        this.hstore_site = new MockHStoreSite(0, catalogContext, HStoreConf.singleton());
        this.idManager = hstore_site.getTransactionIdManager(0);
        this.queueManager = this.hstore_site.getTransactionQueueManager();
        this.work_queue = this.queueManager.getLockQueue(BASE_PARTITION);
        this.scheduler = new SpecExecScheduler(this.checker,
                                               BASE_PARTITION,
                                               this.work_queue,
                                               SpecExecSchedulerPolicyType.FIRST,
                                               WINDOW_SIZE);
        this.schedulerDebug = this.scheduler.getDebugContext();
        
        // Create our current distributed transaction
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        this.dtxn = new LocalTransaction(this.hstore_site);
        this.dtxn.testInit(this.idManager.getNextUniqueTransactionId(),
                           BASE_PARTITION,
                           null,
                           catalogContext.getAllPartitionIds(),
                           catalog_proc);
        assertFalse(this.dtxn.isPredictAllLocal());
        this.dtxnDebug = this.dtxn.getDebugContext();
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private LocalTransaction populateQueue(Collection<LocalTransaction> txns, int size) throws Exception {
        Collection<Procedure> conflicts = ConflictSetUtil.getAllConflicts(dtxn.getProcedure());
        List<Procedure> procList = new ArrayList<Procedure>();
        for (Procedure p : catalogContext.getRegularProcedures()) {
            if (conflicts.contains(p) == false) {
                procList.add(p);
                if (procList.size() >= size)
                    break;
            }
        } // FOR
        assertFalse(procList.isEmpty());
        
        LocalTransaction ts = null, tsWithoutEstimatorState = null;
        for (Procedure proc: procList) {
            ts = new LocalTransaction(this.hstore_site);
            ts.testInit(this.idManager.getNextUniqueTransactionId(),
                        BASE_PARTITION,
                        null,
                        catalogContext.getPartitionSetSingleton(BASE_PARTITION),
                        proc);
            if (tsWithoutEstimatorState == null)
                tsWithoutEstimatorState = ts;
            assertTrue(ts.isPredictSinglePartition());
            this.addToQueue(ts);
            txns.add(ts);
        } // FOR
        EstTimeUpdater.update(System.currentTimeMillis());
        return (tsWithoutEstimatorState);
    }
    
    private LocalTransaction addToQueue(LocalTransaction ts) {
        this.work_queue.offer(ts, false);
        return (ts);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------

    /**
     * testFirstMatchPolicy
     */
    public void testFirstMatchPolicy() throws Exception {
        // We should be able to get one match with only one evaluation
        SpecExecProfiler profiler = this.schedulerDebug.getProfiler(SpeculationType.SP3_REMOTE_BEFORE);
        assertNotNull(profiler);
        assertTrue(profiler.num_comparisons.isEmpty());
        
        this.populateQueue(this.addedTxns, 10);
        this.scheduler.setPolicyType(SpecExecSchedulerPolicyType.FIRST);
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_BEFORE);
        assertNotNull(next);
        assertEquals(CollectionUtil.first(this.addedTxns), next);
        assertEquals(1, profiler.num_comparisons.get(1));
    }
    
    /**
     * testLastMatchPolicy
     */
    public void testLastMatchPolicy() throws Exception {
        // We should be able to get one match with only one evaluation
        SpecExecProfiler profiler = this.schedulerDebug.getProfiler(SpeculationType.SP3_REMOTE_BEFORE);
        assertNotNull(profiler);
        assertTrue(profiler.num_comparisons.isEmpty());
        this.scheduler.setPolicyType(SpecExecSchedulerPolicyType.LAST);
        this.scheduler.setWindowSize(Integer.MAX_VALUE);
        
        this.populateQueue(this.addedTxns, 10);
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_BEFORE);
        assertNotNull(next);
        assertEquals(CollectionUtil.last(this.addedTxns), next);
        assertEquals(this.addedTxns.size(), profiler.num_comparisons.getMaxValue().intValue());
    }
  
    /**
     * testShortestPolicy
     */
    public void testShortestPolicy() throws Exception {
        // We should only evaluate the same # of txns as the WINDOW_SIZE
        SpecExecProfiler profiler = this.schedulerDebug.getProfiler(SpeculationType.SP3_REMOTE_BEFORE);
        assertNotNull(profiler);
        assertTrue(profiler.num_comparisons.isEmpty());
        
        // Add a bunch and then set the last one to have the shortest time
        this.populateQueue(this.addedTxns, 20);
        AbstractTransaction shortest = CollectionUtil.last(this.work_queue);
        for (AbstractTransaction ts : this.work_queue) {
            final long remaining = (ts == shortest ? 10 : 1000);
            EstimatorState state = new EstimatorState(catalogContext) {
                {
                    MockEstimate est = new MockEstimate(remaining);
                    this.addEstimate(est);
                }
            };
            ts.setEstimatorState(state);
        } // FOR
        
        int windowSize = this.work_queue.size();
        this.scheduler.setPolicyType(SpecExecSchedulerPolicyType.SHORTEST);
        this.scheduler.setWindowSize(windowSize);
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_BEFORE);
        assertNotNull(next);
        assertEquals(shortest, next);
        // System.err.println(profiler.num_comparisons.toString());
        assertEquals(1, profiler.num_comparisons.get(windowSize));
    }
    
    /**
     * testLongestPolicy
     */
    public void testLongestPolicy() throws Exception {
        LocalTransaction tsWithoutEstimatorState = this.populateQueue(this.addedTxns, 3);
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.SP1_IDLE);
        // System.err.println(this.dtxn.debug());
        assertNotNull(next);
        assertEquals(tsWithoutEstimatorState, next);
        assertFalse(this.work_queue.toString(), this.work_queue.contains(next));
  }
    
    /**
     * testNonConflicting
     */
    public void testNonConflicting() throws Exception {
        // Make a single-partition txn for a procedure that has no conflicts with
        // our dtxn and add it to our queue. It should always be returned 
        // and marked as speculative by the scheduler
        Collection<Procedure> conflicts = ConflictSetUtil.getAllConflicts(dtxn.getProcedure());
        Procedure proc = null;
        for (Procedure p : catalogContext.getRegularProcedures()) {
            if (conflicts.contains(p) == false) {
                proc = p;
                break;
            }
        } // FOR
        assertNotNull(proc);
        
        LocalTransaction ts = new LocalTransaction(this.hstore_site);
        ts.testInit(this.idManager.getNextUniqueTransactionId(), BASE_PARTITION, null, catalogContext.getPartitionSetSingleton(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.SP1_IDLE);
        //System.err.println(this.dtxn.debug());
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next));
    }
    
    /**
     * testWriteWriteConflicting
     */
    public void testWriteWriteConflicting() throws Exception {
        // Make a single-partition txn for a procedure that has a write-write conflict with
        // our dtxn and add it to our queue. It should only be allowed to be returned by next()
        // if the current dtxn has not written to that table yet (but reads are allowed)
        Procedure dtxnProc = dtxn.getProcedure();
        Procedure proc = null;
        for (Procedure p : catalogContext.getRegularProcedures()) {
            Collection<Procedure> c = ConflictSetUtil.getWriteWriteConflicts(p);
            if (c.contains(dtxnProc)) {
                proc = p;
                break;
            }
        } // FOR
        assertNotNull(proc);
        
        ConflictSet cs = proc.getConflicts().get(dtxnProc.getName());
        assertNotNull(cs);
        Collection<Table> conflictTables = ConflictSetUtil.getAllTables(cs.getWritewriteconflicts());
        assertFalse(conflictTables.isEmpty());
        
        // First time we should be able to get through
        LocalTransaction ts = new LocalTransaction(this.hstore_site);
        ts.testInit(this.idManager.getNextUniqueTransactionId(), BASE_PARTITION, null, catalogContext.getPartitionSetSingleton(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_AFTER);
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next));
        ts.finish();
        
        // Now have the dtxn "write" to one of the tables in our ConflictSet
        dtxnDebug.clearReadWriteSets();
        dtxn.markTableWritten(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(this.idManager.getNextUniqueTransactionId(), BASE_PARTITION, null, catalogContext.getPartitionSetSingleton(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_AFTER);
        assertNull(next);
        ts.finish();
        
        // Reads aren't allowed either
        dtxnDebug.clearReadWriteSets();
        dtxn.markTableRead(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(this.idManager.getNextUniqueTransactionId(), BASE_PARTITION, null, catalogContext.getPartitionSetSingleton(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_AFTER);
        assertNull(next);
        ts.finish();
    }
    
    /**
     * testReadWriteConflicting
     */
    public void testReadWriteConflicting() throws Exception {
        // Make a single-partition txn for a procedure that has a read-write conflict with
        // our dtxn and add it to our queue. We will first test it without updating the
        // dtxn's read/write table set, which means that our single-partition txn should be
        // returned by next(). We will then mark the conflict table as written by the dtxn,
        // which means that the single-partition txn should *not* be returned
        Procedure dtxnProc = dtxn.getProcedure();
        Procedure proc = null;
        for (Procedure p : catalogContext.getRegularProcedures()) {
            Collection<Procedure> c = ConflictSetUtil.getReadWriteConflicts(p);
            if (c.contains(dtxnProc)) {
                proc = p;
                break;
            }
        } // FOR
        assertNotNull(proc);
        
        ConflictSet cs = proc.getConflicts().get(dtxnProc.getName());
        assertNotNull(cs);
        Collection<Table> conflictTables = ConflictSetUtil.getAllTables(cs.getReadwriteconflicts());
        assertFalse(conflictTables.isEmpty());
        
        LocalTransaction ts = new LocalTransaction(this.hstore_site);
        ts.testInit(this.idManager.getNextUniqueTransactionId(), BASE_PARTITION, null, catalogContext.getPartitionSetSingleton(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_BEFORE);
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next));
        ts.finish();
        
        // Reads are allowed!
        dtxnDebug.clearReadWriteSets();
        dtxn.markTableRead(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(this.idManager.getNextUniqueTransactionId(), BASE_PARTITION, null, catalogContext.getPartitionSetSingleton(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_AFTER);
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next));
        ts.finish();
        
        // But writes are not!
        dtxnDebug.clearReadWriteSets();
        dtxn.markTableWritten(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(this.idManager.getNextUniqueTransactionId(), BASE_PARTITION, null, catalogContext.getPartitionSetSingleton(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        next = this.scheduler.next(this.dtxn, SpeculationType.SP3_REMOTE_AFTER);
        assertNull(next);
    }

}
