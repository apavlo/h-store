package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.SpeculationType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogInfo;
import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.hstore.SpecExecScheduler.SchedulerPolicy;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.specexec.AbstractConflictChecker;
import edu.brown.hstore.specexec.TableConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public class TestSpecExecScheduler extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 5;
    private static final int BASE_PARTITION = 1;
    private static long NEXT_TXN_ID = 1;
    
    private MockHStoreSite hstore_site;
    private TransactionQueueManager queueManager;
    private TransactionInitPriorityQueue work_queue;
    private SpecExecScheduler scheduler;
    private AbstractConflictChecker checker;
    private LocalTransaction dtxn;
    private final Map<Long, AbstractTransaction> txns = new HashMap<Long, AbstractTransaction>();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.initializeCatalog(2, 2, NUM_PARTITIONS);
        if (isFirstSetup()) System.err.println(CatalogInfo.getInfo(catalog, null));
        
        this.checker = new TableConflictChecker(catalogContext);
        this.hstore_site = new MockHStoreSite(0, catalogContext, HStoreConf.singleton()) {
            @SuppressWarnings("unchecked")
            @Override
            public <T extends AbstractTransaction> T getTransaction(Long txn_id) {
                return (T)(txns.get(txn_id));
            }
        };
        this.queueManager = this.hstore_site.getTransactionQueueManager();
        this.work_queue = this.queueManager.getInitQueue(BASE_PARTITION);
        this.scheduler = new SpecExecScheduler(this.hstore_site,
                                               this.checker,
                                               BASE_PARTITION,
                                               this.work_queue,
                                               SchedulerPolicy.FIRST,
                                               1);
        
        // Create our current distributed transaction
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        this.dtxn = new LocalTransaction(this.hstore_site);
        this.dtxn.testInit(NEXT_TXN_ID++,
                           BASE_PARTITION,
                           null,
                           catalogContext.getAllPartitionIds(),
                           catalog_proc);
        assertFalse(this.dtxn.isPredictAllLocal());
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private LocalTransaction populateQueue(int size) throws Exception {
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
            ts.testInit(NEXT_TXN_ID++,
                        BASE_PARTITION,
                        null,
                        new PartitionSet(BASE_PARTITION),
                        proc);
            if (tsWithoutEstimatorState == null)
                tsWithoutEstimatorState = ts;
            assertTrue(ts.isPredictSinglePartition());
            this.addToQueue(ts);
        } // FOR
        return (tsWithoutEstimatorState);
    }
    
    private LocalTransaction addToQueue(LocalTransaction ts) {
        this.txns.put(ts.getTransactionId(), ts);
        this.work_queue.offer(ts.getTransactionId());
        return (ts);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------

    /**
     * testShortestPolicy
     */
    public void testShortestPolicy() throws Exception {
          LocalTransaction tsWithoutEstimatorState = this.populateQueue(3);
          LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
          System.err.println(this.dtxn.debug());
          assertNotNull(next);
          assertEquals(tsWithoutEstimatorState, next);
          assertFalse(this.work_queue.toString(), this.work_queue.contains(next));
    }
    
    /**
     * testLongestPolicy
     */
    public void testLongestPolicy() throws Exception {
        LocalTransaction tsWithoutEstimatorState = this.populateQueue(3);
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
        System.err.println(this.dtxn.debug());
        assertNotNull(next);
        assertEquals(tsWithoutEstimatorState, next);
        assertFalse(this.work_queue.toString(), this.work_queue.contains(next.getTransactionId()));
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
        ts.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
        //System.err.println(this.dtxn.debug());
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next.getTransactionId()));
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
        ts.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next.getTransactionId()));
        ts.finish();
        
        // Now have the dtxn "write" to one of the tables in our ConflictSet
        dtxn.clearReadWriteSets();
        dtxn.markTableAsWritten(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
        assertNull(next);
        ts.finish();
        
        // Reads aren't allowed either
        dtxn.clearReadWriteSets();
        dtxn.markTableAsRead(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
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
        ts.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        
        LocalTransaction next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next));
        ts.finish();
        
        // Reads are allowed!
        dtxn.clearReadWriteSets();
        dtxn.markTableAsRead(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
        assertNotNull(next);
        assertEquals(ts, next);
        assertFalse(this.work_queue.contains(next.getTransactionId()));
        ts.finish();
        
        // But writes are not!
        dtxn.clearReadWriteSets();
        dtxn.markTableAsWritten(BASE_PARTITION, CollectionUtil.first(conflictTables));
        ts.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), proc);
        assertTrue(ts.isPredictSinglePartition());
        this.addToQueue(ts);
        next = this.scheduler.next(this.dtxn, SpeculationType.NULL);
        assertNull(next);
    }

}
