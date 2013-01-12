package edu.brown.hstore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.junit.Test;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.LocalInitQueueCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class TestTransactionQueueManager extends BaseTestCase {

    private static final int NUM_PARTITONS = 4;
    private static final int TXN_DELAY = 500;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private TransactionIdManager idManager;
    private TransactionQueueManager queueManager;
    private TransactionQueueManager.Debug dbg;
    private Thread thread;
    private final Map<Long, AbstractTransaction> txns = new HashMap<Long, AbstractTransaction>();
    
    class MockCallback extends LocalInitQueueCallback {
        final Semaphore lock = new Semaphore(0);
        boolean invoked = false;
        boolean aborted = false;

        protected MockCallback() {
            super(TestTransactionQueueManager.this.hstore_site);
        }
        @Override
        public boolean isInitialized() {
            return (true);
        }
        @Override
        protected void unblockCallback() {
            assertFalse(invoked);
            this.lock.release();
            this.invoked = true;
            System.err.println("INVOKED: " + invoked);
        }
        protected void abortCallback(Status status) {
            this.aborted = true;
        }
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        addPartitions(NUM_PARTITONS);
        
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.txn_incoming_delay = TXN_DELAY;
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        assertNotNull(catalog_site);
        this.hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, HStoreConf.singleton()) {
            @SuppressWarnings("unchecked")
            @Override
            public <T extends AbstractTransaction> T getTransaction(Long txn_id) {
                return (T)(txns.get(txn_id));
            }
        };
        this.idManager = hstore_site.getTransactionIdManager(0);
        this.queueManager = this.hstore_site.getTransactionQueueManager();
        this.dbg = this.queueManager.getDebugContext();
        
        this.thread = new Thread(this.queueManager);
        this.thread.setDaemon(true);
        this.thread.start();
    }
    
    @Override
    protected void tearDown() throws Exception {
        this.queueManager.shutdown();
        if (this.thread.isAlive())
            this.thread.interrupt();
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private LocalTransaction createTransaction(Long txn_id, PartitionSet partitions, final MockCallback callback) {
        LocalTransaction ts = new LocalTransaction(this.hstore_site) {
            @Override
            public MockCallback getInitCallback() {
                return (callback);
            }
        };
        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        ts.testInit(txn_id, 0, partitions, catalog_proc);
        callback.init(ts, partitions);
        this.txns.put(txn_id, ts);
        return (ts);
    }
    
    private boolean checkAllQueues() throws InterruptedException {
        return (this.checkQueues(catalogContext.getAllPartitionIds()));
    }
    
    private boolean checkQueues(PartitionSet partitions) throws InterruptedException {
        boolean ret = true; 
        for (int partition : partitions.values()) {
            PartitionLockQueue queue = this.queueManager.getLockQueue(partition);
            assertNotNull(queue);
            AbstractTransaction ts = queue.poll();
            if (ts != null) {
                ts.getInitCallback().run(partition);
                System.err.printf("Partition %d => %s\n", partition, ts);
            }
            ret = (ts != null) || ret;
        }
        return (ret );
    }
    
    private boolean addToQueue(LocalTransaction txn, MockCallback callback) {
        boolean ret = true;
        for (int partition : txn.getPredictTouchedPartitions()) {
            boolean result = (this.queueManager.lockQueueInsert(txn, partition, callback) == Status.OK);
            ret = ret && result;
        } // FOR
        return (ret);
    }
    
    private PartitionSet findTxnInQueues(LocalTransaction txn) {
        PartitionSet partitions = new PartitionSet();
        for (int partition : catalogContext.getAllPartitionIds().values()) {
            if (this.queueManager.getLockQueue(partition).contains(txn)) {
                partitions.add(partition);
            }
        } // FOR
        return (partitions);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * Insert the txn into our queue and then call check
     * This should immediately release our transaction and invoke the inner_callback
     * @throws InterruptedException 
     */
    @Test
    public void testSingleTransaction() throws InterruptedException {
        Long txn_id = this.idManager.getNextUniqueTransactionId();
        MockCallback inner_callback = new MockCallback();
        LocalTransaction txn0 = this.createTransaction(txn_id, catalogContext.getAllPartitionIds(), inner_callback);
        
        // Insert the txn into our queue and then call check
        // This should immediately release our transaction and invoke the inner_callback
        boolean result = this.addToQueue(txn0, inner_callback);
        assertTrue(result);
        
        int tries = 10;
        while (dbg.isLockQueuesEmpty() == false && tries-- > 0) {
            ThreadUtil.sleep(TXN_DELAY);
            this.checkAllQueues();
        }
        assert(inner_callback.lock.availablePermits() > 0);
        // Block on the MockCallback's lock until our thread above is able to release everybody.
        // inner_callback.lock.acquire();
    }
    
    /**
     * testReleaseOrder
     */
    @Test
    public void testReleaseOrder() throws Exception {
        // Add three, check that only one comes out
        // Mark first as done, second comes out
        
        final Long txn_id0 = this.idManager.getNextUniqueTransactionId();
        final Long txn_id1 = this.idManager.getNextUniqueTransactionId();
        final Long txn_id2 = this.idManager.getNextUniqueTransactionId();
        final PartitionSet partitions0 = catalogContext.getAllPartitionIds();
        final PartitionSet partitions1 = catalogContext.getAllPartitionIds();
        final PartitionSet partitions2 = catalogContext.getAllPartitionIds();
        final MockCallback inner_callback0 = new MockCallback();
        final MockCallback inner_callback1 = new MockCallback();
        final MockCallback inner_callback2 = new MockCallback();
        final LocalTransaction txn0 = this.createTransaction(txn_id0, partitions0, inner_callback0);
        final LocalTransaction txn1 = this.createTransaction(txn_id1, partitions1, inner_callback1);
        final LocalTransaction txn2 = this.createTransaction(txn_id2, partitions2, inner_callback2);
        
        System.err.println("TXN0: " + txn0);
        System.err.println("TXN1: " + txn1);
        System.err.println("TXN2: " + txn2);
        System.err.flush();
        
        // insert the higher ID first but make sure it comes out second
        assertTrue(this.queueManager.toString(), this.addToQueue(txn0, inner_callback0));
        assertTrue(this.queueManager.toString(), this.addToQueue(txn2, inner_callback2));
        assertTrue(this.queueManager.toString(), this.addToQueue(txn1, inner_callback1));
        
        ThreadUtil.sleep(TXN_DELAY);
        assertTrue(this.queueManager.toString(), this.checkAllQueues());
        
        assertTrue("callback0", inner_callback0.lock.tryAcquire());
        assertFalse("callback1", inner_callback1.lock.tryAcquire());
        assertFalse("callback2", inner_callback2.lock.tryAcquire());
        assertFalse(dbg.isLockQueuesEmpty());
        for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
            this.queueManager.lockQueueFinished(txn0, Status.OK, partition);
        }
        assertFalse(dbg.isLockQueuesEmpty());
        
        ThreadUtil.sleep(TXN_DELAY);
        assertTrue(this.queueManager.toString(), this.checkAllQueues());
        assertTrue("callback1", inner_callback1.lock.tryAcquire());
        assertFalse("callback2", inner_callback2.lock.tryAcquire());
        assertFalse(dbg.isLockQueuesEmpty());
        for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
            this.queueManager.lockQueueFinished(txn1, Status.OK, partition);
        }
        
        ThreadUtil.sleep(TXN_DELAY);
        assertTrue(this.queueManager.toString(), this.checkAllQueues());
        assertTrue("callback2", inner_callback2.lock.tryAcquire());
        assertTrue(dbg.isLockQueuesEmpty());
        for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
            this.queueManager.lockQueueFinished(txn2, Status.OK, partition);
        }
        assertTrue(dbg.isLockQueuesEmpty());
    }
    
    /**
     * Add two disjoint partitions and third that touches all partitions
     * Two come out right away and get marked as done
     * Third doesn't come out until everyone else is done
     * @throws InterruptedException 
     */
    @Test
    public void testDisjointTransactions() throws InterruptedException {
        final Long txn_id0 = this.idManager.getNextUniqueTransactionId();
        final Long txn_id1 = this.idManager.getNextUniqueTransactionId();
        final Long txn_id2 = this.idManager.getNextUniqueTransactionId();
        final PartitionSet partitions0 = new PartitionSet(0, 2);
        final PartitionSet partitions1 = new PartitionSet(1, 3);
        final PartitionSet partitions2 = catalogContext.getAllPartitionIds();
        final MockCallback inner_callback0 = new MockCallback();
        final MockCallback inner_callback1 = new MockCallback();
        final MockCallback inner_callback2 = new MockCallback();
        final LocalTransaction txn0 = this.createTransaction(txn_id0, partitions0, inner_callback0);
        final LocalTransaction txn1 = this.createTransaction(txn_id1, partitions1, inner_callback1);
        final LocalTransaction txn2 = this.createTransaction(txn_id2, partitions2, inner_callback2);
        
        assert(txn_id0 < txn_id1);
        assert(txn_id1 < txn_id2);
        
        System.err.println("txn_id0: " + txn_id0);
        System.err.println("txn_id1: " + txn_id1);
        System.err.println("txn_id2: " + txn_id2);
        System.err.println();
        System.err.flush();
        
        this.addToQueue(txn0, inner_callback0);
        this.addToQueue(txn1, inner_callback1);
        this.addToQueue(txn2, inner_callback2);
        
        // Both of the first two disjoint txns should be released on the same call to checkQueues()
        ThreadUtil.sleep(TXN_DELAY);
        assertTrue(this.checkAllQueues());
        assertTrue("callback0", inner_callback0.lock.tryAcquire());
        assertTrue("callback1", inner_callback1.lock.tryAcquire());
        assertFalse("callback2", inner_callback2.lock.tryAcquire());
        assertEquals(queueManager.toString(), partitions2, this.findTxnInQueues(txn2));
        assertFalse(dbg.isLockQueuesEmpty());
        
        // Now release mark the first txn as finished. We should still 
        // not be able to get the third txn's lock
        for (int partition : partitions0) {
            queueManager.lockQueueFinished(txn0, Status.OK, partition);
        }
        
        // The third txn should *not* get released right away
        // because the second txn is still running
        assertFalse("callback2", inner_callback2.lock.tryAcquire());
        assertFalse(dbg.isLockQueuesEmpty());
        
        // Now we'll mark it as finished. That should release the third txn
        ThreadUtil.sleep(TXN_DELAY);
        for (int partition : partitions1) {
            queueManager.lockQueueFinished(txn1, Status.OK, partition);
        }
        assertTrue(this.checkAllQueues());
        assertTrue("callback2", inner_callback2.lock.tryAcquire());
        assertTrue(dbg.isLockQueuesEmpty());
    }
    
    /**
     * Add two overlapping partitions, lowest id comes out
     * Mark first as done, second comes out
     * @throws InterruptedException 
     */
    @Test
    public void testOverlappingTransactions() throws InterruptedException {
        final Long txn_id0 = this.idManager.getNextUniqueTransactionId();
        final Long txn_id1 = this.idManager.getNextUniqueTransactionId();
        final PartitionSet partitions0 = new PartitionSet(0, 1, 2);
        final PartitionSet partitions1 = new PartitionSet(2, 3);
        final MockCallback inner_callback0 = new MockCallback();
        final MockCallback inner_callback1 = new MockCallback();
        final LocalTransaction txn0 = this.createTransaction(txn_id0, partitions0, inner_callback0);
        final LocalTransaction txn1 = this.createTransaction(txn_id1, partitions1, inner_callback1);
      
        System.err.println("txn_id0: " + txn_id0);
        System.err.println("txn_id1: " + txn_id1);
        System.err.flush();
        
        this.addToQueue(txn0, inner_callback0);
        ThreadUtil.sleep(1);
        this.addToQueue(txn1, inner_callback1);
        
        ThreadUtil.sleep(TXN_DELAY*2);
        assertTrue(this.checkAllQueues());
        
        // We should get the callback for the first txn right away
        // And then checkLockQueues should always return false
        ThreadUtil.sleep(TXN_DELAY);
        assertTrue("callback0", inner_callback0.lock.tryAcquire());
        assertFalse(dbg.isLockQueuesEmpty());
        
        // Now if we mark the txn as finished, we should be able to acquire the
        // locks for the second txn. We actually need to call checkLockQueues()
        // twice because we only process the finished txns after the first one
        // Make sure that we generate the list of partitions that we need to check
        // *before* we release the first txn. We should only check the ones where
        // we haven't already received the lock for (otherwise we will block forever)
        PartitionSet temp = new PartitionSet(partitions1);
        temp.removeAll(inner_callback1.getReceivedPartitions());
        for (int partition : partitions0) {
            queueManager.lockQueueFinished(txn0, Status.OK, partition);
        }
        assertTrue(this.checkQueues(temp));
        ThreadUtil.sleep(TXN_DELAY);
        assertTrue("callback1", inner_callback1.lock.tryAcquire());
        
        for (int partition : partitions1) {
            queueManager.lockQueueFinished(txn1, Status.OK, partition);
        }
        assertTrue(dbg.isLockQueuesEmpty());
    }
}
