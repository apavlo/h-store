package edu.brown.hstore;

import java.util.concurrent.Semaphore;

import org.junit.Test;
import org.voltdb.catalog.Site;

import com.google.protobuf.RpcCallback;

import edu.brown.BaseTestCase;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

public class TestTransactionQueueManager extends BaseTestCase {

    private static final int NUM_PARTITONS = 4;
    
    HStoreSite hstore_site;
    TransactionQueueManager queue;
    TransactionQueueManager.Debug dbg;
    
    class MockCallback implements RpcCallback<TransactionInitResponse> {
        Semaphore lock = new Semaphore(0);
        boolean invoked = false;
        
        @Override
        public void run(TransactionInitResponse parameter) {
            assertFalse(invoked);
            lock.release();
            invoked = true;
        }
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        addPartitions(NUM_PARTITONS);
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        assertNotNull(catalog_site);
        this.hstore_site = new MockHStoreSite(catalog_site, HStoreConf.singleton());
        this.queue = new TransactionQueueManager(hstore_site);
        this.dbg = this.queue.getDebugContext();
    }
    
    /**
     * Insert the txn into our queue and then call check
     * This should immediately release our transaction and invoke the inner_callback
     * @throws InterruptedException 
     */
    @Test
    public void testSingleTransaction() throws InterruptedException {
        long txn_id = 1000;
        PartitionSet partitions = catalogContext.getAllPartitionIds();
        
        MockCallback inner_callback = new MockCallback();
        
        // Insert the txn into our queue and then call check
        // This should immediately release our transaction and invoke the inner_callback
        boolean ret = this.queue.lockInsert(txn_id, partitions, inner_callback, false);
        assert(ret);
        
        int tries = 10;
        while (dbg.isLockQueuesEmpty() == false && tries-- > 0) {
            queue.checkLockQueues();
            ThreadUtil.sleep(100);
        }
        assert(inner_callback.lock.availablePermits() > 0);
        // Block on the MockCallback's lock until our thread above is able to release everybody.
        // inner_callback.lock.acquire();
    }
    
    /**
     * Add two, check that only one comes out
     * Mark first as done, second comes out
     * @throws InterruptedException 
     */
    @Test
    public void testTwoTransactions() throws InterruptedException {
        final long txn_id0 = 1000;
        final long txn_id1 = 2000;
        PartitionSet partitions0 = new PartitionSet(catalogContext.getAllPartitionIds());
        PartitionSet partitions1 = new PartitionSet(catalogContext.getAllPartitionIds());
        
        final MockCallback inner_callback0 = new MockCallback();
        
        final MockCallback inner_callback1 = new MockCallback();
        
        // insert the higher ID first but make sure it comes out second
        this.queue.lockInsert(txn_id1, partitions1, inner_callback1, false);
        this.queue.lockInsert(txn_id0, partitions0, inner_callback0, false);
        
        // create another thread to get the locks in order
        Thread t = new Thread() {
            public void run() {
                try {
                    inner_callback0.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.lockFinished(txn_id0, Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback1.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.lockFinished(txn_id1, Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
            }
        };
        t.setUncaughtExceptionHandler(this);
        t.start();
        
        while (dbg.isLockQueuesEmpty() == false) {
            queue.checkLockQueues();
            ThreadUtil.sleep(10);
        }
        
        // wait for all the locks to be acquired
        t.join();
    }
    
    /**
     * Add two disjoint partitions and third that touches all partitions
     * Two come out right away and get marked as done
     * Third doesn't come out until everyone else is done
     * @throws InterruptedException 
     */
    @Test
    public void testDisjointTransactions() throws InterruptedException {
        final long txn_id0 = 1000;
        final long txn_id1 = 2000;
        final long txn_id2 = 3000;
        PartitionSet partitions0 = new PartitionSet();
        partitions0.add(0);
        partitions0.add(2);
        PartitionSet partitions1 = new PartitionSet();
        partitions1.add(1);
        partitions1.add(3);
        PartitionSet partitions2 = new PartitionSet(catalogContext.getAllPartitionIds());
        
        final MockCallback inner_callback0 = new MockCallback();
        final MockCallback inner_callback1 = new MockCallback();
        final MockCallback inner_callback2 = new MockCallback();
        
        this.queue.lockInsert(txn_id0, partitions0, inner_callback0, false);
        this.queue.lockInsert(txn_id1, partitions1, inner_callback1, false);
        
        // create another thread to get the locks in order
        Thread t = new Thread() {
            public void run() {
                try {
                    inner_callback0.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.lockFinished(txn_id0, Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback1.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.lockFinished(txn_id1, Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback2.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.lockFinished(txn_id2, Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
            }
        };
        t.start();
        
        // both of the first two disjoint txns should be released on the same call to checkQueues()
        while (queue.checkLockQueues() == false) {
            ThreadUtil.sleep(10);
        }
        // add the third txn and wait for it
        this.queue.lockInsert(txn_id2, partitions2, inner_callback2, false);
        while (dbg.isLockQueuesEmpty() == false) {
            queue.checkLockQueues();
            ThreadUtil.sleep(10);
        }
        
        // wait for all the locks to be acquired
        t.join();
    }
    
    // 
    // 
    /**
     * Add two overlapping partitions, lowest id comes out
     * Mark first as done, second comes out
     * @throws InterruptedException 
     */
    @Test
    public void testOverlappingTransactions() throws InterruptedException {
        final long txn_id0 = 1000;
        final long txn_id1 = 2000;
        PartitionSet partitions0 = new PartitionSet();
        partitions0.add(0);
        partitions0.add(1);
        partitions0.add(2);
        PartitionSet partitions1 = new PartitionSet();
        partitions1.add(2);
        partitions1.add(3);
        
        final MockCallback inner_callback0 = new MockCallback();
        final MockCallback inner_callback1 = new MockCallback();
        
        this.queue.lockInsert(txn_id0, partitions0, inner_callback0, false);
        this.queue.lockInsert(txn_id1, partitions1, inner_callback1, false);
        
        // create another thread to get the locks in order
        Thread t = new Thread() {
            public void run() {
                try {
                    inner_callback0.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.lockFinished(txn_id0, Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback1.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.lockFinished(txn_id1, Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
            }
        };
        t.start();
        
        // only the first txn should be released because they are not disjoint
        while (queue.checkLockQueues() == false) {
            ThreadUtil.sleep(10);
        }
        while (dbg.isLockQueuesEmpty() == false) {
            queue.checkLockQueues();
            ThreadUtil.sleep(10);
        }
        
        // wait for all the locks to be acquired
        t.join();
    }
}
