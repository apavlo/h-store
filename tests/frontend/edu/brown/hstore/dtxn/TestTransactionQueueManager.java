package edu.brown.hstore.dtxn;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Semaphore;

import org.junit.Test;
import org.voltdb.catalog.Site;

import com.google.protobuf.RpcCallback;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

public class TestTransactionQueueManager extends BaseTestCase {

    private static final int NUM_PARTITONS = 4;
    
    HStoreSite hstore_site;
    TransactionQueueManager queue;
    
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
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        assertNotNull(catalog_site);
        this.hstore_site = new MockHStoreSite(catalog_site, HStoreConf.singleton());
        this.queue = new TransactionQueueManager(hstore_site);
    }
    
    /**
     * Insert the txn into our queue and then call check
     * This should immediately release our transaction and invoke the inner_callback
     * @throws InterruptedException 
     */
    @Test
    public void testSingleTransaction() throws InterruptedException {
        long txn_id = 1000;
        Collection<Integer> partitions = CatalogUtil.getAllPartitionIds(catalog_db);
        
        MockCallback inner_callback = new MockCallback();
        TransactionInitQueueCallback outer_callback = new TransactionInitQueueCallback(hstore_site);
        outer_callback.init(txn_id, partitions, inner_callback);
        
        // Insert the txn into our queue and then call check
        // This should immediately release our transaction and invoke the inner_callback
        boolean ret = this.queue.lockInsert(txn_id, partitions, outer_callback);
        assert(ret);
        
        int tries = 10;
        while (queue.isLockQueuesEmpty() == false && tries-- > 0) {
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
        Collection<Integer> partitions0 = CatalogUtil.getAllPartitionIds(catalog_db);
        Collection<Integer> partitions1 = CatalogUtil.getAllPartitionIds(catalog_db);
        
        final MockCallback inner_callback0 = new MockCallback();
        TransactionInitQueueCallback outer_callback0 = new TransactionInitQueueCallback(hstore_site);
        outer_callback0.init(txn_id0, partitions0, inner_callback0);
        
        final MockCallback inner_callback1 = new MockCallback();
        TransactionInitQueueCallback outer_callback1 = new TransactionInitQueueCallback(hstore_site);
        outer_callback1.init(txn_id1, partitions1, inner_callback1);
        
        // insert the higher ID first but make sure it comes out second
        this.queue.lockInsert(txn_id1, partitions1, outer_callback1);
        this.queue.lockInsert(txn_id0, partitions0, outer_callback0);
        
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
        
        while (queue.isLockQueuesEmpty() == false) {
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
        Collection<Integer> partitions0 = new HashSet<Integer>();
        partitions0.add(0);
        partitions0.add(2);
        Collection<Integer> partitions1 = new HashSet<Integer>();
        partitions1.add(1);
        partitions1.add(3);
        Collection<Integer> partitions2 = CatalogUtil.getAllPartitionIds(catalog_db);
        
        final MockCallback inner_callback0 = new MockCallback();
        TransactionInitQueueCallback outer_callback0 = new TransactionInitQueueCallback(hstore_site);
        outer_callback0.init(txn_id0, partitions0, inner_callback0);
        
        final MockCallback inner_callback1 = new MockCallback();
        TransactionInitQueueCallback outer_callback1 = new TransactionInitQueueCallback(hstore_site);
        outer_callback1.init(txn_id1, partitions1, inner_callback1);
        
        final MockCallback inner_callback2 = new MockCallback();
        TransactionInitQueueCallback outer_callback2 = new TransactionInitQueueCallback(hstore_site);
        outer_callback2.init(txn_id2, partitions2, inner_callback2);
        
        this.queue.lockInsert(txn_id0, partitions0, outer_callback0);
        this.queue.lockInsert(txn_id1, partitions1, outer_callback1);
        
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
        assertTrue(queue.isLockQueuesEmpty());
        
        // add the third txn and wait for it
        this.queue.lockInsert(txn_id2, partitions2, outer_callback2);
        while (queue.isLockQueuesEmpty() == false) {
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
        Collection<Integer> partitions0 = new HashSet<Integer>();
        partitions0.add(0);
        partitions0.add(1);
        partitions0.add(2);
        Collection<Integer> partitions1 = new HashSet<Integer>();
        partitions1.add(2);
        partitions1.add(3);
        
        final MockCallback inner_callback0 = new MockCallback();
        TransactionInitQueueCallback outer_callback0 = new TransactionInitQueueCallback(hstore_site);
        outer_callback0.init(txn_id0, partitions0, inner_callback0);
        
        final MockCallback inner_callback1 = new MockCallback();
        TransactionInitQueueCallback outer_callback1 = new TransactionInitQueueCallback(hstore_site);
        outer_callback1.init(txn_id1, partitions1, inner_callback1);
        
        this.queue.lockInsert(txn_id0, partitions0, outer_callback0);
        this.queue.lockInsert(txn_id1, partitions1, outer_callback1);
        
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
        assertFalse(queue.isLockQueuesEmpty());
        
        while (queue.isLockQueuesEmpty() == false) {
            queue.checkLockQueues();
            ThreadUtil.sleep(10);
        }
        
        // wait for all the locks to be acquired
        t.join();
    }
}
