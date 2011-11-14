package edu.mit.hstore.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.junit.Test;
import org.voltdb.ExecutionSite;
import org.voltdb.MockExecutionSite;
import org.voltdb.catalog.Site;

import com.google.protobuf.RpcCallback;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.TransactionInitResponse;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;

public class TestTransactionQueueManager extends BaseTestCase {

    private static final int NUM_PARTITONS = 4;
    
    HStoreSite hstore_site;
    TransactionQueueManager queue;
    
    class MockCallback implements RpcCallback<Hstore.TransactionInitResponse> {
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
        Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
        for (int p = 0; p < NUM_PARTITONS; p++) {
            ExecutionSite site = new MockExecutionSite(p, catalog, p_estimator);
            executors.put(p, site);
        } // FOR
        hstore_site = new HStoreSite(catalog_site, executors, p_estimator);
        for (ExecutionSite site : executors.values()) {
            site.initHStoreSite(hstore_site);    
        } // FOR
        
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
        TransactionInitWrapperCallback outer_callback = new TransactionInitWrapperCallback(hstore_site);
        outer_callback.init(txn_id, partitions, inner_callback);
        
        // Insert the txn into our queue and then call check
        // This should immediately release our transaction and invoke the inner_callback
        boolean ret = this.queue.insert(txn_id, partitions, outer_callback);
        assert(ret);
        
        int tries = 10;
        while (queue.isEmpty() == false && tries-- > 0) {
            queue.checkQueues();
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
        TransactionInitWrapperCallback outer_callback0 = new TransactionInitWrapperCallback(hstore_site);
        outer_callback0.init(txn_id0, partitions0, inner_callback0);
        
        final MockCallback inner_callback1 = new MockCallback();
        TransactionInitWrapperCallback outer_callback1 = new TransactionInitWrapperCallback(hstore_site);
        outer_callback1.init(txn_id1, partitions1, inner_callback1);
        
        // insert the higher ID first but make sure it comes out second
        this.queue.insert(txn_id1, partitions1, outer_callback1);
        this.queue.insert(txn_id0, partitions0, outer_callback0);
        
        // create another thread to get the locks in order
        Thread t = new Thread() {
            public void run() {
                try {
                    inner_callback0.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.finished(txn_id0, Hstore.Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback1.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.finished(txn_id1, Hstore.Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
            }
        };
        t.start();
        
        while (queue.isEmpty() == false) {
            queue.checkQueues();
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
        TransactionInitWrapperCallback outer_callback0 = new TransactionInitWrapperCallback(hstore_site);
        outer_callback0.init(txn_id0, partitions0, inner_callback0);
        
        final MockCallback inner_callback1 = new MockCallback();
        TransactionInitWrapperCallback outer_callback1 = new TransactionInitWrapperCallback(hstore_site);
        outer_callback1.init(txn_id1, partitions1, inner_callback1);
        
        final MockCallback inner_callback2 = new MockCallback();
        TransactionInitWrapperCallback outer_callback2 = new TransactionInitWrapperCallback(hstore_site);
        outer_callback2.init(txn_id2, partitions2, inner_callback2);
        
        this.queue.insert(txn_id0, partitions0, outer_callback0);
        this.queue.insert(txn_id1, partitions1, outer_callback1);
        
        // create another thread to get the locks in order
        Thread t = new Thread() {
            public void run() {
                try {
                    inner_callback0.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.finished(txn_id0, Hstore.Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback1.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.finished(txn_id1, Hstore.Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback2.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.finished(txn_id2, Hstore.Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
            }
        };
        t.start();
        
        // both of the first two disjoint txns should be released on the same call to checkQueues()
        while (queue.checkQueues() == false) {
            ThreadUtil.sleep(10);
        }
        assertTrue(queue.isEmpty());
        
        // add the third txn and wait for it
        this.queue.insert(txn_id2, partitions2, outer_callback2);
        while (queue.isEmpty() == false) {
            queue.checkQueues();
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
        TransactionInitWrapperCallback outer_callback0 = new TransactionInitWrapperCallback(hstore_site);
        outer_callback0.init(txn_id0, partitions0, inner_callback0);
        
        final MockCallback inner_callback1 = new MockCallback();
        TransactionInitWrapperCallback outer_callback1 = new TransactionInitWrapperCallback(hstore_site);
        outer_callback1.init(txn_id1, partitions1, inner_callback1);
        
        this.queue.insert(txn_id0, partitions0, outer_callback0);
        this.queue.insert(txn_id1, partitions1, outer_callback1);
        
        // create another thread to get the locks in order
        Thread t = new Thread() {
            public void run() {
                try {
                    inner_callback0.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.finished(txn_id0, Hstore.Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
                try {
                    inner_callback1.lock.acquire();
                    for (int partition = 0; partition < NUM_PARTITONS; ++partition) {
                        queue.finished(txn_id1, Hstore.Status.OK, partition);
                    }
                } catch (InterruptedException e) {}
            }
        };
        t.start();
        
        // only the first txn should be released because they are not disjoint
        while (queue.checkQueues() == false) {
            ThreadUtil.sleep(10);
        }
        assertFalse(queue.isEmpty());
        
        while (queue.isEmpty() == false) {
            queue.checkQueues();
            ThreadUtil.sleep(10);
        }
        
        // wait for all the locks to be acquired
        t.join();
    }
}
