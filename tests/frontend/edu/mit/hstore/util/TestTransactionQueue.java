package edu.mit.hstore.util;

import java.util.Collection;
import java.util.HashMap;
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
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;

public class TestTransactionQueue extends BaseTestCase {

    private static final int NUM_PARTITONS = 4;
    
    
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
    
    
    HStoreSite hstore_site;
    TransactionQueue queue;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        addPartitions(NUM_PARTITONS);
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
        for (int p = 0; p < NUM_PARTITONS; p++) {
            ExecutionSite site = new MockExecutionSite(p, catalog, p_estimator);
            executors.put(p, site);
        } // FOR
        HStoreSite hstore_site = new HStoreSite(catalog_site, executors, p_estimator);
        for (ExecutionSite site : executors.values()) {
            site.initHStoreSite(hstore_site);    
        } // FOR
        
        this.queue = new TransactionQueue(hstore_site);
    }
    
    /**
     * testSampleTest
     */
    @Test
    public void testSampleTest() throws Exception {
        long txn_id = 1000;
        Collection<Integer> partitions = CatalogUtil.getAllPartitionIds(catalog_db);
        
        MockCallback inner_callback = new MockCallback();
        TransactionInitWrapperCallback outer_callback = new TransactionInitWrapperCallback(hstore_site);
        outer_callback.init(txn_id, partitions, inner_callback);
        
        // Insert the txn into our queue and then call check
        // This should immediately release our transaction and invoke the inner_callback
        this.queue.insert(txn_id, partitions, outer_callback);
//        Thread t = new Thread() {
//            public void run() {
//                while (queue.isEmpty() == false) {
//                    queue.checkQueues();
//                    ThreadUtil.sleep(10);
//                }
//            }
//        };
//        t.start();
        queue.checkQueues();
        
        // Block on the MockCallback's lock until our thread above is able to release everybody.
        inner_callback.lock.acquire();
        
    }
    
}
