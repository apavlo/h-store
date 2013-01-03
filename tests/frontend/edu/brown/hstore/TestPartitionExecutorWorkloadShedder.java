package edu.brown.hstore;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.regressionsuites.specexecprocs.DtxnTester;
import org.voltdb.sysprocs.ExecutorStatus;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ThreadUtil;

/**
 * PartitionExecutorWorkloadShedder Tests!
 * @author pavlo
 */
public class TestPartitionExecutorWorkloadShedder extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 3;
    private static final int NUM_TXNS = 100;
    private static final int BASE_PARTITION = 0;
    private static final int NOTIFY_TIMEOUT = 2500; // ms
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private TransactionQueueManager queueManager;
    private Client client;
    private PartitionExecutorWorkloadShedder workloadShedder;

    private final TM1ProjectBuilder builder = new TM1ProjectBuilder() {
        {
            this.addAllDefaults();
            this.enableReplicatedSecondaryIndexes(false);
            this.removeReplicatedSecondaryIndexes();
            this.addProcedure(DtxnTester.class);
        }
    };
    
    @Before
    public void setUp() throws Exception {
        super.setUp(this.builder);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        for (TransactionCounter tc : TransactionCounter.values()) {
            tc.clear();
        } // FOR
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.pool_profiling = true;
        this.hstore_conf.site.pool_txn_enable = true;
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.status_interval = 4000;
        this.hstore_conf.site.anticache_enable = false;
        this.hstore_conf.site.txn_incoming_delay = 5;
        this.hstore_conf.site.exec_voltdb_procinfo = true;
        this.hstore_conf.site.exec_force_singlepartitioned = false;
        this.hstore_conf.site.queue_shedder_delay = 999999;
        this.hstore_conf.site.queue_shedder_interval = 999999;
        
        this.hstore_site = this.createHStoreSite(catalog_site, hstore_conf);
        this.queueManager = this.hstore_site.getTransactionQueueManager();
        this.workloadShedder = this.hstore_site.getWorkloadShedder();
        this.client = this.createClient();
        
        this.workloadShedder.init();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void statusSnapshot() throws Exception {
        String procName = VoltSystemProcedure.procCallName(ExecutorStatus.class);
        Object params[] = { 0L };
        ClientResponse cr = this.client.callProcedure(procName, params);
        assertEquals(Status.OK, cr.getStatus());
    }
    
    private void invokeTest(Procedure reject_proc) throws Exception {
        // Submit our first dtxn that will block until we tell it to go
        DtxnTester.NOTIFY_BEFORE.drainPermits();
        DtxnTester.LOCK_BEFORE.drainPermits();
        DtxnTester.NOTIFY_AFTER.drainPermits();
        DtxnTester.LOCK_AFTER.drainPermits();
        Procedure catalog_proc = this.getProcedure(DtxnTester.class);
        Object params[] = new Object[]{ BASE_PARTITION };
        LatchableProcedureCallback dtxnCallback = new LatchableProcedureCallback(1);
        this.client.callProcedure(dtxnCallback, catalog_proc.getName(), params);
        
        // Block until we know that the txn has started running
        boolean result = DtxnTester.NOTIFY_BEFORE.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        // Send a bunch of dtxns that will get queued up at all of the partitions
        LatchableProcedureCallback rejectedCallback = new LatchableProcedureCallback(NUM_TXNS);
        for (int i = 0; i < NUM_TXNS; i++) {
            this.client.callProcedure(rejectedCallback, reject_proc.getName(), params);
        } // FOR
        ThreadUtil.sleep(NOTIFY_TIMEOUT);
        
        // Make sure that our boys are in the queue
        TransactionInitPriorityQueue queue = this.queueManager.getInitQueue(BASE_PARTITION);
        assertEquals(NUM_TXNS, queue.size());
        
        // Now invoke the workload shedder directly
        this.workloadShedder.shedWork(BASE_PARTITION, NUM_TXNS);
        
        // Ok, so let's release the blocked dtxn. This will allow everyone 
        // else to come to the party
        DtxnTester.LOCK_AFTER.release();
        DtxnTester.LOCK_BEFORE.release();
        result = dtxnCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH --> " + dtxnCallback.latch, result);
        
        // The DTXN guy should be legit
        assertEquals(1, dtxnCallback.responses.size());
        ClientResponse dtxnCR = CollectionUtil.first(dtxnCallback.responses);
        assertEquals(dtxnCR.toString(), Status.OK, dtxnCR.getStatus());
        
        // All other txns should have been rejected
        result = rejectedCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("REJECTED LATCH --> " + rejectedCallback.latch, result);
        assertEquals(NUM_TXNS, rejectedCallback.responses.size());
        for (ClientResponse cr : rejectedCallback.responses) {
            assertEquals(cr.toString(), Status.ABORT_REJECT, cr.getStatus());
        } // FOR
        
        // Make sure that everyone was cleaned up properly
        ThreadUtil.sleep(NOTIFY_TIMEOUT*2);
        HStoreSiteTestUtil.checkObjectPools(hstore_site);
        this.statusSnapshot();
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testShedWorkMultiPartition
     */
    @Test
    public void testShedWorkMultiPartition() throws Exception {
        // Make sure that we can properly shed distributed txns from our
        // queue and not leave anything hanging around.
        this.invokeTest(this.getProcedure(DtxnTester.class));
    }
    
    /**
     * testShedWorkSinglePartition
     */
    @Test
    public void testShedWorkSinglePartition() throws Exception {
        // Make sure that we can properly shed single-partitions from our
        // queue and not leave anything hanging around.
        this.invokeTest(this.getProcedure(GetSubscriberData.class));
    }
    


}
