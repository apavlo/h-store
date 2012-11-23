package edu.brown.hstore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.ClientResponseDebug;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.specexecprocs.DistributedBlockable;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.specexec.AbstractConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ThreadUtil;

public class TestPartitionExecutorSpecExec extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 2;
    private static final int BASE_PARTITION = 0;
    private static final int NOTIFY_TIMEOUT = 2500; // ms
    private static final int NUM_SPECEXEC_TXNS = 5;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;

    private Procedure dtxnProc;
    private Procedure spProc;
    
    private PartitionExecutor executors[];
    private PartitionExecutor baseExecutor;
    private PartitionExecutor remoteExecutor;

    private final Semaphore lockBefore = DistributedBlockable.LOCK_BEFORE;
    private final Semaphore notifyBefore = DistributedBlockable.NOTIFY_BEFORE;
    private final Semaphore lockAfter = DistributedBlockable.LOCK_AFTER;
    
    
    private final TM1ProjectBuilder builder = new TM1ProjectBuilder() {
        {
            this.addAllDefaults();
            this.addProcedure(DistributedBlockable.class);
        }
    };
    
    /**
     * Simple conflict checker that allows anything to be executed
     */
    private final AbstractConflictChecker checker = new AbstractConflictChecker(null) {
        @Override
        public boolean ignoreProcedure(Procedure proc) {
            return (false);
        }
        @Override
        public boolean canExecute(AbstractTransaction dtxn, LocalTransaction ts, int partitionId) {
            return (true);
        }
    };
    
    // --------------------------------------------------------------------------------------------
    // DISTRIBUTED TXN CALLBACK
    // --------------------------------------------------------------------------------------------
    private ClientResponse dtxnResponse = null;
    private CountDownLatch dtxnLatch = new CountDownLatch(1);
    
    private final ProcedureCallback dtxnCallback = new ProcedureCallback() {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            assertNull(dtxnResponse);
            dtxnResponse = clientResponse;
            dtxnLatch.countDown();
        }
    };
    
    // --------------------------------------------------------------------------------------------
    // SINGLE-PARTITION TXN CALLBACK
    // --------------------------------------------------------------------------------------------
    private final List<ClientResponse> spResponses = new ArrayList<ClientResponse>();
    private CountDownLatch spLatch;
    private final ProcedureCallback spCallback = new ProcedureCallback() {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            assert(spLatch != null);
            spResponses.add(clientResponse);
            spLatch.countDown();
        }
    };
    
    // --------------------------------------------------------------------------------------------
    // SETUP / TEAR-DOWN
    // --------------------------------------------------------------------------------------------
    
    @Before
    public void setUp() throws Exception {
        super.setUp(this.builder);
        initializeCatalog(1, 1, NUM_PARTITIONS);
     
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.specexec_enable = true;
        this.hstore_conf.site.specexec_idle = true;
        this.hstore_conf.site.specexec_pre_query = true;
        this.hstore_conf.site.txn_client_debug = true;
        this.hstore_conf.site.exec_voltdb_procinfo = true;
        
        this.hstore_site = this.createHStoreSite(catalog_site, hstore_conf);
        this.client = createClient();
        
        assertFalse(this.lockBefore.hasQueuedThreads());
        assertFalse(this.notifyBefore.hasQueuedThreads());
        assertFalse(this.lockAfter.hasQueuedThreads());

        this.baseExecutor = this.hstore_site.getPartitionExecutor(BASE_PARTITION);
        assertNotNull(this.baseExecutor);
        this.remoteExecutor = this.hstore_site.getPartitionExecutor(BASE_PARTITION+1);
        assertNotNull(this.remoteExecutor);
        assertNotSame(this.baseExecutor.getPartitionId(), this.remoteExecutor.getPartitionId());
        this.executors = new PartitionExecutor[]{ this.baseExecutor, this.remoteExecutor };
        
        this.dtxnProc = this.getProcedure(DistributedBlockable.class);
        this.spProc = this.getProcedure(GetSubscriberData.class);
        
        // Make sure that we always set to false to ensure that the dtxn won't abort
        // unless the test case really wants it to
        DistributedBlockable.SHOULD_ABORT.set(false);
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testAllCommits
     */
    @Test
    public void testAllCommitsBefore() throws Exception {
        // We will submit a distributed transaction that will first acquire the locks
        // for all of the partitions and then block.
        // We will then submit a bunch of single-partition transactions that will execute
        // on the partition that's not the distributed txns base partition before the dtxn
        // does anything at the partition.
        // All of these txns should get speculatively executed but then never released
        // until we release our distributed txn.
        // All of the txns are going to commit successfully in the right order
        
        // Make sure that we replace the conflict checker on the remote partition
        // so that it can schedule our speculative txns
        PartitionExecutor.Debug remoteDebug = this.remoteExecutor.getDebugContext();
        remoteDebug.getSpecExecScheduler().setConflictChecker(this.checker);
        
        Object params[] = new Object[]{ BASE_PARTITION };
        this.client.callProcedure(this.dtxnCallback, this.dtxnProc.getName(), params);
        
        // Block until we know that the txn has started running
        boolean result = this.notifyBefore.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        // Make sure that this txn is the current dtxn at each of the partitions
        AbstractTransaction dtxn = null;
        for (PartitionExecutor executor : this.executors) {
            AbstractTransaction ts = null;
            int tries = 3;
            while (tries-- > 0) {
                ts = executor.getDebugContext().getCurrentDtxn();
                if (ts != null) break;
                ThreadUtil.sleep(NOTIFY_TIMEOUT);
            } // WHILE
            assertNotNull("No dtxn at " + executor.getPartition(), ts);
            if (dtxn == null) {
                dtxn = ts;
            } else {
                assertEquals(dtxn, ts);
            }
        } // FOR
        assertNotNull(dtxn);
        
        // Now fire off a bunch of single-partition txns
        params = new Object[]{ BASE_PARTITION+1 }; // S_ID
        for (int i = 0; i < NUM_SPECEXEC_TXNS; i++) {
            this.client.callProcedure(this.spCallback, this.spProc.getName(), params);
        } // FOR
        this.spLatch = new CountDownLatch(NUM_SPECEXEC_TXNS);
        
        // Wait until they have all been executed but make sure that nobody actually returned yet
        int tries = 3;
        while (tries-- > 0) {
            int blocked = remoteDebug.getBlockedSpecExecCount();
            if (blocked == NUM_SPECEXEC_TXNS) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertEquals(NUM_SPECEXEC_TXNS, remoteDebug.getBlockedSpecExecCount());
        
        // Now release the locks and then wait until the dtxn returns and all 
        // of the single-partition txns return
        this.lockAfter.release();
        this.lockBefore.release();
        
        result = this.dtxnLatch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+this.dtxnLatch, result);
        result = this.spLatch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH: "+this.spLatch, result);
        
        // Check to make sure that the dtxn succeeded
        assertEquals(Status.OK, this.dtxnResponse.getStatus());
        
        // And that all of our single-partition txns succeeded and were speculatively executed
        for (ClientResponse cr : this.spResponses) {
            assertNotNull(cr);
            assertEquals(cr.toString(), Status.OK, cr.getStatus());
            assertTrue(cr.toString(), cr.isSinglePartition());
            assertTrue(cr.toString(), cr.hasDebug());
            
            ClientResponseDebug crDebug = cr.getDebug();
            assertNotNull(crDebug);
            assertTrue(cr.toString(), crDebug.isSpeculative());
        } // FOR
        assertEquals(NUM_SPECEXEC_TXNS, this.spResponses.size());
    }
 

}
