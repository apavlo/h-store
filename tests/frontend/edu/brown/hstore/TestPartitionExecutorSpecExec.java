package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
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
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

public class TestPartitionExecutorSpecExec extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestPartitionExecutorSpecExec.class);
    
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
    private final Semaphore lockAfter = DistributedBlockable.LOCK_AFTER;
    private final Semaphore notifyBefore = DistributedBlockable.NOTIFY_BEFORE;
    private final Semaphore notifyAfter = DistributedBlockable.NOTIFY_AFTER;
    
    
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
            System.err.printf("DTXN ClientResponse: %d / %s\n", clientResponse.getTransactionId(), clientResponse.getStatus());
            dtxnLatch.countDown();
        }
    };
    
    // --------------------------------------------------------------------------------------------
    // SINGLE-PARTITION TXN CALLBACK
    // --------------------------------------------------------------------------------------------
    
    private class SinglePartitionProcedureCallback implements ProcedureCallback {
        private final List<ClientResponse> responses = new ArrayList<ClientResponse>();
        private final CountDownLatch latch;
        SinglePartitionProcedureCallback(int expected) {
            this.latch = new CountDownLatch(expected);
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            this.responses.add(clientResponse);
            this.latch.countDown();
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
        
        // Make sure that we replace the conflict checker on the remote partition
        // so that it can schedule our speculative txns
        PartitionExecutor.Debug remoteDebug = this.remoteExecutor.getDebugContext();
        remoteDebug.getSpecExecScheduler().setConflictChecker(this.checker);
        
        // Make sure that we always set to false to ensure that the dtxn won't abort
        // unless the test case really wants it to
        DistributedBlockable.SHOULD_ABORT.set(false);
        this.lockBefore.drainPermits();
        this.lockAfter.drainPermits();
        this.notifyBefore.drainPermits();
        this.notifyAfter.drainPermits();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }

    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void checkCurrentDtxn() {
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
    }
    
    private void checkBlockedSpeculativeTxns(PartitionExecutor executor, int expected) {
        // Wait until they have all been executed but make sure that nobody actually returned yet
        int tries = 3;
        while (tries-- > 0) {
            int blocked = executor.getDebugContext().getBlockedSpecExecCount();
            if (blocked == expected) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertEquals(expected, executor.getDebugContext().getBlockedSpecExecCount());
    }
    
    private void checkClientResponses(Collection<ClientResponse> responses, boolean speculative, Integer restarts) {
        for (ClientResponse cr : responses) {
            assertNotNull(cr);
            assertEquals(cr.toString(), Status.OK, cr.getStatus());
            assertTrue(cr.toString(), cr.isSinglePartition());
            assertTrue(cr.toString(), cr.hasDebug());
            
            ClientResponseDebug crDebug = cr.getDebug();
            assertNotNull(crDebug);
            assertEquals("SPECULATIVE", speculative, crDebug.isSpeculative());
            if (restarts != null) {
                assertEquals("RESTARTS", restarts.intValue(), cr.getRestartCounter());
            }
        } // FOR
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testAllCommitsBefore
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
        Object params[] = new Object[]{ BASE_PARTITION };
        this.client.callProcedure(this.dtxnCallback, this.dtxnProc.getName(), params);
        
        // Block until we know that the txn has started running
        boolean result = this.notifyBefore.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        this.checkCurrentDtxn();
        
        // Now fire off a bunch of single-partition txns
        SinglePartitionProcedureCallback spCallback = new SinglePartitionProcedureCallback(NUM_SPECEXEC_TXNS);
        params = new Object[]{ BASE_PARTITION+1 }; // S_ID
        for (int i = 0; i < NUM_SPECEXEC_TXNS; i++) {
            this.client.callProcedure(spCallback, this.spProc.getName(), params);
        } // FOR
        this.checkBlockedSpeculativeTxns(this.remoteExecutor, NUM_SPECEXEC_TXNS);
        
        // Now release the locks and then wait until the dtxn returns and all 
        // of the single-partition txns return
        this.lockAfter.release();
        this.lockBefore.release();
        
        result = this.dtxnLatch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+this.dtxnLatch, result);
        result = spCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH: "+spCallback.latch, result);
        
        // Check to make sure that the dtxn succeeded
        assertEquals(Status.OK, this.dtxnResponse.getStatus());
        
        // And that all of our single-partition txns succeeded and were speculatively executed
        this.checkClientResponses(spCallback.responses, true, null);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback.responses.size());
    }
    
    /**
     * testDtxnAbortBefore
     */
    @Test
    public void testDtxnAbortBefore() throws Exception {
        // Execute a dtxn that will abort *after* it executes a query
        // We will also issue two batches of single-p txns. The first batch
        // will get executed before the dtxn executes a query at the remote partition
        // The second will get executed after the dtxn executed a query.
        // When the dtxn aborts, this means that all of the txns in the first batch
        // will be allowed to commit, but the second batch will get restarted
        Object params[] = new Object[]{ BASE_PARTITION };
        DistributedBlockable.SHOULD_ABORT.set(true);
        this.client.callProcedure(this.dtxnCallback, this.dtxnProc.getName(), params);
        
        // Block until we know that the txn has started running
        boolean result = this.notifyBefore.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        this.checkCurrentDtxn();
        
        // Now fire off our first batch of single-partition txns
        // This should be allowed to be speculatively executed right away
        SinglePartitionProcedureCallback spCallback0 = new SinglePartitionProcedureCallback(NUM_SPECEXEC_TXNS);
        params = new Object[]{ BASE_PARTITION+1 }; // S_ID
        for (int i = 0; i < NUM_SPECEXEC_TXNS; i++) {
            this.client.callProcedure(spCallback0, this.spProc.getName(), params);
        } // FOR
        this.checkBlockedSpeculativeTxns(this.remoteExecutor, NUM_SPECEXEC_TXNS);
        LOG.info(StringUtil.header("BEFORE"));
        
        // Release the before lock, then wait until it reaches the next barrier
        this.lockBefore.release();
        result = this.notifyAfter.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        // Now execute the second batch of single-partition txns
        // All of these will get restarted when the dtxn gets aborted
        SinglePartitionProcedureCallback spCallback1 = new SinglePartitionProcedureCallback(NUM_SPECEXEC_TXNS);
        for (int i = 0; i < NUM_SPECEXEC_TXNS; i++) {
            this.client.callProcedure(spCallback1, this.spProc.getName(), params);
        } // FOR
        this.checkBlockedSpeculativeTxns(this.remoteExecutor, NUM_SPECEXEC_TXNS*2);
        LOG.info(StringUtil.header("AFTER"));
        
        // We will now release the last lock. The dtxn will abort and will
        // get its response
        this.lockAfter.release();
        result = this.dtxnLatch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+this.dtxnLatch, result);
        assertEquals(this.dtxnResponse.toString(), Status.ABORT_USER, this.dtxnResponse.getStatus());
        
        // All of our single-partition txns should now come back to us too
        result = spCallback0.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH0: "+spCallback0.latch, result);
        result = spCallback1.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH1: "+spCallback1.latch, result);
        
        // The first wave should have succeeded with zero restarts and marked as speculative
        this.checkClientResponses(spCallback0.responses, true, 0);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback0.responses.size());
        // The second wave should have succeeded but with one restart and not marked as speculative
        this.checkClientResponses(spCallback1.responses, false, 1);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback1.responses.size());
        
    }
 

}
