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
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.specexecprocs.CheckSubscriber;
import org.voltdb.regressionsuites.specexecprocs.DistributedBlockable;
import org.voltdb.regressionsuites.specexecprocs.SinglePartitionAbortable;
import org.voltdb.sysprocs.LoadMultipartitionTable;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.specexec.AbstractConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
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
            this.addProcedure(SinglePartitionAbortable.class);
            this.addProcedure(CheckSubscriber.class);
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
    
    private class LatchableProcedureCallback implements ProcedureCallback {
        private final List<ClientResponse> responses = new ArrayList<ClientResponse>();
        private final CountDownLatch latch;
        LatchableProcedureCallback(int expected) {
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
        
        // We want to always insert one SUBSCRIBER record per partition so 
        // that we can play with them. Set VLR_LOCATION to zero so that 
        // can check whether it has been modified
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column catalog_col = this.getColumn(catalog_tbl, "VLR_LOCATION");
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = new Long(i);
            row[catalog_col.getIndex()] = 0l;
            vt.addRow(row);
        } // FOR
        String procName = VoltSystemProcedure.procCallName(LoadMultipartitionTable.class);
        ClientResponse cr = this.client.callProcedure(procName, catalog_tbl.getName(), vt);
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
        // HACK: Delete JAR
        if (catalogContext.jarPath != null && catalogContext.jarPath.exists()) {
            System.err.println("DELETE: " + catalogContext.jarPath);
            catalogContext.jarPath.delete();
        }
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
    
    private void checkClientResponses(Collection<ClientResponse> responses, Status status, boolean speculative, Integer restarts) {
        for (ClientResponse cr : responses) {
            assertNotNull(cr);
            assertEquals(cr.toString(), status, cr.getStatus());
            assertTrue(cr.toString(), cr.isSinglePartition());
            assertTrue(cr.toString(), cr.hasDebug());
            
            ClientResponseDebug crDebug = cr.getDebug();
            assertNotNull(crDebug);
            assertEquals(cr.getTransactionId() + " - SPECULATIVE", speculative, crDebug.isSpeculative());
            if (restarts != null) {
                assertEquals(cr.getTransactionId() + " - RESTARTS", restarts.intValue(), cr.getRestartCounter());
            }
        } // FOR
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testSpeculativeInterleavedAborts
     */
    @Test
    public void testSpeculativeInterleavedAborts() throws Exception {
        // This one is a bit more complicated. We're going to execute 
        // transactions where we interleave speculative txns that abort
        // We want to make sure that the final value is what we expect it to be
        Object params[] = new Object[]{ BASE_PARTITION };
        this.client.callProcedure(this.dtxnCallback, this.dtxnProc.getName(), params);
        
        // Block until we know that the txn has started running
        boolean result = this.notifyBefore.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        this.checkCurrentDtxn();
        
        // Now submit our aborting single-partition txn
        // This should be allowed to be speculatively executed right away
        Procedure spProc0 = this.getProcedure(SinglePartitionAbortable.class);
        Procedure spProc1 = this.getProcedure(CheckSubscriber.class);
        LatchableProcedureCallback spCallback0 = new LatchableProcedureCallback(NUM_SPECEXEC_TXNS);
        LatchableProcedureCallback spCallback1 = new LatchableProcedureCallback(NUM_SPECEXEC_TXNS);
        LatchableProcedureCallback spCallback2 = new LatchableProcedureCallback(NUM_SPECEXEC_TXNS);
        int MARKER = 1000;
        for (int i = 0; i < NUM_SPECEXEC_TXNS; i++) {
            // First txn will not abort
            params = new Object[]{ BASE_PARTITION+1, MARKER, 0 };
            this.client.callProcedure(spCallback0, spProc0.getName(), params);
            
            // Second txn will abort
            params = new Object[]{ BASE_PARTITION+1, MARKER+1, 1 };
            this.client.callProcedure(spCallback1, spProc0.getName(), params);
            
            // Third txn should only see the first txn's marker value
            params = new Object[]{ BASE_PARTITION+1, MARKER, 1 }; // SHOULD BE EQUAL!
            this.client.callProcedure(spCallback2, spProc1.getName(), params);
            
            MARKER += 1;
        } // FOR
        
        // We should get back all of the aborting txns' responses, but none from
        // the other txns
        result = spCallback1.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH1: "+spCallback1.latch, result);
        assertTrue(spCallback0.responses.isEmpty());
        assertTrue(spCallback2.responses.isEmpty());
        
        // Release all of the dtxn's locks
        this.lockBefore.release();
        this.lockAfter.release();
        result = this.dtxnLatch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+this.dtxnLatch, result);
        assertEquals(this.dtxnResponse.toString(), Status.OK, this.dtxnResponse.getStatus());
        
        // Now all of our single-partition txns should now come back to us too
        result = spCallback0.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH0: "+spCallback0.latch, result);
        result = spCallback2.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH2: "+spCallback2.latch, result);
        
        // The first + third batch should be all successful
        this.checkClientResponses(spCallback0.responses, Status.OK, true, 0);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback0.responses.size());
        this.checkClientResponses(spCallback2.responses, Status.OK, true, 0);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback2.responses.size());

        // The second batch should all have been aborted
        this.checkClientResponses(spCallback1.responses, Status.ABORT_USER, true, 0);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback1.responses.size());
    }
    
    /**
     * testSpeculativeAbort
     */
    @Test
    public void testSpeculativeAbort() throws Exception {
        // We're going to execute a dtxn that will block on the remote partition
        // We will then execute a single-partition transaction that will throw a user
        // abort. We will then execute a bunch of speculative txns that should *not*
        // see the changes made by the aborted txn
        Object params[] = new Object[]{ BASE_PARTITION };
        this.client.callProcedure(this.dtxnCallback, this.dtxnProc.getName(), params);
        
        // Block until we know that the txn has started running
        boolean result = this.notifyBefore.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        this.checkCurrentDtxn();
        
        // Now submit our aborting single-partition txn
        // This should be allowed to be speculatively executed right away
        Procedure spProc0 = this.getProcedure(SinglePartitionAbortable.class);
        LatchableProcedureCallback spCallback0 = new LatchableProcedureCallback(1);
        int MARKER = 9999;
        params = new Object[]{ BASE_PARTITION+1, MARKER, 1 };
        this.client.callProcedure(spCallback0, spProc0.getName(), params);

        // Now execute the second batch of single-partition txns
        // These should never see the changes made by our first single-partition txn
        Procedure spProc1 = this.getProcedure(CheckSubscriber.class);
        LatchableProcedureCallback spCallback1 = new LatchableProcedureCallback(NUM_SPECEXEC_TXNS);
        params = new Object[]{ BASE_PARTITION+1, MARKER, 0 }; // Should not be equal!
        for (int i = 0; i < NUM_SPECEXEC_TXNS; i++) {
            this.client.callProcedure(spCallback1, spProc1.getName(), params);
        } // FOR
        this.checkBlockedSpeculativeTxns(this.remoteExecutor, NUM_SPECEXEC_TXNS);
        
        // Release all of the dtxn's locks
        this.lockBefore.release();
        this.lockAfter.release();
        result = this.dtxnLatch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+this.dtxnLatch, result);
        assertEquals(this.dtxnResponse.toString(), Status.OK, this.dtxnResponse.getStatus());
        
        // All of our single-partition txns should now come back to us too
        result = spCallback0.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH0: "+spCallback0.latch, result);
        result = spCallback1.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SINGLE-P LATCH1: "+spCallback1.latch, result);
        
        // We should only have one response in the first batch that should have aborted
        this.checkClientResponses(spCallback0.responses, Status.ABORT_USER, true, 0);
        assertEquals(1, spCallback0.responses.size());

        // The second wave should have all succeeded with being marked as speculative
        // with no restarts
        this.checkClientResponses(spCallback1.responses, Status.OK, true, 0);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback1.responses.size());
    }
    
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
        LatchableProcedureCallback spCallback = new LatchableProcedureCallback(NUM_SPECEXEC_TXNS);
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
        this.checkClientResponses(spCallback.responses, Status.OK, true, null);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback.responses.size());
    }
    
    /**
     * testDtxnAbort
     */
    @Test
    public void testDtxnAbort() throws Exception {
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
        LatchableProcedureCallback spCallback0 = new LatchableProcedureCallback(NUM_SPECEXEC_TXNS);
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
        LatchableProcedureCallback spCallback1 = new LatchableProcedureCallback(NUM_SPECEXEC_TXNS);
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
        this.checkClientResponses(spCallback0.responses, Status.OK, true, 0);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback0.responses.size());
        // The second wave should have succeeded but with one restart and not marked as speculative
        this.checkClientResponses(spCallback1.responses, Status.OK, false, 1);
        assertEquals(NUM_SPECEXEC_TXNS, spCallback1.responses.size());
        
    }

}
