package edu.brown.hstore;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.ClientResponseDebug;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.regressionsuites.specexecprocs.DtxnTester;
import org.voltdb.sysprocs.ExecutorStatus;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.MockClientCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ThreadUtil;

public class TestHStoreSite extends BaseTestCase {
    
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = GetNewDestination.class;
    private static final long CLIENT_HANDLE = 1l;
    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_TUPLES = 10;
    private static final int NUM_TXNS = 5;
    private static final int BASE_PARTITION = 0;
    private static final int NOTIFY_TIMEOUT = 2500; // ms
    
    private HStoreSite hstore_site;
    private HStoreSite.Debug hstore_debug;
    private HStoreConf hstore_conf;
    private TransactionQueueManager.Debug queue_debug;
    private Client client;

    private static final ParameterSet PARAMS = new ParameterSet(
        new Long(0), // S_ID
        new Long(1), // SF_TYPE
        new Long(2), // START_TIME
        new Long(3)  // END_TIME
    );

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
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.status_interval = 4000;
        this.hstore_conf.site.anticache_enable = false;
        this.hstore_conf.site.specexec_enable = false;
        this.hstore_conf.site.txn_incoming_delay = 5;
        this.hstore_conf.site.exec_voltdb_procinfo = true;
        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.hstore_debug = this.hstore_site.getDebugContext();
        this.queue_debug = this.hstore_site.getTransactionQueueManager().getDebugContext();
        this.client = createClient();
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
    
    private void loadData(Table catalog_tbl) throws Exception {
        // Load some data directly into the EEs without going through transactions
        VoltTable vts[] = {
            CatalogUtil.getVoltTable(catalog_tbl),
            CatalogUtil.getVoltTable(catalog_tbl)
        };
        assertEquals(NUM_PARTITIONS, vts.length);
        AbstractHasher hasher = p_estimator.getHasher();
        Column sub_nbr = catalog_tbl.getColumns().getIgnoreCase("SUB_NBR");
        Column sf_type = catalog_tbl.getColumns().getIgnoreCase("SF_TYPE");
        Column start_time = catalog_tbl.getColumns().getIgnoreCase("START_TIME");
        
        for (int i = 0; i < NUM_TUPLES; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = Integer.valueOf(i);
            
            // Column Fixes
            if (sub_nbr != null) row[sub_nbr.getIndex()] = row[0].toString();
            if (sf_type != null) row[sf_type.getIndex()] = 1l;
            if (start_time != null) row[start_time.getIndex()] = 1l;
            
            vts[hasher.hash(row[0])].addRow(row);
        } // FOR
        for (int i = 0; i < vts.length; i++) {
            PartitionExecutor executor = hstore_site.getPartitionExecutor(i);
            executor.loadTable((long)i, catalog_tbl, vts[i], false);
            // System.err.println(catalog_tbl + " - " + i + "\n" + VoltTableUtil.format(vts[i]) + "\n");
        } // FOR
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testSinglePartitionTxn
     */
    @Test
    public void testSinglePartitionTxn() throws Exception {
        // Simple test to check whether we can execute single-partition txns serially
        Procedure catalog_proc = this.getProcedure(GetSubscriberData.class);
        for (int i = 0; i < 4; i++) {
            Object params[] = { (long)i };
            ClientResponse cr = this.client.callProcedure(catalog_proc.getName(), params);
            assertEquals(Status.OK, cr.getStatus());
        }
//        System.err.println(cr);
        this.statusSnapshot();
    }
    
    /**
     * testMultiPartitionTxn
     */
    @Test
    public void testMultiPartitionTxn() throws Exception {
        this.loadData(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        this.loadData(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING));
        
        // Simple test to check whether we can execute multi-partition txns serially
        Procedure catalog_proc = this.getProcedure(DeleteCallForwarding.class);
        for (int i = 0; i < NUM_TXNS; i++) {
            Object params[] = { Integer.toString(i), 1l, 1l };
            ClientResponse cr = this.client.callProcedure(catalog_proc.getName(), params);
            assertEquals(Status.OK, cr.getStatus());
        }
//        System.err.println(cr);
        this.statusSnapshot();
    }
    
    /**
     * testMultiPartitionTxnAsynchronous
     */
    @Test
    public void testMultiPartitionTxnAsynchronous() throws Exception {
        this.loadData(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        this.loadData(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING));
        
        HStoreSiteTestUtil.LatchableProcedureCallback callback = new HStoreSiteTestUtil.LatchableProcedureCallback(NUM_TXNS + 1);
        
        // Submit our first dtxn that will block until we tell it to go
        DtxnTester.NOTIFY_BEFORE.drainPermits();
        DtxnTester.LOCK_BEFORE.drainPermits();
        DtxnTester.NOTIFY_AFTER.drainPermits();
        DtxnTester.LOCK_AFTER.drainPermits();
        Procedure catalog_proc = this.getProcedure(DtxnTester.class);
        Object params[] = new Object[]{ BASE_PARTITION };
        this.client.callProcedure(callback, catalog_proc.getName(), params);
        
        // Block until we know that the txn has started running
        boolean result = DtxnTester.NOTIFY_BEFORE.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);

        // Now fire off all of our other dtxns. They should not 
        // get executed until our first guy is finished.
        catalog_proc = this.getProcedure(DeleteCallForwarding.class);
        for (int i = 0; i < NUM_TXNS; i++) {
            params = new Object[]{ Integer.toString(i), 1l, 1l };
            this.client.callProcedure(callback, catalog_proc.getName(), params);
        } // FOR
        
        // Sleep for a bit. We still should not have gotten back any responses
        ThreadUtil.sleep(NOTIFY_TIMEOUT);
        assertEquals(0, callback.responses.size());
        assertEquals(NUM_TXNS+1, callback.latch.getCount());
        
        // Ok, so let's release the blocked dtxn. This will allow everyone 
        // else to come to the party
        DtxnTester.LOCK_AFTER.release();
        DtxnTester.LOCK_BEFORE.release();
        result = callback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH --> " + callback.latch, result);
        
        // Check to make sure the responses are all legit
        for (ClientResponse cr : callback.responses) {
            assertEquals(cr.toString(), Status.OK, cr.getStatus());
        } // FOR
        
        this.statusSnapshot();
    }
    
    /**
     * testMixedAsynchronous
     */
    @Test
    public void testMixedAsynchronous() throws Exception {
        this.loadData(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        this.loadData(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING));
        
        // Submit our first dtxn that will block until we tell it to go
        HStoreSiteTestUtil.LatchableProcedureCallback blockedCallback = new HStoreSiteTestUtil.LatchableProcedureCallback(1);
        DtxnTester.NOTIFY_BEFORE.drainPermits();
        DtxnTester.LOCK_BEFORE.drainPermits();
        DtxnTester.NOTIFY_AFTER.drainPermits();
        DtxnTester.LOCK_AFTER.drainPermits();
        Procedure catalog_proc = this.getProcedure(DtxnTester.class);
        Object params[] = new Object[]{ BASE_PARTITION };
        this.client.callProcedure(blockedCallback, catalog_proc.getName(), params);
        // Block until we know that the txn has started running
        boolean result = DtxnTester.NOTIFY_BEFORE.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);

        // Fire off a mix of single-partition txns and distributed txns
        // Since we will delay each invocation, we know that the txn ids
        // will be farther enough apart that we should expect them to be
        // returned in the proper order
        Procedure spProc = this.getProcedure(GetSubscriberData.class);
        HStoreSiteTestUtil.LatchableProcedureCallback spCallback = new HStoreSiteTestUtil.LatchableProcedureCallback(NUM_TXNS);
        // spCallback.setDebug(true);
        Procedure mpProc = this.getProcedure(DeleteCallForwarding.class);
        HStoreSiteTestUtil.LatchableProcedureCallback mpCallback = new HStoreSiteTestUtil.LatchableProcedureCallback(NUM_TXNS);
        // mpCallback.setDebug(true);
        
        for (int i = 0; i < NUM_TXNS; i++) {
            // SINGLE-PARTITION
            params = new Object[]{ new Long(i) };
            this.client.callProcedure(spCallback, spProc.getName(), params);
            ThreadUtil.sleep(100);
            
            // MULTI-PARTITION
            params = new Object[]{ Integer.toString(i), 1l, 1l };
            this.client.callProcedure(mpCallback, mpProc.getName(), params);
            ThreadUtil.sleep(100);
        } // FOR
        
        // Sleep for a bit. We still should have gotten back any responses
        ThreadUtil.sleep(NOTIFY_TIMEOUT);
        assertEquals(0, spCallback.responses.size());
        assertEquals(NUM_TXNS, spCallback.latch.getCount());
        assertEquals(0, mpCallback.responses.size());
        assertEquals(NUM_TXNS, mpCallback.latch.getCount());
        
        // Ok, so let's release the blocked dtxn. This will allow everyone 
        // else to come to the party
        DtxnTester.LOCK_AFTER.release();
        DtxnTester.LOCK_BEFORE.release();
        result = blockedCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("BLOCKING LATCH --> " + blockedCallback.latch, result);
        
        // Wait until we get back all the other responses and then check to make 
        // sure the responses are ok.
        result = spCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SP LATCH --> " + spCallback.latch, result);
        result = mpCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("MP LATCH --> " + mpCallback.latch, result);

        // Although the results may come back in a different order, we expect that
        // their transaction ids are in the right order. We'll sort them by their
        // clientHandle, which is the ordered id of when they were sent to the cluster.
        Comparator<ClientResponse> sorter = new Comparator<ClientResponse>() {
            @Override
            public int compare(ClientResponse o1, ClientResponse o2) {
                return (int)(o1.getClientHandle() - o2.getClientHandle());
            }
        };
        Collections.sort(spCallback.responses, sorter);
        Collections.sort(mpCallback.responses, sorter);
        
        // Just make sure that the txnIds are always increasing
        long lastTxnIds[] = new long[NUM_PARTITIONS];
        int basePartition;
        long txnId;
        Arrays.fill(lastTxnIds, -1l);
        for (int i = 0; i < NUM_TXNS; i++) {
            ClientResponse spResponse = spCallback.responses.get(i);
            assertEquals(spResponse.toString(), Status.OK, spResponse.getStatus());
            txnId = spResponse.getTransactionId();
            basePartition = spResponse.getBasePartition();
            assertTrue(String.format("%02d :: LAST[%d] < SP[%d]", basePartition, lastTxnIds[basePartition], txnId),
                       lastTxnIds[basePartition] < txnId);
            lastTxnIds[basePartition] = txnId;
            
            ClientResponse mpResponse = mpCallback.responses.get(i);
            assertEquals(mpResponse.toString(), Status.OK, mpResponse.getStatus());
            txnId = mpResponse.getTransactionId();
            basePartition = mpResponse.getBasePartition();
            assertTrue(String.format("%02d :: LAST[%d] < SP[%d]", basePartition, lastTxnIds[basePartition], txnId),
                    lastTxnIds[basePartition] < txnId);
            lastTxnIds[basePartition] = txnId;
        } // FOR
        
        this.statusSnapshot();
    }
    
    /**
     * testClientResponseDebug
     */
    @Test
    public void testClientResponseDebug() throws Exception {
         hstore_conf.site.txn_client_debug = true;
        
        // Submit a transaction and check that our ClientResponseDebug matches
        // what the transaction was initialized with
        final Map<Long, LocalTransaction> copiedHandles = new HashMap<Long, LocalTransaction>(); 
        EventObserver<LocalTransaction> newTxnObserver = new EventObserver<LocalTransaction>() {
            @Override
            public void update(EventObservable<LocalTransaction> o, LocalTransaction ts) {
                LocalTransaction copy = new LocalTransaction(hstore_site);
                copy.init(ts.getTransactionId(),
                          ts.getInitiateTime(),
                          ts.getClientHandle(),
                          ts.getBasePartition(),
                          new PartitionSet(ts.getPredictTouchedPartitions()),
                          ts.isPredictReadOnly(),
                          ts.isPredictAbortable(),
                          ts.getProcedure(),
                          ts.getProcedureParameters(),
                          null);
                copiedHandles.put(ts.getTransactionId(), copy);
            }
        };
        hstore_site.getTransactionInitializer().getNewTxnObservable().addObserver(newTxnObserver);
        
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        Object params[] = { 1234l, "XXXX" };
        ClientResponse cr = this.client.callProcedure(catalog_proc.getName(), params);
        assertEquals(Status.OK, cr.getStatus());
        // System.err.println(cr);
        // System.err.println(StringUtil.formatMaps(copiedHandles));
        
        assertTrue(cr.hasDebug());
        ClientResponseDebug crDebug = cr.getDebug();
        assertNotNull(crDebug);
        
        LocalTransaction copy = copiedHandles.get(cr.getTransactionId());
        assertNotNull(copiedHandles.toString(), copy);
        assertEquals(copy.getTransactionId().longValue(), cr.getTransactionId());
        assertEquals(copy.getClientHandle(), cr.getClientHandle());
        assertEquals(copy.getBasePartition(), cr.getBasePartition());
        assertEquals(copy.isPredictAbortable(), crDebug.isPredictAbortable());
        assertEquals(copy.isPredictReadOnly(), crDebug.isPredictReadOnly());
        assertEquals(copy.isPredictSinglePartition(), crDebug.isPredictSinglePartition());
        assertEquals(copy.getPredictTouchedPartitions(), crDebug.getPredictTouchedPartitions());
    }
    
    /**
     * testTransactionCounters
     */
    @Test
    public void testTransactionCounters() throws Exception {
        hstore_conf.site.txn_counters = true;
        hstore_site.updateConf(hstore_conf, null);
        
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        ClientResponse cr = null;
        int num_txns = 500;
        
        Object params[] = { 1234l, "XXXX" };
        for (int i = 0; i < num_txns; i++) {
            this.client.callProcedure(catalog_proc.getName(), params);
        } // FOR
        ThreadUtil.sleep(1000);
        assertEquals(num_txns, TransactionCounter.RECEIVED.get());
        
        // Now try invoking @Statistics to get back more information
        params = new Object[]{ SysProcSelector.TXNCOUNTER.name(), 0 };
        cr = this.client.callProcedure(VoltSystemProcedure.procCallName(Statistics.class), params);
//        System.err.println(cr);
        assertNotNull(cr);
        assertEquals(Status.OK, cr.getStatus());
        
        VoltTable results[] = cr.getResults();
        assertEquals(1, results.length);
        boolean found = false;
        while (results[0].advanceRow()) {
            if (results[0].getString(3).equalsIgnoreCase(catalog_proc.getName())) {
                for (int i = 4; i < results[0].getColumnCount(); i++) {
                    String counterName = results[0].getColumnName(i);
                    TransactionCounter tc = TransactionCounter.get(counterName);
                    assertNotNull(counterName, tc);
                    
                    Long tcVal = tc.get(catalog_proc);
                    if (tcVal == null) tcVal = 0l;
                    assertEquals(counterName, tcVal.intValue(), (int)results[0].getLong(i));
                } // FOR
                found = true;
                break;
            }
        } // WHILE
        assertTrue(found);
    }
    
    /**
     * testTransactionProfilers
     */
    @Test
    public void testTransactionProfilers() throws Exception {
        hstore_conf.site.txn_counters = true;
        hstore_conf.site.txn_profiling = true;
        hstore_site.updateConf(hstore_conf, null);
        
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        ClientResponse cr = null;
        int num_txns = 500;
        
        Object params[] = { 1234l, "XXXX" };
        for (int i = 0; i < num_txns; i++) {
            this.client.callProcedure(catalog_proc.getName(), params);
        } // FOR
        ThreadUtil.sleep(1000);
        assertEquals(num_txns, TransactionCounter.RECEIVED.get());
        
        // Now try invoking @Statistics to get back more information
        // Invoke it multiple times to make sure we get something...
        String procName = VoltSystemProcedure.procCallName(Statistics.class);
        params = new Object[]{ SysProcSelector.TXNPROFILER.name(), 0 };
        String fields[] = { "TOTAL", "INIT_TOTAL" };
        for (int ii = 0; ii < 5; ii++) {
            cr = this.client.callProcedure(procName, params);
            System.err.println(VoltTableUtil.format(cr.getResults()[0]));
            assertNotNull(cr);
            assertEquals(Status.OK, cr.getStatus());
            
            VoltTable results[] = cr.getResults();
            assertEquals(1, results.length);
            
            if (ii != 0) continue;
            
            boolean found = false;
            results[0].resetRowPosition();
            while (results[0].advanceRow()) {
                if (results[0].getString(3).equalsIgnoreCase(catalog_proc.getName())) {
                    for (String f : fields) {
                        int i = results[0].getColumnIndex(f);
                        assertEquals(f, results[0].getColumnName(i));
                        long val = results[0].getLong(i);
                        assertFalse(f, results[0].wasNull());
                        assertTrue(f, val > 0);
                    } // FOR
                    found = true;
                    break;
                }
            } // WHILE
            assertTrue(found);
        } // FOR
    }
    
    /**
     * testSendClientResponse
     */
    @Test
    public void testSendClientResponse() throws Exception {
        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        PartitionSet predict_touchedPartitions = new PartitionSet(BASE_PARTITION);
        boolean predict_readOnly = true;
        boolean predict_canAbort = true;
        
        MockClientCallback callback = new MockClientCallback();
        
        LocalTransaction ts = new LocalTransaction(hstore_site);
        ts.init(1000l, EstTime.currentTimeMillis(), CLIENT_HANDLE, BASE_PARTITION,
                predict_touchedPartitions, predict_readOnly, predict_canAbort,
                catalog_proc, PARAMS, callback);
        
        ClientResponseImpl cresponse = new ClientResponseImpl(ts.getTransactionId(),
                                                              ts.getClientHandle(),
                                                              ts.getBasePartition(),
                                                              Status.OK,
                                                              HStoreConstants.EMPTY_RESULT,
                                                              "");
        hstore_site.responseSend(ts, cresponse);
        
        // Check to make sure our callback got the ClientResponse
        // And just make sure that they're the same
        assertEquals(callback, ts.getClientCallback());
        ClientResponseImpl clone = callback.getResponse();
        assertNotNull(clone);
        assertEquals(cresponse.getTransactionId(), clone.getTransactionId());
        assertEquals(cresponse.getClientHandle(), clone.getClientHandle());
    }
    
//    /**
//     * testAbortReject
//     */
//    @Test
//    public void testAbortReject() throws Exception {
//        // Check to make sure that we reject a bunch of txns that all of our
//        // handles end up back in the object pool. To do this, we first need
//        // to set the PartitionExecutor's to reject all incoming txns
//        // hstore_conf.site.network_incoming_max_per_partition = 4;
//        hstore_conf.site.txn_restart_limit = 0;
//        hstore_conf.site.exec_force_allpartitions = true;
//        hstore_site.updateConf(hstore_conf);
//
//        final Set<LocalTransaction> expectedHandles = new HashSet<LocalTransaction>(); 
//        final List<Long> expectedIds = new ArrayList<Long>();
//        
//        EventObserver<LocalTransaction> newTxnObserver = new EventObserver<LocalTransaction>() {
//            @Override
//            public void update(EventObservable<LocalTransaction> o, LocalTransaction ts) {
//                expectedHandles.add(ts);
//                assertFalse(ts.toString(), expectedIds.contains(ts.getTransactionId()));
//                expectedIds.add(ts.getTransactionId());
//            }
//        };
//        hstore_site.getTransactionInitializer().getNewTxnObservable().addObserver(newTxnObserver);
//        
//        // We need to get at least one ABORT_REJECT
//        final int num_txns = 500;
//        final CountDownLatch latch = new CountDownLatch(num_txns);
//        final AtomicInteger numAborts = new AtomicInteger(0);
//        final Histogram<Status> statusHistogram = new ObjectHistogram<Status>();
//        final List<Long> actualIds = new ArrayList<Long>();
//         
//        ProcedureCallback callback = new ProcedureCallback() {
//            @Override
//            public void clientCallback(ClientResponse cr) {
//                statusHistogram.put(cr.getStatus());
//                if (cr.getStatus() == Status.ABORT_REJECT) {
//                    numAborts.incrementAndGet();
//                }
//                if (cr.getTransactionId() > 0) actualIds.add(cr.getTransactionId());
//                latch.countDown();
//            }
//        };
//        
//        // Then blast out a bunch of txns that should all come back as rejected
//        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
//        Object params[] = { 1234l, "XXXX" };
//        for (int i = 0; i < num_txns; i++) {
//            boolean queued = this.client.callProcedure(callback, catalog_proc.getName(), params);
//            assertTrue(queued);
//            if (queued == false) latch.countDown();
//        } // FOR
//        
//        boolean result = latch.await(20, TimeUnit.SECONDS);
////        System.err.println("InflightTxnCount: " + hstore_debug.getInflightTxnCount());
////        System.err.println("DeletableTxnCount: " + hstore_debug.getDeletableTxnCount());
////        System.err.println("--------------------------------------------");
////        System.err.println("EXPECTED IDS:");
////        System.err.println(StringUtil.join("\n", CollectionUtil.sort(expectedIds)));
////        System.err.println("--------------------------------------------");
////        System.err.println("ACTUAL IDS:");
////        System.err.println(StringUtil.join("\n", CollectionUtil.sort(actualIds)));
////        System.err.println("--------------------------------------------");
//        
//        System.err.println(statusHistogram);
//        System.err.println(hstore_site.statusSnapshot());
//        assertTrue("Timed out [latch="+latch.getCount() + "]", result);
//        assertNotSame(0, expectedHandles.size());
//        assertNotSame(0, expectedIds.size());
//        assertNotSame(0, actualIds.size());
//        assertNotSame(0, numAborts.get());
//        
//        // HACK: Wait a little to know that the periodic thread has attempted
//        // to clean-up our deletable txn handles
//        int sleepTime = 5000;
//        System.err.printf("Sleeping for %.1f seconds... ", sleepTime/1000d);
//        ThreadUtil.sleep(sleepTime);
//        System.err.println("Awake!");
//
//        assertEquals(0, hstore_debug.getDeletableTxnCount());
//        assertEquals(0, hstore_debug.getInflightTxnCount());
//        
//        // Make sure that there is nothing sitting around in our queues
//        assertEquals("INIT", 0, queue_debug.getInitQueueSize());
//        assertEquals("BLOCKED", 0, queue_debug.getBlockedQueueSize());
//        assertEquals("RESTART", 0, queue_debug.getRestartQueueSize());
//        
//        // Check to make sure that all of our handles are not initialized
//        // XXX: We only need to do this if object pooling is enabled
//        if (hstore_conf.site.pool_txn_enable) {
//            System.err.println("Checking whether object pools are cleaned up...");
//            for (LocalTransaction ts : expectedHandles) {
//                assertNotNull(ts);
//                if (ts.isInitialized()) System.err.println(ts.debug());
//                assertFalse(ts.debug(), ts.isInitialized());
//            } // FOR
//        }
//        
//        // Then check to make sure that there aren't any active objects in the
//        // the various object pools
//        Map<String, TypedObjectPool<?>[]> allPools = this.objectPools.getPartitionedPools(); 
//        assertNotNull(allPools);
//        assertFalse(allPools.isEmpty());
//        for (String name : allPools.keySet()) {
//            TypedObjectPool<?> pools[] = allPools.get(name);
//            TypedPoolableObjectFactory<?> factory = null;
//            assertNotNull(name, pools);
//            assertNotSame(0, pools.length);
//            for (int i = 0; i < pools.length; i++) {
//                if (pools[i] == null) continue;
//                String poolName = String.format("%s-%02d", name, i);  
//                factory = (TypedPoolableObjectFactory<?>)pools[i].getFactory();
//                assertTrue(poolName, factory.isCountingEnabled());
//                
//                System.err.println(poolName + ": " + pools[i].toString());
//                assertEquals(poolName, 0, pools[i].getNumActive());
//            } // FOR
//        } // FOR
//    }

}
