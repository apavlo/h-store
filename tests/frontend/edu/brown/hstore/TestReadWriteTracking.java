/**
 * 
 */
package edu.brown.hstore;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.regressionsuites.TestSmallBankSuite;
import org.voltdb.regressionsuites.specexecprocs.BlockingSendPayment;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.smallbank.SmallBankProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ThreadUtil;

/**
 * REad/Write Set Tracking
 * @author pavlo
 */
public class TestReadWriteTracking extends BaseTestCase {

    private static final int NUM_PARTITONS = 1;
    private static final int BASE_PARTITION = 0;
    private static final int NOTIFY_TIMEOUT = 1000; // ms
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;
    private PartitionExecutor executor;
    private ExecutionEngine ee;
    private PartitionExecutor.Debug executorDebug;
    private Procedure blockingProc;
    
    private final SmallBankProjectBuilder builder = new SmallBankProjectBuilder() {
        {
            this.addAllDefaults();
            this.addProcedure(BlockingSendPayment.class);
        }
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(builder);
        this.addPartitions(NUM_PARTITONS);
        
        this.blockingProc = this.getProcedure(BlockingSendPayment.class);
        
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.exec_readwrite_tracking = true;
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_site = this.createHStoreSite(catalog_site, hstore_conf);
        
        this.executor = hstore_site.getPartitionExecutor(0);
        assertNotNull(this.executor);
        this.ee = executor.getExecutionEngine();
        assertNotNull(this.executor);

        this.client = createClient();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    @SuppressWarnings("unchecked")
    private <T extends VoltProcedure> T getCurrentVoltProcedure(PartitionExecutor executor, Class<T> expectedType) {
        int tries = 3;
        VoltProcedure voltProc = null;
        while (tries-- > 0) {
            voltProc = executor.getDebugContext().getCurrentVoltProcedure();
            if (voltProc != null) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertNotNull(String.format("Failed to get %s from %s", expectedType, executor), voltProc);
        assertEquals(expectedType, voltProc.getClass());
        return ((T)voltProc);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------

//    /**
//     * testEnableTracking
//     */
//    @Test
//    public void testEnableTracking() throws Exception {
//        Long txnId = 12345l;
//        this.ee.trackingEnable(txnId);
//    }
//    
//    /**
//     * testFinishTracking
//     */
//    @Test
//    public void testFinishTracking() throws Exception {
//        Long txnId = 12345l;
//        this.ee.trackingEnable(txnId);
//        this.ee.trackingFinish(txnId);
//    }
    
    /**
     * testReadSets
     */
    @Test
    public void testReadSets() throws Exception {
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, this.client);
        
        // Fire off a distributed a txn that will block.
        Object dtxnParams[] = new Object[]{ BASE_PARTITION, BASE_PARTITION+1, 1.0 };
        StoredProcedureInvocationHints dtxnHints = new StoredProcedureInvocationHints();
        dtxnHints.basePartition = BASE_PARTITION;
        LatchableProcedureCallback dtxnCallback = new LatchableProcedureCallback(1);
        this.client.callProcedure(dtxnCallback, this.blockingProc.getName(), dtxnHints, dtxnParams);
        
        // Block until we know that the txn has started running
        BlockingSendPayment voltProc = this.getCurrentVoltProcedure(this.executor, BlockingSendPayment.class); 
        assertNotNull(voltProc);
        boolean result = voltProc.NOTIFY_BEFORE.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        Long txnId = voltProc.getTransactionId();
        assertNotNull(txnId);
        
        // Ok now we're going to release our txn. It will execute a bunch of stuff.
        voltProc.LOCK_BEFORE.release();
        result = voltProc.NOTIFY_AFTER.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        // It's blocked again. Let's take a peek at its ReadSet
        VoltTable readSet = this.ee.trackingReadSet(txnId);
        assertNotNull(readSet);
        System.err.println("READ SET:\n" + VoltTableUtil.format(readSet));
        ThreadUtil.sleep(10000000);
        
        // Check to make sure that the dtxn succeeded
        voltProc.LOCK_AFTER.release();
        result = dtxnCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+dtxnCallback.latch, result);
        assertEquals(Status.OK, CollectionUtil.first(dtxnCallback.responses).getStatus());
    }

    
}
