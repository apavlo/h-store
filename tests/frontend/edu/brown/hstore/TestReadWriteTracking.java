/**
 * 
 */
package edu.brown.hstore;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.regressionsuites.TestSmallBankSuite;
import org.voltdb.regressionsuites.specexecprocs.BlockingSendPayment;
import org.voltdb.sysprocs.AdHoc;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.smallbank.SmallBankConstants;
import edu.brown.benchmark.smallbank.SmallBankProjectBuilder;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;

/**
 * Read/Write Set Tracking
 * @author pavlo
 */
public class TestReadWriteTracking extends BaseTestCase {

    private static final int NUM_PARTITONS = 1;
    private static final int BASE_PARTITION = 0;
    
    public static final VoltTable.ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("TABLE_NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("TUPLE_ID", VoltType.INTEGER),
    };
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;
    private PartitionExecutor executor;
    private ExecutionEngine ee;
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
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private BlockingSendPayment startTxn() throws Exception {
        // Fire off a distributed a txn that will block.
        Object dtxnParams[] = new Object[]{ BASE_PARTITION, BASE_PARTITION+1, 1.0 };
        StoredProcedureInvocationHints dtxnHints = new StoredProcedureInvocationHints();
        dtxnHints.basePartition = BASE_PARTITION;
        LatchableProcedureCallback dtxnCallback = new LatchableProcedureCallback(1);
        this.client.callProcedure(dtxnCallback, this.blockingProc.getName(), dtxnHints, dtxnParams);
        
        // Block until we know that the txn has started running
        BlockingSendPayment voltProc = HStoreSiteTestUtil.getCurrentVoltProcedure(this.executor, BlockingSendPayment.class); 
        assertNotNull(voltProc);
        boolean result = voltProc.NOTIFY_BEFORE.tryAcquire(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        // Ok now we're going to release our txn. It will execute a bunch of stuff.
        voltProc.LOCK_BEFORE.release();
        result = voltProc.NOTIFY_AFTER.tryAcquire(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        
        return (voltProc);
    }
    
    private void finishTxn(BlockingSendPayment voltProc) throws Exception {
        // Check to make sure that the dtxn succeeded
        voltProc.LOCK_AFTER.release();
        // VoltTable result[] = dtxnCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        // assertTrue("DTXN LATCH"+dtxnCallback.latch, result);
        // assertEquals(Status.OK, CollectionUtil.first(dtxnCallback.responses).getStatus());
    }
    
    private void verifySchema(VoltTable vt) {
        assertEquals(RESULT_COLS.length, vt.getColumnCount());
        for (int i = 0; i < vt.getColumnCount(); i++) {
            assertEquals(Integer.toString(i), RESULT_COLS[i].getName(), vt.getColumnName(i));
            assertEquals(Integer.toString(i), RESULT_COLS[i].getType(), vt.getColumnType(i));
        } // FOR
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testCaching
     */
    @Test
    public void testCaching() throws Exception {
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, this.client);
        
        BlockingSendPayment voltProc = this.startTxn();
        Long txnId = voltProc.getTransactionId();
        assertNotNull(txnId);
        LocalTransaction ts = hstore_site.getTransaction(txnId);
        assertNotNull(ts);
        
        Set<String> expectedTables = new HashSet<String>();
        for (Table tbl : catalogContext.database.getTables()) {
            if (ts.isTableRead(BASE_PARTITION, tbl)) {
                expectedTables.add(tbl.getName());
            }
        } // FOR
        
        // We should always get back the same handle each time.
        try {
            VoltTable result0 = this.ee.trackingReadSet(txnId);
            VoltTable result1 = this.ee.trackingReadSet(txnId);
            assert(result0 == result1);
        } finally {
            this.finishTxn(voltProc);
        }
    }
    
    /**
     * testReadSetsAdHoc
     */
    @Test
    public void testReadSetsAdHoc() throws Exception {
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, this.client);
        
        String sql = String.format("SELECT * FROM %s WHERE custid = 1",
                                   SmallBankConstants.TABLENAME_ACCOUNTS);
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        ClientResponse cresponse = client.callProcedure(procName, sql);
        assert(cresponse.getStatus() == Status.OK) : cresponse.toString();

        // XXX: We currently have no way of checking the read/write set
        //      for adhoc queries, so I just have this for checking manually.
    }
    
    
    /**
     * testReadSets
     */
    @Test
    public void testReadSets() throws Exception {
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, this.client);
        
        BlockingSendPayment voltProc = this.startTxn();
        Long txnId = voltProc.getTransactionId();
        assertNotNull(txnId);
        LocalTransaction ts = hstore_site.getTransaction(txnId);
        assertNotNull(ts);
        
        Set<String> expectedTables = new HashSet<String>();
        for (Table tbl : catalogContext.database.getTables()) {
            if (ts.isTableRead(BASE_PARTITION, tbl)) {
                expectedTables.add(tbl.getName());
            }
        } // FOR
        
        // Let's take a peek at its ReadSet
        VoltTable result = this.ee.trackingReadSet(txnId);
        assertNotNull(result);
        this.verifySchema(result);
        Set<String> foundTables = new HashSet<String>();
        while (result.advanceRow()) {
            String tableName = result.getString(0);
            int tupleId = (int)result.getLong(1);
            foundTables.add(tableName);
            assert(tupleId >= 0);
        } // WHILE 
        this.finishTxn(voltProc);
        System.err.println("READ SET:\n" + VoltTableUtil.format(result));
        
        assertEquals(expectedTables, foundTables);
    }
    
    /**
     * testWriteSets
     */
    @Test
    public void testWriteSets() throws Exception {
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, this.client);
        
        BlockingSendPayment voltProc = this.startTxn();
        Long txnId = voltProc.getTransactionId();
        assertNotNull(txnId);
        LocalTransaction ts = hstore_site.getTransaction(txnId);
        assertNotNull(ts);
        
        Set<String> expectedTables = new HashSet<String>();
        for (Table tbl : catalogContext.database.getTables()) {
            if (ts.isTableWritten(BASE_PARTITION, tbl)) {
                expectedTables.add(tbl.getName());
            }
        } // FOR
        
        // Let's take a peek at its WriteSet
        VoltTable result = this.ee.trackingWriteSet(txnId);
        assertNotNull(result);
        this.verifySchema(result);
        Set<String> foundTables = new HashSet<String>();
        while (result.advanceRow()) {
            String tableName = result.getString(0);
            int tupleId = (int)result.getLong(1);
            foundTables.add(tableName);
            assert(tupleId >= 0);
        } // WHILE 
        this.finishTxn(voltProc);
        System.err.println("WRITE SET:\n" + VoltTableUtil.format(result));
        
        assertEquals(expectedTables, foundTables);
    }


    /**
     * testEnableTracking
     */
    @Test
    public void testEnableTracking() throws Exception {
        Long txnId = 12345l;
        this.ee.trackingEnable(txnId);
    }
    
    /**
     * testFinishTracking
     */
    @Test
    public void testFinishTracking() throws Exception {
        Long txnId = 12345l;
        this.ee.trackingEnable(txnId);
        this.ee.trackingFinish(txnId);
    }
    
}
