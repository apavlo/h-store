package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.voltdb.client.ProcedureCallback;
import org.voltdb.regressionsuites.specexecprocs.DtxnTester;
import org.voltdb.sysprocs.ExecutorStatus;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
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
import edu.brown.pools.TypedObjectPool;
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ThreadUtil;

public class TestPartitionExecutorWorkloadShedder extends BaseTestCase {
    
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = GetNewDestination.class;
    private static final long CLIENT_HANDLE = 1l;
    private static final int NUM_PARTITIONS = 3;
    private static final int NUM_TUPLES = 10;
    private static final int NUM_TXNS = 200;
    private static final int BASE_PARTITION = 0;
    private static final int NOTIFY_TIMEOUT = 2500; // ms
    
    private HStoreSite hstore_site;
    private HStoreSite.Debug hstore_debug;
    private HStoreObjectPools objectPools;
    private HStoreConf hstore_conf;
    private TransactionQueueManager.Debug queue_debug;
    private Client client;
    private PartitionExecutorWorkloadShedder workloadShedder;

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
        this.hstore_conf.site.pool_txn_enable = true;
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.status_interval = 4000;
        this.hstore_conf.site.anticache_enable = false;
        this.hstore_conf.site.txn_incoming_delay = 5;
        this.hstore_conf.site.exec_voltdb_procinfo = false;
        this.hstore_conf.site.exec_force_singlepartitioned = true;
        this.hstore_conf.site.queue_shedder_delay = 999999;
        this.hstore_conf.site.queue_shedder_interval = 999999;
        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.objectPools = this.hstore_site.getObjectPools();
        this.hstore_debug = this.hstore_site.getDebugContext();
        this.queue_debug = this.hstore_site.getTransactionQueueManager().getDebugContext();
        this.workloadShedder = this.hstore_site.getWorkloadShedder();
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
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testWorkloadShedding
     */
    @Test
    public void testSinglePartitionTxn() throws Exception {
        LatchableProcedureCallback callback = new LatchableProcedureCallback(NUM_TXNS + 1);
        
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
        
        // Send a bunch of single-partition txns that will get queued up at a single partition.
        catalog_proc = this.getProcedure(GetSubscriberData.class);
        for (int i = 0; i < NUM_TXNS; i++) {
            this.client.callProcedure(callback, catalog_proc.getName(), params);
        } // FOR
        ThreadUtil.sleep(NOTIFY_TIMEOUT);
        
        // Now invoke the workload shedder directly
        this.workloadShedder.run();
        
        HStoreSiteTestUtil.checkObjectPools(hstore_site);
        this.statusSnapshot();
    }
    


}
