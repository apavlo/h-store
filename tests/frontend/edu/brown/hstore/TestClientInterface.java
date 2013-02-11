package edu.brown.hstore;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.sysprocs.Sleep;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

public class TestClientInterface extends BaseTestCase {
    
    private static final int NUM_TXNS = 1000;
    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_ROWS = 10000;
    private static final int WAIT_TIME = 2500;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private ClientInterface clientInterface;
    private Client client;

    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        initializeCatalog(1, 1, NUM_PARTITIONS);
     
        for (TransactionCounter tc : TransactionCounter.values()) {
            tc.clear();
        } // FOR
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.profiling = true;
        this.hstore_conf.site.pool_profiling = true;
        this.hstore_conf.site.network_incoming_limit_txns = 1;
        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.clientInterface = this.hstore_site.getClientInterface();
        this.client = createClient();
        this.client.configureBlocking(true);
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    /**
     * testBackPressure
     */
    @Test
    public void testBackPressure() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING);
        VoltTable data[] = { CatalogUtil.getVoltTable(catalog_tbl) };
        for (int i = 0; i < NUM_ROWS; i++) {
            data[0].addRow(VoltTableUtil.getRandomRow(catalog_tbl));
        } // FOR
         
        final AtomicBoolean onBackPressure = new AtomicBoolean(false);
        EventObserver<HStoreSite> onBackPressureObserver = new EventObserver<HStoreSite>() {
            @Override
            public void update(EventObservable<HStoreSite> o, HStoreSite ts) {
                System.err.println("On back pressure");
                onBackPressure.set(true);
            }
        };
        this.clientInterface.getOnBackPressureObservable().addObserver(onBackPressureObserver);
        
        final AtomicBoolean offBackPressure = new AtomicBoolean(false);
        EventObserver<HStoreSite> offBackPressureObserver = new EventObserver<HStoreSite>() {
            @Override
            public void update(EventObservable<HStoreSite> o, HStoreSite ts) {
                System.err.println("Off back pressure");
                offBackPressure.set(true);
            }
        };
        this.clientInterface.getOffBackPressureObservable().addObserver(offBackPressureObserver);

        // Submit a bunch of txns that will block and check to make sure that
        // can go on and off on the backpressure status properly
        LatchableProcedureCallback callback = new LatchableProcedureCallback(1);
        String procName = VoltSystemProcedure.procCallName(Sleep.class);
        Object params[] = { WAIT_TIME, data };
        client.callProcedure(callback, procName, params);
        ThreadUtil.sleep(WAIT_TIME);
        
        LatchableProcedureCallback floodCallback = new LatchableProcedureCallback(NUM_TXNS);
        for (int i = 0; i < NUM_TXNS; i++) {
            client.callProcedure(floodCallback, procName, new Object[]{0, data});
        } // FOR
        
        boolean result = callback.latch.await(WAIT_TIME*3, TimeUnit.MILLISECONDS);
        assertTrue(result);
        assertTrue("onBackPressure", onBackPressure.get());
        
        // client.backpressureBarrier();
        // ThreadUtil.sleep(WAIT_TIME);
        
        // result = floodCallback.latch.await(WAIT_TIME*5, TimeUnit.MILLISECONDS);
        // assertTrue("floodCallback->"+floodCallback.latch, result);
        
        // HStoreSiteTestUtil.checkObjectPools(hstore_site);
    }

}
