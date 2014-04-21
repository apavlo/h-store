package edu.brown.hstore;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ThreadUtil;

public class TestAntiCacheDistributedTxnEndToEnd extends BaseTestCase{
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 100000;
    private static final String TARGET_TABLE = YCSBConstants.TABLE_NAME;
    
    private static final String statsFields[] = {
        "ANTICACHE_TUPLES_EVICTED",
        "ANTICACHE_BLOCKS_EVICTED",
        "ANTICACHE_BYTES_EVICTED"
    };
    
    
    private HStoreSite hstore_sites[] = new HStoreSite[2];
    private HStoreCoordinator coordinators[] = new HStoreCoordinator[2];

    private HStoreConf hstore_conf;
    private File anticache_dir;
    private Client client;
    
    private PartitionExecutor [] executors = new PartitionExecutor[2];
    private ExecutionEngine [] ee = new ExecutionEngine[2];
    private Table catalog_tbl;
    private int locators[];

    private final AbstractProjectBuilder builder = new YCSBProjectBuilder() {
        {
            this.markTableEvictable(TARGET_TABLE);
            this.addAllDefaults();
            this.addStmtProcedure("GetRecords",
                                  "SELECT * FROM " + TARGET_TABLE); // needs all tuples
        }
    };
	
    
    @Before
    public void setUp() throws Exception {
    	super.setUp(builder, false);
        initializeCatalog(1, 2, NUM_PARTITIONS);
        this.anticache_dir = FileUtil.getTempDirectory();
        
        // Just make sure that the Table has the evictable flag set to true
        this.catalog_tbl = getTable(TARGET_TABLE);
        assertTrue(catalog_tbl.getEvictable());
        this.locators = new int[] { catalog_tbl.getRelativeIndex() };

        // Create a fake cluster of two HStoreSites, each with two partitions
        // This will allow us to test same site communication as well as cross-site communication
        for (int i = 0; i < 2; i++) {

	        this.hstore_conf = HStoreConf.singleton();
	        this.hstore_conf.site.status_enable = false;
	        this.hstore_conf.site.txn_partition_id_managers = true;
	        this.hstore_conf.site.anticache_enable = true;
	        this.hstore_conf.site.anticache_profiling = true;
	        this.hstore_conf.site.anticache_check_interval = Integer.MAX_VALUE;
	        this.hstore_conf.site.anticache_dir = this.anticache_dir.getAbsolutePath();
	        this.hstore_conf.site.coordinator_sync_time = false;
        
	        Site catalog_site = CollectionUtil.get(CatalogUtil.getCluster(catalog).getSites(), i);
	        hstore_sites[i] = createHStoreSite(catalog_site, hstore_conf);
            coordinators[i] = hstore_sites[i].getCoordinator();
            int p_id = CollectionUtil.first(hstore_sites[i].getLocalPartitionIds());
	        executors[i] = hstore_sites[i].getPartitionExecutor(p_id);
	        assertNotNull(executors[i]);
	        ee[i] = executors[i].getExecutionEngine();
	        assertNotNull(ee[i]);
        }
        
        this.client = createClient();

        this.startMessengers();
        
        
        System.err.println("All HStoreCoordinators started!");
    }
    
    @Override
    protected void tearDown() throws Exception {
        System.err.println("TEAR DOWN!");
        super.tearDown();
        this.stopMessengers();
        
        // Check to make sure all of the ports are free for each messenger
        for (HStoreCoordinator m : this.coordinators) {
            // assert(m.isStopped()) : "Site #" + m.getLocalSiteId() + " wasn't stopped";
            int port = m.getLocalMessengerPort();
            ServerSocketChannel channel = ServerSocketChannel.open();
            try {
                channel.socket().bind(new InetSocketAddress(port));
            } catch (IOException ex) {
                ex.printStackTrace();
                assert(false) : "Messenger port #" + port + " for Site #" + m.getLocalSiteId() + " isn't open: " + ex.getLocalizedMessage();
            } finally {
                channel.close();
                Thread.yield();
            }
        } // FOR
        FileUtil.deleteDirectory(this.anticache_dir);
    }
    /**
     * To keep track out how many threads fail
     */
    public class AssertThreadGroup extends ThreadGroup {
        private List<Throwable> exceptions = new ArrayList<Throwable>();
        
        public AssertThreadGroup() {
            super("Assert");
        }
        public void uncaughtException(Thread t, Throwable e) {
            e.printStackTrace();
            this.exceptions.add(e);
        }
    }
    
    /**
     * Start all of the HStoreMessengers
     * @throws Exception
     */
    private void startMessengers() throws Exception {
        // We have to fire of threads in parallel because HStoreMessenger.start() blocks!
        List<Thread> threads = new ArrayList<Thread>();
        AssertThreadGroup group = new AssertThreadGroup();
        for (final HStoreCoordinator m : this.coordinators) {
            threads.add(new Thread(group, "Site#" + m.getLocalSiteId()) {
                @Override
                public void run() {
                    System.err.println("START: " + m);
                    m.start();
                } 
            });
        } // FOR
        ThreadUtil.runNewPool(threads);
        if (group.exceptions.isEmpty() == false) stopMessengers(); 
        assert(group.exceptions.isEmpty()) : group.exceptions;
    }
    
    /**
     * Stop all of the HStoreMessengers
     * @throws Exception
     */
    private void stopMessengers() throws Exception {
        // Tell everyone to prepare to stop
        for (final HStoreCoordinator m : this.coordinators) {
            if (m.isStarted()) {
                System.err.println("PREPARE: " + m);
                m.prepareShutdown(false);
            }
        } // FOR
        // Now stop everyone for real!
        for (final HStoreCoordinator m : this.coordinators) {
            if (m.isShuttingDown()) {
                System.err.println("STOP: " + m);
                m.shutdown();
            }
        } // FOR
    }
    
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void loadData() throws Exception {
        // Load in a bunch of dummy data for this table
    	// site 1
        VoltTable vt1 = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt1);
        for (int i = 0; i < NUM_TUPLES/2; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i;
            vt1.addRow(row);
        } // FOR
        this.executors[0].loadTable(1000l, catalog_tbl, vt1, false);
        
        // site 2
        VoltTable vt2 = CatalogUtil.getVoltTable(catalog_tbl);
        for (int i = 0; i < NUM_TUPLES/2; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i;
            vt2.addRow(row);
        } // FOR
        this.executors[1].loadTable(1001l, catalog_tbl, vt2, false);
        
        VoltTable stats[] = this.ee[0].getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, stats.length);
        System.err.println(VoltTableUtil.format(stats));
        stats = this.ee[1].getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, stats.length);
        System.err.println(VoltTableUtil.format(stats));

    }
    
    private VoltTable evictData() throws Exception {
    	// evict only from remote site i.e site 1
        VoltTable results[] = this.ee[1].getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, results.length);
        // System.err.println(VoltTableUtil.format(results));
        for (String col : statsFields) {
			results[0].advanceRow(); 
            int idx = results[0].getColumnIndex(col);
            assertEquals(0, results[0].getLong(idx));    
        } // FOR

        // Now force the EE to evict our boys out
        // We'll tell it to remove 1MB, which is guaranteed to include all of our tuples
        VoltTable evictResult = this.ee[1].antiCacheEvictBlock(catalog_tbl, 1024 * 500, 1);

        System.err.println("-------------------------------");
        System.err.println(VoltTableUtil.format(evictResult));
        assertNotNull(evictResult);
        assertEquals(1, evictResult.getRowCount());
        //assertNotSame(results[0].getColumnCount(), evictResult.getColumnCount());
        evictResult.resetRowPosition();
        boolean adv = evictResult.advanceRow();
        assertTrue(adv);
          return (evictResult);
    }
	
//    @Test
//    public void testEndToEndDistributedTxn() throws Exception{
//    	this.loadData();
//    	this.evictData();
//    	
//        Procedure proc = this.getProcedure("GetRecords"); // Special Single-Stmt Proc
//        ClientResponse cresponse = this.client.callProcedure(proc.getName());
//        assertEquals(Status.OK, cresponse.getStatus());
//        
//        VoltTable results[] = cresponse.getResults();
//        assertEquals(1, results.length);
//    }
}
