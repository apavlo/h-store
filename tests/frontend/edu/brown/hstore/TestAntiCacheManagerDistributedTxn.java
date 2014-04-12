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
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.TestHStoreCoordinator.AssertThreadGroup;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

public class TestAntiCacheManagerDistributedTxn extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 100000;
    private static final String TARGET_TABLE = YCSBConstants.TABLE_NAME;
    
    private static final String statsFields[] = {
        "ANTICACHE_TUPLES_EVICTED",
        "ANTICACHE_BLOCKS_EVICTED",
        "ANTICACHE_BYTES_EVICTED"
    };
    
    
    private MockHStoreSite hstore_sites[] = new MockHStoreSite[2];
    private HStoreCoordinator coordinators[] = new HStoreCoordinator[2];

    private HStoreConf hstore_conf;
    private File anticache_dir;
    private Client client;
    
    private PartitionExecutor executor;
    private ExecutionEngine ee;
    private Table catalog_tbl;
    private int locators[];

    private final AbstractProjectBuilder builder = new YCSBProjectBuilder() {
        {
            this.markTableEvictable(TARGET_TABLE);
            this.addAllDefaults();
            this.addStmtProcedure("GetRecord",
                                  "SELECT * FROM " + TARGET_TABLE + " WHERE ycsb_key = ?");
        }
    };
	
    
    @Before
    public void setUp() throws Exception {
    	super.setUp(builder, false);
        this.anticache_dir = FileUtil.getTempDirectory();
        
        // Just make sure that the Table has the evictable flag set to true
        this.catalog_tbl = getTable(TARGET_TABLE);
        assertTrue(catalog_tbl.getEvictable());
        this.locators = new int[] { catalog_tbl.getRelativeIndex() };

        // Create a fake cluster of two HStoreSites, each with two partitions
        // This will allow us to test same site communication as well as cross-site communication
        this.initializeCatalog(1, 2, NUM_PARTITIONS);
        for (int i = 0; i < 2; i++) {
            this.hstore_conf = HStoreConf.singleton();
            this.hstore_conf.site.status_enable = false;
            this.hstore_conf.site.anticache_enable = true;
            this.hstore_conf.site.anticache_profiling = true;
            this.hstore_conf.site.anticache_check_interval = Integer.MAX_VALUE;
            this.hstore_conf.site.anticache_dir = this.anticache_dir.getAbsolutePath();
            this.hstore_conf.site.coordinator_sync_time = false;

            this.hstore_sites[i] = new MockHStoreSite(i, catalogContext, hstore_conf);
            this.hstore_sites[i].setCoordinator();
            this.coordinators[i] = this.hstore_sites[i].getCoordinator();
            int p_id = CollectionUtil.first(this.hstore_sites[i].getLocalPartitionIds());
            MockPartitionExecutor es = new MockPartitionExecutor(p_id, catalogContext, p_estimator);
            this.hstore_sites[i].addPartitionExecutor(p_id, es);
            if(i == 0){
            	this.executor = es;
	            assertNotNull(this.executor);
	            this.ee = executor.getExecutionEngine();
	            assertNotNull(this.executor);
            }            
            
            
        } // FOR

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
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        for (int i = 0; i < NUM_TUPLES; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i;
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000l, catalog_tbl, vt, false);
        
        VoltTable stats[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, stats.length);
        System.err.println(VoltTableUtil.format(stats));
    }
    
    private VoltTable evictData() throws Exception {
        VoltTable results[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, results.length);
        // System.err.println(VoltTableUtil.format(results));
        for (String col : statsFields) {
			results[0].advanceRow(); 
            int idx = results[0].getColumnIndex(col);
            assertEquals(0, results[0].getLong(idx));    
        } // FOR

        // Now force the EE to evict our boys out
        // We'll tell it to remove 1MB, which is guaranteed to include all of our tuples
        VoltTable evictResult = this.ee.antiCacheEvictBlock(catalog_tbl, 1024 * 500, 1);

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


    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------

    @Test
    public void testQueueingOfTransaction() throws Exception {
    	AntiCacheManager manager = hstore_sites[0].getAntiCacheManager();
        short block_ids[] = new short[]{ 1111 };
        int tuple_offsets[] = new int[]{0};
        int partition_id = 0;
        this.hstore_conf.site.anticache_profiling = false;
        LocalTransaction txn = MockHStoreSite.makeLocalTransaction(hstore_sites[0]);

        assertTrue(manager.queue(txn, partition_id, catalog_tbl, block_ids, tuple_offsets));

    }
  
    @Test
    public void testQueueingOfDistributedTransaction() throws Exception {
    	AntiCacheManager manager = hstore_sites[0].getAntiCacheManager();
        short block_ids[] = new short[]{ 1111 };
        int tuple_offsets[] = new int[]{0};
         // different from the base partition. This means the exception was 
        // thrown by a remote site
        this.hstore_conf.site.anticache_profiling = false;
        LocalTransaction txn = MockHStoreSite.makeLocalTransaction(hstore_sites[0]);
        int partition_id = CollectionUtil.first(this.hstore_sites[1].getLocalPartitionIds());

        assertTrue(manager.queue(txn, partition_id, catalog_tbl, block_ids, tuple_offsets));

    }

}
