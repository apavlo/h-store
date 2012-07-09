package edu.brown.hstore;

import java.io.File;
import java.util.concurrent.Semaphore;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.UnknownBlockAccessException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.voter.VoterConstants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestAntiCacheManager extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 100;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Thread thread;
    private File anticache_dir;
    private Semaphore readyLock;
    
    private PartitionExecutor executor;
    private ExecutionEngine ee;

    private EventObserver<HStoreSite> ready = new EventObserver<HStoreSite>() {
        @Override
        public void update(EventObservable<HStoreSite> o, HStoreSite arg) {
            readyLock.release();
        }
    };
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.VOTER);
        initializeCluster(1, 1, NUM_PARTITIONS);
        this.anticache_dir = FileUtil.getTempDirectory();
        this.readyLock = new Semaphore(0);
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.anticache_enable = true;
        this.hstore_conf.site.anticache_dir = this.anticache_dir.getAbsolutePath();
        
        this.hstore_site = HStore.initialize(catalog_site, hstore_conf);
        this.hstore_site.getReadyObservable().addObserver(this.ready);
        this.thread = new Thread(this.hstore_site);
        this.thread.start();
        
        // Wait until we know that our HStoreSite has started
        this.readyLock.acquire();
        
        this.executor = hstore_site.getPartitionExecutor(0);
        assertNotNull(this.executor);
        this.ee = executor.getExecutionEngine();
        assertNotNull(this.executor);
    }
    
    @Override
    protected void tearDown() throws Exception {
        this.hstore_site.shutdown();
        FileUtil.deleteDirectory(this.anticache_dir);
    }
    
    /**
     * testEvictTuples
     */
    @Test
    public void testEvictTuples() throws Exception {
        // Load in a bunch of dummy data for this table
        Table catalog_tbl = getTable(VoterConstants.TABLENAME_VOTES);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        for (int i = 0; i < NUM_TUPLES; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000l, catalog_tbl, vt, false);

        // Now force the EE to evict our boys out

    }

    /**
     * testReadNonExistentBlock
     */
    @Test
    public void testReadNonExistentBlock() throws Exception {
        Table catalog_tbl = getTable(VoterConstants.TABLENAME_VOTES);
        short block_ids[] = new short[]{ 1111 };
        boolean failed = false;
        try {
            ee.antiCacheReadBlocks(catalog_tbl, block_ids);   
        } catch (UnknownBlockAccessException ex) {
            // This is what we want!
            assertEquals(catalog_tbl, ex.getTableId(catalog_db));
            assertEquals(block_ids[0], ex.getBlockId());
            failed = true;
            System.err.println(ex);
        }
        assertTrue(failed);
    }
    
}
