package edu.brown.hstore;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.UnknownBlockAccessException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * Anti-Cache Manager Test Cases
 * @author pavlo
 * @author jdebrabant
 */
public class TestAntiCacheManager extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 10;
    private static final String TARGET_TABLE = YCSBConstants.TABLE_NAME;

    private String readBackTracker;
    
    private static final String statsFields[] = {
        "ANTICACHE_TUPLES_EVICTED",
        "ANTICACHE_BLOCKS_EVICTED",
        "ANTICACHE_BYTES_EVICTED"
    };
    
    private HStoreSite hstore_site;
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
        initializeCatalog(1, 1, NUM_PARTITIONS);
        this.anticache_dir = FileUtil.getTempDirectory();
        
        // Just make sure that the Table has the evictable flag set to true
        this.catalog_tbl = getTable(TARGET_TABLE);
        assertTrue(catalog_tbl.getEvictable());
        this.locators = new int[] { catalog_tbl.getRelativeIndex() };
        
        Site catalog_site = CollectionUtil.first(getCatalogContext().sites);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.anticache_enable = true;
        this.hstore_conf.site.anticache_profiling = true;
        this.hstore_conf.site.anticache_check_interval = Integer.MAX_VALUE;
        this.hstore_conf.site.anticache_dir = this.anticache_dir.getAbsolutePath();
        this.hstore_conf.site.anticache_dbtype = "BERKELEY";
        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
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
        FileUtil.deleteDirectory(this.anticache_dir);
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
            if (i == 1) readBackTracker = row[1].toString();
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
        VoltTable evictResult = this.ee.antiCacheEvictBlock(catalog_tbl, 1024 * 256, 1);

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
    public void testStats() throws Exception {
        boolean adv;
        this.loadData();
        
        String writtenFields[] = new String[statsFields.length];
        String readFields[] = new String[statsFields.length];
        for (int i = 0; i < statsFields.length; i++) {
            writtenFields[i] = statsFields[i].replace("EVICTED", "WRITTEN");
            readFields[i] = statsFields[i].replace("EVICTED", "READ");
        } // FOR
        
        // Evict some data
        VoltTable evictResult = this.evictData();
        
        // Check to make sure that our stats say that something was evicted
        VoltTable origStats[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, origStats.length);
        System.err.println(VoltTableUtil.format(origStats));
        adv = origStats[0].advanceRow();
        assert(adv);
        for (int i = 0; i < statsFields.length; i++) {
            // ACTIVE
            assertTrue(statsFields[i], evictResult.getLong(statsFields[i]) > 0);
            assertEquals(statsFields[i], evictResult.getLong(statsFields[i]), origStats[0].getLong(statsFields[i]));
            
            // GLOBAL WRITTEN
            assertEquals(writtenFields[i], evictResult.getLong(statsFields[i]), origStats[0].getLong(writtenFields[i]));
            
            // GLOBAL READ
            assertEquals(readFields[i], 0, origStats[0].getLong(readFields[i]));
        } // FOR
        
        // TODO: Check that the string data has all been evicted.
        
        // Now execute a query that needs to access data from this block
        long expected = 1;
        Procedure proc = this.getProcedure("GetRecord");
        ClientResponse cresponse = this.client.callProcedure(proc.getName(), expected);
        assertEquals(Status.OK, cresponse.getStatus());
        
        // Check to make sure that our stats were updated
        VoltTable newStats[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, newStats.length);
        System.err.println(VoltTableUtil.format(newStats));
        adv = newStats[0].advanceRow();
        assert(adv);
        for (int i = 0; i < statsFields.length; i++) {
            // ACTIVE
            assertEquals(statsFields[i], 0, newStats[0].getLong(statsFields[i]));
            
            // GLOBAL WRITTEN
            assertEquals(writtenFields[i], origStats[0].getLong(writtenFields[i]), newStats[0].getLong(writtenFields[i]));
            
            // GLOBAL READ
            assertEquals(readFields[i], origStats[0].getLong(writtenFields[i]), newStats[0].getLong(readFields[i]));
        } // FOR
        
        // Check that the global stats for the site matches too
        this.executor.getDebugContext().updateMemory();
        proc = this.getProcedure(VoltSystemProcedure.procCallName(Statistics.class));
        Object params[] = { SysProcSelector.MEMORY.name(), 0 };
        cresponse = this.client.callProcedure(proc.getName(), params);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results = cresponse.getResults()[0];
        adv = results.advanceRow();
        assert(adv);
        for (int i = 0; i < statsFields.length; i++) {
            // XXX: Skip the byte counters since it will be kilobytes
            if (statsFields[i].contains("BYTES")) continue;
            
            // ACTIVE
            assertEquals(statsFields[i], newStats[0].getLong(statsFields[i]), results.getLong(statsFields[i]));
            
            // GLOBAL WRITTEN
            assertEquals(writtenFields[i], newStats[0].getLong(writtenFields[i]), results.getLong(writtenFields[i]));
            
            // GLOBAL READ
            assertEquals(readFields[i], newStats[0].getLong(readFields[i]), results.getLong(readFields[i]));
        } // FOR
        
    }

    @Test
    public void testReadEvictedTuples() throws Exception {
        this.loadData();
        
        // We should have all of our tuples evicted
        VoltTable evictResult = this.evictData();
        long evicted = evictResult.getLong("ANTICACHE_TUPLES_EVICTED");
        assertTrue("No tuples were evicted!"+evictResult, evicted > 0);
        
        // Now execute a query that needs to access data from this block
        long expected = 1;
        Procedure proc = this.getProcedure("GetRecord"); // Special Single-Stmt Proc
        ClientResponse cresponse = this.client.callProcedure(proc.getName(), expected);
        assertEquals(Status.OK, cresponse.getStatus());
        
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        boolean adv = results[0].advanceRow();
        assertTrue(adv);
        assertEquals(expected, results[0].getLong(0));

        // try to read a content back
        assertEquals(readBackTracker, results[0].getString(1));
        
        AntiCacheManagerProfiler profiler = hstore_site.getAntiCacheManager().getDebugContext().getProfiler(0);
        assertNotNull(profiler);
        assertEquals(1, profiler.evictedaccess_history.size());

        evicted = evictResult.getLong("ANTICACHE_TUPLES_EVICTED");
        assertTrue("No tuples were evicted!"+evictResult, evicted > 0);
    }
        
    @Test
    public void testMultipleReadEvictedTuples() throws Exception {
        this.loadData();
        
        // We should have all of our tuples evicted
        VoltTable evictResult = this.evictData();
        long evicted = evictResult.getLong("ANTICACHE_TUPLES_EVICTED");
        assertTrue("No tuples were evicted!"+evictResult, evicted > 0);
        
        // Execute multiple queries that try to access tuples the same block
        // Only one should get an evicted access exception
        Procedure proc = this.getProcedure("GetRecord"); // Special Single-Stmt Proc
        for (int i = 1; i < 10; i++) {
            long expected = i;
            ClientResponse cresponse = this.client.callProcedure(proc.getName(), expected);
            assertEquals(Status.OK, cresponse.getStatus());
            
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            boolean adv = results[0].advanceRow();
            assertTrue(adv);
            assertEquals(expected, results[0].getLong(0));
        } // FOR
        
        AntiCacheManagerProfiler profiler = hstore_site.getAntiCacheManager().getDebugContext().getProfiler(0);
        assertNotNull(profiler);
        assertEquals(1, profiler.evictedaccess_history.size());
    }
    
    @Test
    public void testEvictTuples() throws Exception {
        this.loadData();
        VoltTable evictResult = this.evictData();
		evictResult.advanceRow(); 

        // Our stats should now come back with at least one block evicted
        VoltTable results[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, results.length);
        System.err.println("-------------------------------");
        System.err.println(VoltTableUtil.format(results));

		results[0].advanceRow(); 
        for (String col : statsFields) {
            assertEquals(col, evictResult.getLong(col), results[0].getLong(col));
            if (col == "BLOCKS_EVICTED") {
                assertEquals(col, 1, results[0].getLong(col));
            } else {
                assertNotSame(col, 0, results[0].getLong(col));
            }
        } // FOR
    }

    @Test
    public void testEvictTuplesMultiple() throws Exception {
        // Just checks whether we can call evictBlock multiple times
        this.loadData();

        VoltTable results[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, results.length);
        System.err.println(VoltTableUtil.format(results));

		results[0].advanceRow(); 
        for (String col : statsFields) {
            int idx = results[0].getColumnIndex(col);
            assertEquals(0, results[0].getLong(idx));    
        } // FOR
        System.err.println(StringUtil.repeat("=", 100));
        
        // Now force the EE to evict our boys out in multiple rounds
        VoltTable evictResult = null;
        for (int i = 0; i < 5; i++) {
            if (i > 0) {
                System.err.println(StringUtil.repeat("-", 100));
                ThreadUtil.sleep(1000);
            }
            System.err.println("Eviction #" + i);
            evictResult = this.ee.antiCacheEvictBlock(catalog_tbl, 512, 1);
            System.err.println(VoltTableUtil.format(evictResult));
            assertNotNull(evictResult);
            assertEquals(1, evictResult.getRowCount());
            assertNotSame(results[0].getColumnCount(), evictResult.getColumnCount());
            evictResult.resetRowPosition();
            boolean adv = evictResult.advanceRow();
            assertTrue(adv);
        } // FOR
    }

    @Test
    public void testReadNonExistentBlock() throws Exception {
        int block_ids[] = new int[]{ 1111 };
        int tuple_offsets[] = new int[]{0}; 
        boolean failed = false;
        try {
            ee.antiCacheReadBlocks(catalog_tbl, block_ids, tuple_offsets);
        } catch (UnknownBlockAccessException ex) {
            // This is what we want!
            assertEquals(block_ids[0], ex.getBlockId());
            failed = true;
            System.err.println(ex);
        }
        assertTrue(failed);
    }   
}
