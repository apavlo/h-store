package edu.brown.hstore;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;

/**
 * Anti-Cache Manager Test Cases for TPC-C
 * @author pavlo
 */
public class TestAntiCacheMultiTable extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 1000;
    private static final String TARGET_TABLES[] = {
        TPCCConstants.TABLENAME_ORDER_LINE,
        TPCCConstants.TABLENAME_HISTORY,  
    };
    
//    private static final String SEQSCAN_PROCEDURE = "GetRecordNoLimit";
//    private static final String SEQSCANLIMIT_PROCEDURE = "GetRecordWithLimit";
//    private static final String INDEXSCAN_PROCEDURE = "GetRecordWithIndex";
    
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
    private AntiCacheManagerProfiler profiler;
    private int locators[];

    private final AbstractProjectBuilder builder = new TPCCProjectBuilder() {
        {
            for (String table : TARGET_TABLES)
                this.markTableEvictable(table);
                
            this.addAllDefaults();
//            this.addStmtProcedure(SEQSCAN_PROCEDURE,
//                                  "SELECT * FROM " + TARGET_TABLE);
//            this.addStmtProcedure(SEQSCANLIMIT_PROCEDURE,
//                                  "SELECT * FROM " + TARGET_TABLE + " LIMIT ?");
//            this.addStmtProcedure(INDEXSCAN_PROCEDURE,
//                                  "SELECT * FROM " + TARGET_TABLE + 
//                                  " WHERE OL_O_ID >= ?" +
//                                  "   AND OL_D_ID >= ?" +
//                                  "   AND OL_W_ID >= ?" +
//                                  " LIMIT ?");
        }
    };
    
    @Before
    public void setUp() throws Exception {
        super.setUp(builder, false);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        this.anticache_dir = FileUtil.getTempDirectory();
        
        // Just make sure that the Table has the evictable flag set to true
        this.locators = new int[TARGET_TABLES.length];
        for (int i = 0; i < TARGET_TABLES.length; i++) {
            Table catalog_tbl = getTable(TARGET_TABLES[i]);
            assertTrue(catalog_tbl.getEvictable());
            this.locators[i] = catalog_tbl.getRelativeIndex(); 
        } // FOR
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.anticache_enable = true;
        this.hstore_conf.site.anticache_profiling = true;
        this.hstore_conf.site.anticache_check_interval = Integer.MAX_VALUE;
        this.hstore_conf.site.anticache_dir = this.anticache_dir.getAbsolutePath();
        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.executor = hstore_site.getPartitionExecutor(0);
        assertNotNull(this.executor);
        this.ee = executor.getExecutionEngine();
        assertNotNull(this.executor);
        this.profiler = hstore_site.getAntiCacheManager().getDebugContext().getProfiler(0);
        assertNotNull(profiler);
        
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
    
    private void loadData(Table catalog_tbl) throws Exception {
        // Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        for (int i = 0; i < NUM_TUPLES; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            
            if (catalog_tbl.getName().equalsIgnoreCase(TPCCConstants.TABLENAME_ORDER_LINE)) {
                row[0] = i; // OL_O_ID
                row[1] = (byte)i; // OL_D_ID
                row[2] = (short)i; // OL_W_ID
            }
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000l, catalog_tbl, vt, false);
        
        int statsLocators[] = { catalog_tbl.getRelativeIndex() };
        VoltTable stats[] = this.ee.getStats(SysProcSelector.TABLE, statsLocators, false, 0L);
        assertEquals(1, stats.length);
        // System.err.println(VoltTableUtil.format(stats));
    }
    
    private VoltTable evictData(Table catalog_tbl) throws Exception {
        VoltTable results[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, results.length);
        // System.err.println(VoltTableUtil.format(results));
        while (results[0].advanceRow()) {
            if (results[0].getString("TABLE_NAME").equalsIgnoreCase(catalog_tbl.getName()) == false)
                continue;
            for (String col : statsFields) {
                int idx = results[0].getColumnIndex(col);
                assertEquals(0, results[0].getLong(idx));    
            } // FOR
        } // WHILE
        
        // Now force the EE to evict our boys out
        // We'll tell it to remove 1MB, which is guaranteed to include all of our tuples
        VoltTable evictResult = this.ee.antiCacheEvictBlock(catalog_tbl, 1024 * 256, 1);

//        System.err.println("-------------------------------");
//        System.err.println(VoltTableUtil.format(evictResult));
        assertNotNull(evictResult);
        assertEquals(1, evictResult.getRowCount());
        assertNotSame(results[0].getColumnCount(), evictResult.getColumnCount());
        evictResult.resetRowPosition();
        boolean adv = evictResult.advanceRow();
        assertTrue(adv);
        return (evictResult);
    }
 
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
 
    /**
     * testEvictTuples
     */
    @Test
    public void testEvictTuples() throws Exception {
        // First load some stuff into the database
        for (String tableName : TARGET_TABLES) {
            this.loadData(this.getTable(tableName));
        }
        
        // Then make sure that we can evict from each of them
        for (String tableName : TARGET_TABLES) {
            Table catalog_tbl = this.getTable(tableName);
            VoltTable evictResult = this.evictData(catalog_tbl);
            evictResult.advanceRow();

            // Our stats should now come back with at least one block evicted
            VoltTable results[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
            assertEquals(1, results.length);
            // System.err.println("-------------------------------");
            // System.err.println(VoltTableUtil.format(results));

            while (results[0].advanceRow()) {
                if (results[0].getString("TABLE_NAME").equalsIgnoreCase(catalog_tbl.getName()) == false)
                    continue;
                for (String col : statsFields) {
                    assertEquals(col, evictResult.getLong(col), results[0].getLong(col));
                    if (col == "ANTICACHE_BLOCKS_EVICTED") {
                        assertEquals(col, 1, results[0].getLong(col));
                    } else {
                        assertNotSame(col, 0, results[0].getLong(col));
                    }
                } // FOR
            } // WHILE
        } // FOR
    }
    

}
