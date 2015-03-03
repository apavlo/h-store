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
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.articles.ArticlesConstants;
import edu.brown.benchmark.articles.ArticlesProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;

/**
 * Anti-Cache Manager Test Cases
 * @author amaiti
 */
public class TestAntiCacheBatching extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 10;
    private static final String TARGET_TABLE = ArticlesConstants.TABLENAME_ARTICLES;
    private static final String CHILD_TABLE = ArticlesConstants.TABLENAME_COMMENTS;
    
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
    private Table child_tbl;
    private int locators[];

    private final ArticlesProjectBuilder builder = new ArticlesProjectBuilder() {
        {
            this.markTableEvictable(TARGET_TABLE);
            this.markTableEvictable(CHILD_TABLE);
            this.markTableBatchEvictable(CHILD_TABLE);
            
            this.addAllDefaults();
            this.addStmtProcedure("GetRecord",
                                  "SELECT * FROM " + TARGET_TABLE + " WHERE a_id = ?");
            this.addStmtProcedure("GetComments",
                    "SELECT * FROM " + CHILD_TABLE + " WHERE c_a_id = ?");
            
        }
    };
    
    @Before
    public void setUp() throws Exception {
        super.setUp(builder, false);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        this.anticache_dir = FileUtil.getTempDirectory();
        
        // Just make sure that the Table has the evictable flag set to true
        this.catalog_tbl = getTable(TARGET_TABLE);
        this.child_tbl = getTable(CHILD_TABLE);
        
        assertTrue(catalog_tbl.getEvictable());
        assertTrue(child_tbl.getEvictable());
        assertTrue(child_tbl.getBatchevicted());
        this.locators = new int[] { catalog_tbl.getRelativeIndex(), child_tbl.getRelativeIndex() };
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.anticache_enable = true;
        this.hstore_conf.site.anticache_profiling = true;
        this.hstore_conf.site.anticache_check_interval = Integer.MAX_VALUE;
        this.hstore_conf.site.anticache_dir = this.anticache_dir.getAbsolutePath();
        
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
    
    private void loadDataInBatch() throws Exception {
        // Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        VoltTable childTable = CatalogUtil.getVoltTable(child_tbl);
        assertNotNull(vt);
        assertNotNull(childTable);
        for (int i = 0; i < NUM_TUPLES; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i;
            vt.addRow(row);
            Object childRow[] = VoltTableUtil.getRandomRow(child_tbl); // 1 comment per article
            childRow[0] = 0; // c_id
            childRow[1] = i; // c_a_id
            childTable.addRow(childRow);
            childRow = VoltTableUtil.getRandomRow(child_tbl); // 1 comment per article
            childRow[0] = 1; // c_id
            childRow[1] = i; // c_a_id
            childTable.addRow(childRow);

        } // FOR
        this.executor.loadTable(1000l, catalog_tbl, vt, false);
        this.executor.loadTable(1000l, child_tbl, childTable, false);
        
        VoltTable stats[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, stats.length);

        assertNotNull(stats[0]);
        final VoltTable resultTable = stats[0];
        assertEquals(2, resultTable.getRowCount());
        System.err.println(VoltTableUtil.format(stats));
    }
    
    private VoltTable evictDataInBatch() throws Exception {
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
        VoltTable evictResult = this.ee.antiCacheEvictBlockInBatch(catalog_tbl, child_tbl, 1024 * 250, 1);

        System.err.println("-------------------------------");
        System.err.println(VoltTableUtil.format(evictResult));
        assertNotNull(evictResult);
        assertEquals(2, evictResult.getRowCount());
        //assertNotSame(results[0].getColumnCount(), evictResult.getColumnCount());
        evictResult.resetRowPosition();
        boolean adv = evictResult.advanceRow();
        assertTrue(adv);
          return (evictResult);
    }

    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testEvictTuplesInBatch
     */
    @Test
    public void testEvictTuplesInBatch() throws Exception {
        this.loadDataInBatch();
        VoltTable evictResult = this.evictDataInBatch();
//		evictResult.advanceRow(); 

        // Our stats should now come back with at least one block evicted
        VoltTable results[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, results.length);
        assertNotNull(results[0]);
        final VoltTable resultTable = results[0];
        assertEquals(2, resultTable.getRowCount());
        resultTable.resetRowPosition();
        System.err.println("-------------------------------");
        System.err.println(VoltTableUtil.format(results));

        resultTable.advanceRow(); 
        for (String col : statsFields) {
            assertEquals(col, evictResult.getLong(col), resultTable.getLong(col));
            if (col == "BLOCKS_EVICTED") {
                assertEquals(col, 1, resultTable.getLong(col));
            } else {
                assertNotSame(col, 0, resultTable.getLong(col));
            }
        } // FOR
    }

    /**
     * testReadEvictedTuples
     */
    @Test
    public void testReadEvictedTuples() throws Exception {
        this.loadDataInBatch();
        
        // We should have all of our tuples evicted
        VoltTable evictResult = this.evictDataInBatch();
        
        // Now execute a query that needs to access data from this block
        // the article fetch will also cause comments to be fetched
        long expected = 1;
        Procedure proc = this.getProcedure("GetRecord"); // Special Single-Stmt Proc
        ClientResponse cresponse = this.client.callProcedure(proc.getName(), expected);
        assertEquals(Status.OK, cresponse.getStatus());
        
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        boolean adv = results[0].advanceRow();
        assertTrue(adv);
        System.out.println(results[0]);
        assertEquals(expected, results[0].getLong(0));
        
        AntiCacheManagerProfiler profiler = hstore_site.getAntiCacheManager().getDebugContext().getProfiler(0);
        assertNotNull(profiler);
        assertEquals(1, profiler.evictedaccess_history.size());
        
        // clear out the evicted access history
        profiler.evictedaccess_history.clear();
        
        cresponse = this.client.callProcedure(proc.getName(), expected);
        assertEquals(Status.OK, cresponse.getStatus());
        
        results = cresponse.getResults();
        assertEquals(1, results.length);
        adv = results[0].advanceRow();
        assertTrue(adv);
        System.out.println(results[0]);
        assertEquals(expected, results[0].getLong(0));
        
        // Now execute a query to get comments
        // there should not be any anticachemanager activity
        
        Procedure childProc = this.getProcedure("GetComments"); // Special Single-Stmt Proc
        ClientResponse childResponse = this.client.callProcedure(childProc.getName(), expected);
        assertEquals(Status.OK, childResponse.getStatus());
        
        VoltTable childResults[] = childResponse.getResults();
        assertEquals(1, childResults.length);
        adv = childResults[0].advanceRow();
        assertTrue(adv);
        System.out.println(childResults[0]);
        // here we should check if we got all the comments for the article
        // and compare the article id
        assertEquals(expected, childResults[0].getLong(1));
        
        
        // there should not be any anticache manager activity
        assertEquals(0, profiler.evictedaccess_history.size());

    }

    /**
     * testReadChildTuples
     */
    @Test
    public void testReadChildTuples() throws Exception {
        this.loadDataInBatch();
        
        // We should have all of our tuples evicted
        VoltTable evictResult = this.evictDataInBatch();
        
        // Now execute a query that needs to access child tuples from this block
        // which means existing functionality still works
        long expected = 1;
        Procedure proc = this.getProcedure("GetComments"); // Special Single-Stmt Proc
        ClientResponse cresponse = this.client.callProcedure(proc.getName(), expected);
        assertEquals(Status.OK, cresponse.getStatus());
        
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        boolean adv = results[0].advanceRow();
        assertTrue(adv);
        System.out.println(results[0]);
        // here we should check if we got all the comments for the article
        // and compare the article id
        assertEquals(expected, results[0].getLong(1));
        
        AntiCacheManagerProfiler profiler = hstore_site.getAntiCacheManager().getDebugContext().getProfiler(0);
        assertNotNull(profiler);
        assertEquals(1, profiler.evictedaccess_history.size());
        
    }
    // TODO
    // 
    // Test maybe to see that child is not tracked?
    /**
     * testStats
     */
    @Test
    public void testStats() throws Exception {
        boolean adv;
        this.loadDataInBatch();
        
        String writtenFields[] = new String[statsFields.length];
        String readFields[] = new String[statsFields.length];
        for (int i = 0; i < statsFields.length; i++) {
            writtenFields[i] = statsFields[i].replace("EVICTED", "WRITTEN");
            readFields[i] = statsFields[i].replace("EVICTED", "READ");
        } // FOR
        
        // Evict some data
        VoltTable evictResult = this.evictDataInBatch();
        
        // Check to make sure that our stats say that something was evicted
        VoltTable origStats[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, origStats.length);
        System.err.println(VoltTableUtil.format(origStats));
        for(int j = 0 ; j < 2; j++){
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
        	evictResult.advanceRow();
        }
                
        // Now execute a query that needs to access data from this block
        long expected = 1;
        Procedure proc = this.getProcedure("GetRecord");
        ClientResponse cresponse = this.client.callProcedure(proc.getName(), expected);
        assertEquals(Status.OK, cresponse.getStatus());
        
        // Check to make sure that our stats were updated
        VoltTable newStats[] = this.ee.getStats(SysProcSelector.TABLE, this.locators, false, 0L);
        assertEquals(1, newStats.length);
        System.err.println(VoltTableUtil.format(newStats));
        long[] cumulativeStats = new long[statsFields.length];
        long[] cumulativeReads = new long[readFields.length];
        long[] cumulativeWrites = new long[writtenFields.length];        
        origStats[0].resetRowPosition();
        for( int j = 0 ; j < 2; j++){
            adv = newStats[0].advanceRow();
            assert(adv);
            adv = origStats[0].advanceRow();
            assert(adv);
            
            for (int i = 0; i < statsFields.length; i++) {
                // ACTIVE
                assertEquals(statsFields[i], 0, newStats[0].getLong(statsFields[i]));
                
                cumulativeStats[i] += newStats[0].getLong(statsFields[i]);
                
                // GLOBAL WRITTEN
//                assertEquals(writtenFields[i], origStats[0].getLong(readFields[i]), newStats[0].getLong(writtenFields[i]));
                cumulativeWrites[i] += newStats[0].getLong(writtenFields[i]);
                
                // GLOBAL READ
                assertEquals(readFields[i], origStats[0].getLong(writtenFields[i]), newStats[0].getLong(readFields[i]));
                cumulativeReads[i] += newStats[0].getLong(readFields[i]);
            } // FOR        	
        }
        
        
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
            assertEquals(statsFields[i], cumulativeStats[i], results.getLong(statsFields[i]));
            
            // GLOBAL WRITTEN
            assertEquals(writtenFields[i], cumulativeWrites[i], results.getLong(writtenFields[i]));
            
            // GLOBAL READ
            assertEquals(readFields[i], cumulativeReads[i], results.getLong(readFields[i]));
        } // FOR
        
    }


}
