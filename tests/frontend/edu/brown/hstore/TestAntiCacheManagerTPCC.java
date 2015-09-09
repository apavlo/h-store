package edu.brown.hstore;

import java.io.File;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.UnknownBlockAccessException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * Anti-Cache Manager Test Cases for TPC-C
 * @author pavlo
 */
public class TestAntiCacheManagerTPCC extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 1000;
    private static final String TARGET_TABLE = TPCCConstants.TABLENAME_ORDER_LINE;
    
    private static final String SEQSCAN_PROCEDURE = "GetRecordNoLimit";
    private static final String SEQSCANLIMIT_PROCEDURE = "GetRecordWithLimit";
    private static final String INDEXSCAN_PROCEDURE = "GetRecordWithIndex";
    
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
    private Table catalog_tbl;
    private int locators[];

    private final AbstractProjectBuilder builder = new TPCCProjectBuilder() {
        {
            this.markTableEvictable(TARGET_TABLE);
            this.addAllDefaults();
            this.addStmtProcedure(SEQSCAN_PROCEDURE,
                                  "SELECT * FROM " + TARGET_TABLE);
            this.addStmtProcedure(SEQSCANLIMIT_PROCEDURE,
                                  "SELECT * FROM " + TARGET_TABLE + " LIMIT ?");
            this.addStmtProcedure(INDEXSCAN_PROCEDURE,
                                  "SELECT * FROM " + TARGET_TABLE + 
                                  " WHERE OL_O_ID >= ?" +
                                  "   AND OL_D_ID >= ?" +
                                  "   AND OL_W_ID >= ?" +
                                  " LIMIT ?");
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
    
    private void loadData() throws Exception {
        // Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        for (int i = 0; i < NUM_TUPLES; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i; // OL_O_ID
            row[1] = (byte)i; // OL_D_ID
            row[2] = (short)i; // OL_W_ID
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
        VoltTable evictResult = this.ee.antiCacheEvictBlock(catalog_tbl, 1024 * 256, 1);

        System.err.println("-------------------------------");
        System.err.println(VoltTableUtil.format(evictResult));
        assertNotNull(evictResult);
        assertEquals(1, evictResult.getRowCount());
        assertNotSame(results[0].getColumnCount(), evictResult.getColumnCount());
        evictResult.resetRowPosition();
        boolean adv = evictResult.advanceRow();
        assertTrue(adv);
        return (evictResult);
    }
    
    private void simpleScan(String  procName, int expected, boolean useLimit) throws Exception {
        assert(expected <= NUM_TUPLES);
        this.loadData();
        
        // We should have all of our tuples evicted
        VoltTable evictResult = this.evictData();
        long evicted = evictResult.getLong("ANTICACHE_TUPLES_EVICTED");
        assertTrue("No tuples were evicted!\n"+evictResult, evicted > 0);
        // System.err.println(VoltTableUtil.format(evictResult));
        
        // Now execute a query that needs to access data from this block
        Procedure proc = this.getProcedure(procName); // Special Single-Stmt Proc
        ClientResponse cresponse = null;
        if (useLimit) {
            cresponse = this.client.callProcedure(proc.getName(), expected);
        } else {
            cresponse = this.client.callProcedure(proc.getName());
        }
        assertEquals(Status.OK, cresponse.getStatus());

        // Make sure that we tracked that we tried to touch evicted data
        assertEquals(1, this.profiler.evictedaccess_history.size());

        // And then check to make sure that we get the correct number of rows back
        VoltTable results[] = cresponse.getResults();
        System.err.println(VoltTableUtil.format(results[0]));
        assertEquals(1, results.length);
        assertEquals(expected, results[0].getRowCount());
    }
    
    private void scanWithParams(String  procName, int expected) throws Exception {
        assert(expected < NUM_TUPLES);
        this.loadData();
        
        // We should have all of our tuples evicted
        VoltTable evictResult = this.evictData();
        long evicted = evictResult.getLong("ANTICACHE_TUPLES_EVICTED");
        assertTrue("No tuples were evicted!\n"+evictResult, evicted > 0);
        // System.err.println(VoltTableUtil.format(evictResult));
        
        // Now execute a query that needs to access data from this block
        Procedure proc = this.getProcedure(procName); // Special Single-Stmt Proc
        Object params[] = { 1, 1, 1, expected };
        ClientResponse cresponse = this.client.callProcedure(proc.getName(), params);
        assertEquals(Status.OK, cresponse.getStatus());

        // Make sure that we tracked that we tried to touch evicted data
        assertEquals(1, this.profiler.evictedaccess_history.size());

        // And then check to make sure that we get the correct number of rows back
        VoltTable results[] = cresponse.getResults();
        System.err.println(VoltTableUtil.format(results[0]));
        assertEquals(1, results.length);
        assertEquals(expected, results[0].getRowCount());
    }
    
    private void validateStmt(String procName, Class<? extends AbstractPlanNode> target) throws Exception {
        // Just make sure that our target query plan contains the proper plan node.
        // This in case somebody changes something and we don't actually
        // execute a query with the expected query plan
        Procedure proc = this.getProcedure(procName);
        Statement stmt = CollectionUtil.first(proc.getStatements());
        assertNotNull(stmt);
        
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(stmt, true);
        assertNotNull(root);
        Collection<?> scans = PlanNodeUtil.getPlanNodes(root, target);
        assertEquals(1, scans.size());
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testSeqScanPlanValidate
     */
    @Test
    public void testSeqScanPlanValidate() throws Exception {
        this.validateStmt(SEQSCAN_PROCEDURE, SeqScanPlanNode.class);
    }
    
    /**
     * testIndexScanPlanValidate
     */
    @Test
    public void testIndexScanPlanValidate() throws Exception {
        this.validateStmt(INDEXSCAN_PROCEDURE, IndexScanPlanNode.class);
    }

    /**
     * testSeqScanReadOneEvictedTuple
     */
    @Test
    public void testSeqScanNotLimit() throws Exception {
        this.simpleScan(SEQSCAN_PROCEDURE, NUM_TUPLES, false);
    }
    
    /**
     * testSeqScanReadOneEvictedTuple
     */
    @Test
    public void testSeqScanReadOneEvictedTuple() throws Exception {
        this.simpleScan(SEQSCANLIMIT_PROCEDURE, 1, true);
    }
    
    /**
     * testSeqScanReadMultipleEvictedTuples
     */
    @Test
    public void testSeqScanReadMultipleEvictedTuples() throws Exception {
        this.simpleScan(SEQSCANLIMIT_PROCEDURE, 19, true); // Pick a screwy number
    }
        
    /**
     * testIndexScanReadOneEvictedTuple
     */
    @Test
    public void testIndexScanReadOneEvictedTuple() throws Exception {
        this.scanWithParams(INDEXSCAN_PROCEDURE, 1);
    }
    
    /**
     * testIndexScanReadMultipleEvictedTuples
     */
    @Test
    public void testIndexScanReadMultipleEvictedTuples() throws Exception {
        this.scanWithParams(INDEXSCAN_PROCEDURE, 13);
    }
    
    /**
     * testEvictTuples
     */
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
            if (col == "ANTICACHE_BLOCKS_EVICTED") {
                assertEquals(col, 1, results[0].getLong(col));
            } else {
                assertNotSame(col, 0, results[0].getLong(col));
            }
        } // FOR
    }
    
    /**
     * testMultipleEvictions
     */
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

    /**
     * testReadNonExistentBlock
     */
    @Test
    public void testReadNonExistentBlock() throws Exception {
        int block_ids[] = new int[]{ 1111 };
        int offsets[] = new int[]{0};
        boolean failed = false;
        try {
            ee.antiCacheReadBlocks(catalog_tbl, block_ids, offsets);
        } catch (UnknownBlockAccessException ex) {
            // This is what we want!
            assertEquals(block_ids[0], ex.getBlockId());
            failed = true;
            System.err.println(ex);
        }
        assertTrue(failed);
    }

}
