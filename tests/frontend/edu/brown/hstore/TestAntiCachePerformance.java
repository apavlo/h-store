package edu.brown.hstore;

import java.io.File;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
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
import edu.brown.utils.MathUtil;

/**
 * Anti-Cache Manager Test Cases
 * @author pavlo
 */
public class TestAntiCachePerformance extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 100000;
    private static final int NUM_TRIALS = 3;
    private static final int NUM_KEYS = 32766; // MAX
    private static final String TARGET_TABLE = YCSBConstants.TABLE_NAME;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private File anticache_dir;
    private Client client;
    
    private PartitionExecutor executor;
    @SuppressWarnings("unused")
    private ExecutionEngine ee;
    private Table catalog_tbl;
    @SuppressWarnings("unused")
    private int locators[];

    private final AbstractProjectBuilder builder = new YCSBProjectBuilder() {
        {
            this.markTableEvictable(TARGET_TABLE);
            this.addAllDefaults();
            this.addProcedure(SelectBlaster.class);
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
    
    private void startSite(boolean anticaching) throws Exception {
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.anticache_enable = anticaching;
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
    }

    private void runSpeedTest() throws Exception {
        Random rand = new Random(0);
        double data[] = new double[NUM_TRIALS];
        for (int trial = 0; trial < NUM_TRIALS; trial++) {
            long keys[][] = new long[3][NUM_KEYS];
            int num_reads = 0;
            for (int i = 0; i < keys.length; i++) {
                for (int j = 0; j < keys[i].length; j++) {
                    keys[i][j] = rand.nextInt(NUM_TUPLES);
                    num_reads++;
                } // FOR
            } // FOR
            
            Procedure proc = this.getProcedure(SelectBlaster.class);
            Object params[] = new Object[keys.length];
            for (int i = 0; i < keys.length; i++) {
                params[i] = keys[i];
            } // FOR
            ClientResponse cresponse = this.client.callProcedure(proc.getName(), params);
            assertEquals(Status.OK, cresponse.getStatus());
            
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            data[trial] = results[0].asScalarLong() / 1000000d;
            System.err.printf("  TRIAL %d: %.2f ms [# of Reads=%d]\n", trial, data[trial], num_reads); 
        } // FOR
        double avg = MathUtil.arithmeticMean(data);
        System.err.printf("Elapsed Time: %.2f ms\n", avg);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testWithoutAntiCache
     */
    @Test
    public void testWithoutAntiCache() throws Exception {
        this.startSite(false);
        this.loadData();
        this.runSpeedTest();
    }
    
    /**
     * testWithAntiCache
     */
    @Test
    public void testWithAntiCache() throws Exception {
        this.startSite(true);
        this.loadData();
        this.runSpeedTest();
    }
}
