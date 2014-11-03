package edu.brown.hstore;

import java.io.File;
import java.util.Random;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import org.voltdb.CatalogContext;
import org.voltdb.sysprocs.EvictTuples;
import org.voltdb.sysprocs.EvictHistory;
import org.voltdb.sysprocs.EvictedAccessHistory;
import org.voltdb.sysprocs.Statistics; 

import org.voltdb.VoltSystemProcedure;


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
import org.voltdb.SysProcSelector;

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

import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;


/**
 * Anti-Cache Manager Test Cases
 * @author pavlo
 */
public class TestAntiCachePerformance extends BaseTestCase {
    
    private static final int NOTIFY_TIMEOUT = 2000; // ms
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 1000;
    private static final int NUM_TRIALS = 1;
    private static final int NUM_KEYS = 32766; // MAX
    private static final String TARGET_TABLE = YCSBConstants.TABLE_NAME;
    
    private static final long BLOCK_SIZE_1_MB = 1048576;
    private static final long BLOCK_SIZE_2_MB = BLOCK_SIZE_1_MB * 2;
    private static final long BLOCK_SIZE_3_MB = BLOCK_SIZE_1_MB * 3;
    private static final long BLOCK_SIZE_4_MB = BLOCK_SIZE_1_MB * 4;
    private static final long BLOCK_SIZE_8_MB = BLOCK_SIZE_1_MB * 8;
    private static final long BLOCK_SIZE_16_MB = BLOCK_SIZE_1_MB * 16;

    
    private static final double ZIPF_CONSTANT = 1.5;
    
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
        this.hstore_conf.site.anticache_block_size = BLOCK_SIZE_2_MB;
        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.executor = hstore_site.getPartitionExecutor(0);
        assertNotNull(this.executor);
        this.ee = executor.getExecutionEngine();
        assertNotNull(this.executor);
        
        this.client = createClient();
    }
    

    private void loadData(int tuples) throws Exception {
        // Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        for (int i = 0; i < tuples; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i;
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000l, catalog_tbl, vt, false);
    }

    private Map<Integer, VoltTable> evictData(long block_size) throws Exception {

//        System.err.printf("Evicting data...");
        long start, stop;

        String procName = VoltSystemProcedure.procCallName(EvictTuples.class);
        CatalogContext catalogContext = this.getCatalogContext();
        String tableNames[] = { TARGET_TABLE };
        LatchableProcedureCallback callback = new LatchableProcedureCallback(catalogContext.numberOfPartitions);

        long evictBytes[] = {block_size};
        int numBlocks[] = { 1 };

        start = System.currentTimeMillis();  // start timer

        for (int partition : catalogContext.getAllPartitionIds()) {
//            System.err.printf("Evicting data at partition %d...\n", partition);
            Object params[] = { partition, tableNames, null, evictBytes, numBlocks };
            boolean result = this.client.callProcedure(callback, procName, params);
            assertTrue(result);
        } // FOR

        // Wait until they all finish
        boolean result = callback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(callback.toString(), result);

        stop = System.currentTimeMillis();
        System.err.println("  Eviction time: " + (stop-start) + " ms");

        // Construct a mapping BasePartition->VoltTable
        Map<Integer, VoltTable> m = new TreeMap<Integer, VoltTable>();
        for (ClientResponse cr : callback.responses) {
            assertEquals(cr.toString(), Status.OK, cr.getStatus());
            assertEquals(cr.toString(), 1, cr.getResults().length);
            m.put(cr.getBasePartition(), cr.getResults()[0]);
        } // FOR
        assertEquals(catalogContext.numberOfPartitions, m.size());
        //        System.err.printf("Finished evicting data.");


        // Our stats should now come back with one eviction executed
        procName = VoltSystemProcedure.procCallName(EvictHistory.class);
        ClientResponse cresponse = client.callProcedure(procName);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
        VoltTable result_table = cresponse.getResults()[0];
        System.err.println(VoltTableUtil.format(result_table));

        return (m);
    }

    private void UnevictData() throws Exception {

        long start, stop;

        String procName = "ReadRecord";
        Object params[] = { 1 };

        start = System.currentTimeMillis();  // start timer

        ClientResponse cresponse = client.callProcedure(procName, params);

        stop = System.currentTimeMillis();
        System.err.println("  Uneviction Time: " + (stop-start) + " ms");

//        procName = VoltSystemProcedure.procCallName(EvictedAccessHistory.class);
//        cresponse = client.callProcedure(procName);
//        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
//        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
//        VoltTable result = cresponse.getResults()[0];
//        assertEquals(1, result.getRowCount());
//        System.err.println(VoltTableUtil.format(result));

        procName = VoltSystemProcedure.procCallName(Statistics.class);
        Object params2[] = { SysProcSelector.ANTICACHE.name(), 0 };
        cresponse = this.client.callProcedure(procName, params2);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
        VoltTable result = cresponse.getResults()[0];
        assertEquals(1, result.getRowCount());
        System.err.println(VoltTableUtil.format(result));
    }

    private void runSpeedTest() throws Exception {
        Random rand = new Random(0);
        double data[] = new double[NUM_TRIALS];
        for (int trial = 0; trial < NUM_TRIALS; trial++) {
            long keys[][] = new long[3][NUM_KEYS];
            int num_reads = 0;

            ZipfianGenerator zipf = new ZipfianGenerator(NUM_TUPLES, ZIPF_CONSTANT);

            for (int i = 0; i < keys.length; i++) {
                for (int j = 0; j < keys[i].length; j++) {
//                    keys[i][j] = rand.nextInt(NUM_TUPLES);
                      keys[i][j] = zipf.nextInt(NUM_TUPLES); 
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
         this.loadData(NUM_TUPLES);
         this.runSpeedTest();
     }

    /**
     * testWithAntiCache
     */
    @Test
    public void testWithAntiCache() throws Exception {
        this.startSite(true);
        this.loadData(NUM_TUPLES);
        this.runSpeedTest();
    }

    /**
     * testWithAntiCache
     */
    @Test
    public void testEvictData1() throws Exception {
        this.startSite(true);
        this.loadData(50000);
        System.err.println("Testing with 1 MB block size: ");
        this.evictData(BLOCK_SIZE_1_MB);
        this.UnevictData();
    }

    /**
     * testWithAntiCache
     */
    @Test
    public void testEvictData2() throws Exception {
        this.startSite(true);
        this.loadData(50000);
        System.err.println("Testing with 2 MB block size: ");
        this.evictData(BLOCK_SIZE_2_MB);
        this.UnevictData();
    }

//    /**
//     * testWithAntiCache
//     */
//    @Test
//        public void testEvictData3() throws Exception {
//        this.startSite(true);
//        this.loadData(50000);
//        System.err.println("Testing with 3 MB block size: ");
//        this.evictData(BLOCK_SIZE_3_MB);
//        this.UnevictData();
//    }

//    /**
//     * testWithAntiCache
//     */
//    @Test
//    public void testEvictData4() throws Exception {
//        this.startSite(true);
//        this.loadData(50000);
//        System.err.println("Testing with 4 MB block size: ");
//        this.evictData(BLOCK_SIZE_4_MB);
//        this.UnevictData();
//    }
//
//    /**
//     * testWithAntiCache
//     */
//    @Test
//    public void testEvictData5() throws Exception {
//        this.startSite(true);
//        this.loadData(50000);
//        System.err.println("Testing with 8 MB block size: ");
//        this.evictData(BLOCK_SIZE_8_MB);
//        this.UnevictData();
//    }
//
//    /**
//     * testWithAntiCache
//     */
//    @Test
//    public void testEvictData6() throws Exception {
//        this.startSite(true);
//        this.loadData(50000);
//        System.err.println("Testing with 16 MB block size: ");
//        this.evictData(BLOCK_SIZE_16_MB);
//        this.UnevictData();
//    }
}
