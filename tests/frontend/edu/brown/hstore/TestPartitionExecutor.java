/**
 * 
 */
package edu.brown.hstore;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.voltdb.DependencySet;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTypeUtil;

import com.google.protobuf.ByteString;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.profilers.PartitionExecutorProfiler;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

/**
 * Partition Executor Tests
 * @author pavlo
 */
public class TestPartitionExecutor extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = GetAccessData.class;
    private static final int NUM_PARTITONS = 10;
    private static final int PARTITION_ID = 1;
    private static final int NOTIFY_TIMEOUT = 2500; // ms
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;
    private PartitionExecutor executor;
    private PartitionExecutor.Debug executorDebug;
    private Procedure catalog_proc;
    
    private final Random rand = new Random(1); 

    protected class BlockingObserver extends EventObserver<ClientResponse> {
        public final LinkedBlockingDeque<ClientResponse> lock = new LinkedBlockingDeque<ClientResponse>(1);
        
        @Override
        public void update(EventObservable<ClientResponse> o, ClientResponse response) {
            assert(response != null);
            Logger.getRootLogger().info("BlockingObserver got update for txn #" + response.getTransactionId());
            lock.offer(response);
        }
        
        public ClientResponse poll() throws Exception {
            return (lock.poll(10, TimeUnit.SECONDS));
        }
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITONS);
        
        this.hstore_conf = HStoreConf.singleton();
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_site = this.createHStoreSite(catalog_site, hstore_conf);
        this.client = createClient();
        this.executor = hstore_site.getPartitionExecutor(PARTITION_ID);
        this.executorDebug = this.executor.getDebugContext();
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testUpdateMemoryStats
     */
    public void testUpdateMemoryStats() throws Exception {
        executorDebug.updateMemory();
    }
    
    /**
     * testProfiling
     */
    @Test
    public void testProfiling() throws Exception {
        hstore_conf.site.exec_profiling = true;
        hstore_conf.site.exec_force_allpartitions = true;
        
        int num_txns = 10;
        LatchableProcedureCallback callback = new LatchableProcedureCallback(num_txns);
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        for (int i = 0; i < num_txns; i++) {
            Object params[] = { i, Integer.toString(i) };
            boolean queued = this.client.callProcedure(callback, catalog_proc.getName(), params);
            assertTrue(queued);
        } // FOR
        
        // Wait until they all finish, then build a histogram of their base partitions
        boolean result = callback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SP LATCH --> " + callback.latch, result);
        assertEquals(num_txns, callback.responses.size());
        Histogram<Integer> basePartitions = new ObjectHistogram<Integer>(); 
        for (ClientResponse cresponse : callback.responses) {
            assertEquals(Status.OK, cresponse.getStatus());
            basePartitions.put(cresponse.getBasePartition());
        } // FOR
        
        // Now invoke the @Statistics sysproc to get back what we want
        catalog_proc = this.getProcedure(Statistics.class);
        Object params[] = { SysProcSelector.EXECPROFILER.name(), 0 };
        ClientResponse cresponse = this.client.callProcedure(catalog_proc.getName(), params);
        assertEquals(Status.OK, cresponse.getStatus());
        assertEquals(1, cresponse.getResults().length);
        basePartitions.put(cresponse.getBasePartition());
        
        // Examine the stats output. 
        VoltTable vt = cresponse.getResults()[0];
        System.err.println(VoltTableUtil.format(vt));
        assertEquals(NUM_PARTITONS, vt.getRowCount());
        PartitionExecutorProfiler profiler = this.executor.getDebugContext().getProfiler();
        assertNotNull(profiler);
        while (vt.advanceRow()) {
            int partition = (int)vt.getLong("PARTITION");
            assertTrue(partition >= 0);
            assertTrue(partition < NUM_PARTITONS);
            
            // We expect the following things:
            //  (1) Each partition should have waited for dtxn info the same # of txns 
            //      that we executed in the entire cluster
            //  (2) Each partition should have waited for 2PC for the same # of txns
            //      that we executed on it.
            Map<String, Long> expected = new HashMap<String, Long>();
            expected.put("TRANSACTIONS", basePartitions.get(partition, 0l));
            expected.put("NETWORK_CNT", Math.min(basePartitions.get(partition, 0l), 1));
            expected.put(profiler.sp3_local_time.getName()+"_CNT",
                         Math.min(basePartitions.get(partition, 0l), 1));
            
            for (String colName : expected.keySet()) {
                assertTrue(colName, vt.hasColumn(colName));
                long val = vt.getLong(colName);
                assertEquals(partition + " - " + colName, expected.get(colName).longValue(), val);
            } // FOR
        } // WHILE
    }
    
    /**
     * testGetVoltProcedure
     */
    @Test
    public void testGetVoltProcedure() {
        VoltProcedure volt_proc0 = executor.getVoltProcedure(catalog_proc.getId());
        assertNotNull(volt_proc0);
    }
    
    /**
     * testMultipleGetVoltProcedure
     */
    @Test
    public void testMultipleGetVoltProcedure() {
        // Invoke getVoltProcedure() multiple times and make sure that we never get back the same handle
        int count = 10;
        Set<VoltProcedure> procs = new HashSet<VoltProcedure>();
        for (int i = 0; i < count; i++) {
            VoltProcedure volt_proc = executor.getVoltProcedure(catalog_proc.getId());
            assertNotNull(volt_proc);
            assertFalse(procs.contains(volt_proc));
            procs.add(volt_proc);
        } // FOR
        assertEquals(count, procs.size());
    }
    
    /**
     * testBuildPartitionResult
     */
    public void testBuildPartitionResult() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SPECIAL_FACILITY); 
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        int num_rows = 50;
        for (int i = 0; i < num_rows; i++) {
            Object row[] = new Object[catalog_tbl.getColumns().size()];
            for (int j = 0; j < row.length; j++) {
                VoltType vtype = VoltType.get(catalog_tbl.getColumns().get(j).getType());
                row[j] = VoltTypeUtil.getRandomValue(vtype, rand);
            } // FOR
            vt.addRow(row);
        } // FOR
        
        int dep_id = 10001;
        DependencySet result = new DependencySet(new int[]{ dep_id }, new VoltTable[]{ vt });
        
        RemoteTransaction ts = new RemoteTransaction(hstore_site);
        WorkResult partitionResult = executor.buildWorkResult(ts, result, Status.OK, null);
        assertNotNull(partitionResult);
        assertEquals(result.size(), partitionResult.getDepDataCount());
        
        assertEquals(1, partitionResult.getDepDataCount());
        for (int i = 0; i < partitionResult.getDepDataCount(); i++) {
            assertEquals(dep_id, partitionResult.getDepId(i));
            
            ByteString bs = partitionResult.getDepData(i);
            assertFalse(bs.isEmpty());
            System.err.println("SIZE: " + StringUtil.md5sum(bs.asReadOnlyByteBuffer()));
            
            byte serialized[] = bs.toByteArray();
            VoltTable clone = FastDeserializer.deserialize(serialized, VoltTable.class);
            assertNotNull(clone);
            assertEquals(vt.getRowCount(), clone.getRowCount());
            assertEquals(vt.getColumnCount(), clone.getColumnCount());
        } // FOR
        
    }
    
}
