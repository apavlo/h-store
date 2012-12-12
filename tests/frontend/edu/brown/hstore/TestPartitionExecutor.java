/**
 * 
 */
package edu.brown.hstore;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.voltdb.DependencySet;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.VoltTypeUtil;

import com.google.protobuf.ByteString;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 *
 */
public class TestPartitionExecutor extends BaseTestCase {

    private static final int NUM_PARTITONS = 10;
    private static final int PARTITION_ID = 1;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = GetAccessData.class;
    
    private HStoreSite hstore_site;
    private Client client;
    private PartitionExecutor executor;
    
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
        
        HStoreConf hstore_conf = HStoreConf.singleton();
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_site = this.createHStoreSite(catalog_site, hstore_conf);
        this.client = createClient();
        this.executor = hstore_site.getPartitionExecutor(PARTITION_ID);
        assertNotNull(this.executor);
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
     * testGetVoltProcedure
     */
    @Test
    public void testGetVoltProcedure() {
        VoltProcedure volt_proc0 = executor.getVoltProcedure(TARGET_PROCEDURE.getSimpleName());
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
            VoltProcedure volt_proc = executor.getVoltProcedure(TARGET_PROCEDURE.getSimpleName());
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
