/**
 * 
 */
package edu.brown.hstore;

import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.VoltTypeUtil;

import com.google.protobuf.ByteString;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.RemoteTransaction;

/**
 * @author pavlo
 *
 */
public class TestPartitionExecutor extends BaseTestCase {

    private static final int NUM_PARTITONS = 10;
    private static final int PARTITION_ID = 1;
    private static final int CLIENT_HANDLE = 1001;
    private static final long LAST_SAFE_TXN = -1;

    private static final String TARGET_PROCEDURE = "GetAccessData";
    private static final Object TARGET_PARAMS[] = new Object[] { new Long(1), new Long(1) };
    
    private HStoreSite hstore_site;
    private PartitionExecutor executor;
    
    private final Random rand = new Random(1); 
    
//    private class MockCallback implements RpcCallback<Dtxn.FragmentResponse> {
//        @Override
//        public void run(FragmentResponse parameter) {
//            // Nothing!
//        }
//    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITONS);
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        HStoreConf hstore_conf = HStoreConf.singleton();
        hstore_site = new MockHStoreSite(catalog_site, hstore_conf);
        executor = hstore_site.getPartitionExecutor(PARTITION_ID);
        assertNotNull(executor);
    }
    
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
    
    /**
     * testGetProcedure
     */
    public void testGetProcedure() {
        VoltProcedure volt_proc0 = executor.getVoltProcedure(TARGET_PROCEDURE);
        assertNotNull(volt_proc0);
    }
    
    /**
     * testRunClientResponse
     */
//    public void testRunClientResponse() throws Exception {
//        Thread thread = new Thread(site);
//        thread.start();
//        
//        VoltProcedure volt_proc = site.getProcedure(TARGET_PROCEDURE);
//        assertNotNull(volt_proc);
//        assertFalse(site.proc_pool.get(TARGET_PROCEDURE).contains(volt_proc));
//        
//        // For now just check whether our VoltProcedure goes back in the pool
//        site.callback.update(null, new Pair<VoltProcedure, ClientResponse>(volt_proc, new ClientResponseImpl()));
//        
//        int tries = 5;
//        boolean found = false;
//        while (tries-- > 0) {
//            if (site.proc_pool.get(TARGET_PROCEDURE).contains(volt_proc)) {
//                found = true;
//                break;
//            }
//            Thread.sleep(1000);
//        } // WHILE
//        assertTrue(found);
//        
//        thread.interrupt();
//    }
    
    /**
     * testRunInitiateTaskMessage
     */
//    public void testRunInitiateTaskMessage() throws Exception {
//        Thread thread = new Thread(site);
//        thread.start();
//        
//        // Use this callback to attach to the VoltProcedure and get the ClientResponse
//        BlockingObserver observer = new BlockingObserver();
//        
//        // Create an InitiateTaskMessage and shove that to the ExecutionSite
//        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
//        invocation.setProcName(TARGET_PROCEDURE);
//        invocation.setParams(TARGET_PARAMS);
//        invocation.setClientHandle(CLIENT_HANDLE);
//        
//        Long txn_id = new Long(rand.nextInt());
//        InitiateTaskMessage init_task = new InitiateTaskMessage(PARTITION_ID, PARTITION_ID, txn_id, true, true, invocation, LAST_SAFE_TXN); 
//        site.doWork(init_task, new MockCallback());
//        
//        int tries = 10000;
//        boolean found = false;
//        while (tries-- > 0) {
//            VoltProcedure running_volt_proc = site.getRunningVoltProcedure(txn_id);
//            if (running_volt_proc != null) {
//                assertEquals(TARGET_PROCEDURE, running_volt_proc.getProcedureName());
//                running_volt_proc.registerCallback(observer);
//                found = true;
//                break;
//            }
//            Thread.sleep(1);
//        } // WHILE
//        assertTrue(found);
//        
//        // Now check whether we got the ClientResponse
//        ClientResponse response = observer.poll();
//        assertNotNull(response);
//        assertEquals(1, response.getResults().length);
//        Logger.getRootLogger().info("Finished checking transaction information");
//        
//        thread.interrupt();
//        thread.join();
//    }
    
    /**
     * testMultipleTransactions
     */
//    public void testMultipleTransactions() throws Exception {
//        final Thread thread = new Thread(site);
//        thread.setPriority(Thread.MIN_PRIORITY);
//        thread.start();
//        
//        //
//        // Fire up a bunch of transactions and make sure that they don't get the same VoltProcedure
//        //
//        final int num_xacts = 4;
//        final InitiateTaskMessage tasks[] = new InitiateTaskMessage[num_xacts];
//        final BlockingObserver observers[] = new BlockingObserver[num_xacts];
//        final VoltProcedure volt_procs[] = new VoltProcedure[num_xacts];
//        final long xact_ids[] = new long[num_xacts];
//        final boolean found[] = new boolean[num_xacts];
//        final boolean started[] = new boolean[num_xacts];
//        Thread check_threads[] = new Thread[num_xacts];
//        for (int i = 0; i < num_xacts; i++) {
//            xact_ids[i] = rand.nextLong();
//            found[i] = false;
//            started[i] = false;
//            
//            // Peek in the VoltProcedure pool and register a callback
//            int ii = 0;
//            for (VoltProcedure volt_proc : site.proc_pool.get(TARGET_PROCEDURE)) {
//                volt_procs[i] = volt_proc;
//                if (ii++ == i) break; 
//            } // FOR
//            assertNotNull(volt_procs[i]);
//            observers[i] = new BlockingObserver();
//            volt_procs[i].registerCallback(observers[i]);
//        
//            // Create an InitiateTaskMessage and shove that to the ExecutionSite
//            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
//            invocation.setProcName(TARGET_PROCEDURE);
//            invocation.setParams(TARGET_PARAMS);
//            tasks[i] = new InitiateTaskMessage(PARTITION_ID, PARTITION_ID, xact_ids[i], CLIENT_HANDLE, true, true, invocation, LAST_SAFE_TXN);
//            
//            final int idx = i;
//            check_threads[i] = new Thread() {
//                public void run() {
//                    int tries = 1000;
//                    boolean done = false;
//                    started[idx] = true;
//                    site.doWork(tasks[idx]);
//                    while (tries-- > 0 && !done) {
//                        // Try to grab the running VoltProcedure handle
//                        VoltProcedure running_volt_proc = site.getRunningVoltProcedure(xact_ids[idx]);
//                        if (!found[idx] && running_volt_proc != null) {
//                            assert(volt_procs[idx] == running_volt_proc) : "Expected to get VoltProcedure " + volt_procs[idx] + " but got " + running_volt_proc;
//                            assertEquals(TARGET_PROCEDURE, running_volt_proc.getProcedureName());
//                            Logger.getRootLogger().info("Got running VoltProcedure handle for txn #" + xact_ids[idx] + " [" + idx + "]");
//                            found[idx] = true;
//                        }
//                        done = done && found[idx];
//                        if (!done) {
//                            try {
//                                Thread.sleep(1);
//                            } catch (InterruptedException ex) {
//                                return;
//                            }
//                        }
//                    } // WHILE
//                    return;
//                }
//            };
//        } // WHILE
//        for (Thread t : check_threads) {
//            t.start();
//            Thread.sleep(10);
//        }
//        for (Thread t : check_threads) t.join();
//        
//        
//        //
//        // Check to make sure that we got what we were looking for 
//        //
//        for (int i = 0; i < num_xacts; i++) {
//            assert(found[i]) : "Failed to running VoltProcedure for txn #" + xact_ids[i] + " [" + i + "]";
//            
//            // Make sure that it didn't execute with the same VoltProcedure
//            for (int ii = 0; ii < num_xacts; ii++) {
//                if (i == ii) continue;
//                assertTrue(volt_procs[i] != volt_procs[ii]);
//            } // FOR
//        
//            // Now check whether we got the ClientResponse
//            ClientResponse response = observers[i].poll();
//            assertNotNull(response);
//            assertEquals(1, response.getResults().length);
//        } // FOR
//        
//        thread.interrupt();
//        thread.join();
//    }
    
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
