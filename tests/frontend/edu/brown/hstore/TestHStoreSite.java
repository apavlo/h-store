package edu.brown.hstore;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.MockClientCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.pools.TypedObjectPool;
import edu.brown.pools.TypedPoolableObjectFactory;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

public class TestHStoreSite extends BaseTestCase {
    
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = GetNewDestination.class;
    private static final long CLIENT_HANDLE = 1l;
    private static final int NUM_PARTITIONS = 2;
    private static final int BASE_PARTITION = 0;
    
    private HStoreSite hstore_site;
    private HStoreSite.DebugContext debug;
    private HStoreConf hstore_conf;
    private Client client;

    private static final ParameterSet PARAMS = new ParameterSet(
        new Long(0), // S_ID
        new Long(1), // SF_TYPE
        new Long(2), // START_TIME
        new Long(3)  // END_TIME
    );

    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.pool_profiling = true;
        this.hstore_conf.site.status_enable = false;
        this.hstore_conf.site.anticache_enable = false;
        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.debug = this.hstore_site.getDebugContext();
        this.client = createClient();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    /**
     * testObjectPools
     */
    @Test
    public void testObjectPools() throws Exception {
        // Check to make sure that we reject a bunch of txns that all of our
        // handles end up back in the object pool. To do this, we first need
        // to set the PartitionExecutor's to reject all incoming txns
        hstore_conf.site.queue_incoming_max_per_partition = 1;
        hstore_conf.site.queue_incoming_release_factor = 0;
        hstore_conf.site.queue_incoming_increase = 0;
        hstore_conf.site.queue_incoming_increase_max = 0;
        hstore_site.updateConf(hstore_conf);
        
        // We need to get at least one ABORT_REJECT
        final int num_txns = 1000;
        final CountDownLatch latch = new CountDownLatch(num_txns);
        final AtomicInteger numAborts = new AtomicInteger(0);
        
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse cr) {
                if (cr.getStatus() == Status.ABORT_REJECT) {
                    numAborts.incrementAndGet();
                }
                latch.countDown();
            }
        };
        
        // Then blast out a bunch of txns that should all come back as rejected
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        Object params[] = { 1234l, "XXXX" };
        for (int i = 0; i < num_txns; i++) {
            this.client.callProcedure(callback, catalog_proc.getName(), params);
        } // FOR
        
        boolean result = latch.await(20, TimeUnit.SECONDS);
        assertTrue(result);
        assertNotSame(0, numAborts.get());
        
        // HACK: Wait a little to know that the periodic thread has attempted
        // to clean-up our deletable txn handles
        ThreadUtil.sleep(1000);
        System.err.println("InflightTxnCount: " + debug.getInflightTxnCount());
        System.err.println("DeletableTxnCount: " + debug.getDeletableTxnCount());
        assertEquals(0, debug.getInflightTxnCount());
        assertEquals(0, debug.getDeletableTxnCount());
        
        // Then check to make sure that there aren't any active objects in the
        // the various object pools
        Map<String, TypedObjectPool<?>[]> allPools = hstore_site.getObjectPools().getPartitionedPools(); 
        assertNotNull(allPools);
        assertFalse(allPools.isEmpty());
        for (String name : allPools.keySet()) {
            TypedObjectPool<?> pools[] = allPools.get(name);
            TypedPoolableObjectFactory<?> factory = null;
            assertNotNull(name, pools);
            assertNotSame(0, pools.length);
            for (int i = 0; i < pools.length; i++) {
                if (pools[i] == null) continue;
                String poolName = String.format("%s-%02d", name, i);  
                factory = (TypedPoolableObjectFactory<?>)pools[i].getFactory();
                assertTrue(poolName, factory.isCountingEnabled());
                
                System.err.println(poolName + ": " + pools[i].toString());
                assertEquals(poolName, 0, pools[i].getNumActive());
            } // FOR
        } // FOR
    }
    
    /**
     * testSendClientResponse
     */
//    @Test
//    public void testSendClientResponse() throws Exception {
//        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
//        PartitionSet predict_touchedPartitions = new PartitionSet(BASE_PARTITION);
//        boolean predict_readOnly = true;
//        boolean predict_canAbort = true;
//        
//        MockClientCallback callback = new MockClientCallback();
//        
//        LocalTransaction ts = new LocalTransaction(hstore_site);
//        ts.init(1000l, CLIENT_HANDLE, BASE_PARTITION,
//                predict_touchedPartitions, predict_readOnly, predict_canAbort,
//                catalog_proc, PARAMS, callback);
//        
//        ClientResponseImpl cresponse = new ClientResponseImpl(ts.getTransactionId(),
//                                                              ts.getClientHandle(),
//                                                              ts.getBasePartition(),
//                                                              Status.OK,
//                                                              HStoreConstants.EMPTY_RESULT,
//                                                              "");
//        hstore_site.responseSend(ts, cresponse);
//        
//        // Check to make sure our callback got the ClientResponse
//        // And just make sure that they're the same
//        assertEquals(callback, ts.getClientCallback());
//        ClientResponseImpl clone = callback.getResponse();
//        assertNotNull(clone);
//        assertEquals(cresponse.getTransactionId(), clone.getTransactionId());
//        assertEquals(cresponse.getClientHandle(), clone.getClientHandle());
//    }
    
//    @Test
//    public void testHStoreSite_AdHoc(){
//    	this.hstore_site.run();
//    	Client client = ClientFactory.createClient();
//        try {
//			client.createConnection(null, "localhost", Client.VOLTDB_SERVER_PORT, "program", "password");
//		} catch (UnknownHostException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//        VoltTable result;
//		try {
//			result = client.callProcedure("@AdHoc", "SELECT * FROM NEW_ORDER;").getResults()[0];
//			assertTrue(result.getRowCount() == 1);
//	        System.out.println(result.toString());
//		} catch (NoConnectionsException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ProcCallException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//        
//    	
//    }
    
  
    
//    @Test
//    public void testSinglePartitionPassThrough() {
        // FIXME This won't work because the HStoreCoordinatorNode is now the thing that
        // actually fires off the txn in the ExecutionSite
        
//        StoreResultCallback<byte[]> done = new StoreResultCallback<byte[]>();
//        coordinator.procedureInvocation(invocation_bytes, done);
//
//        // Passed through to the mock coordinator
//        assertTrue(dtxnCoordinator.request.getLastFragment());
//        assertEquals(1, dtxnCoordinator.request.getTransactionId());
//        assertEquals(1, dtxnCoordinator.request.getFragmentCount());
//        assertEquals(0, dtxnCoordinator.request.getFragment(0).getPartitionId());
//        InitiateTaskMessage task = (InitiateTaskMessage) VoltMessage.createMessageFromBuffer(
//                dtxnCoordinator.request.getFragment(0).getWork().asReadOnlyByteBuffer(), true);
//        assertEquals(TARGET_PROCEDURE, task.getStoredProcedureName());
//        assertArrayEquals(PARAMS, task.getParameters());
//        assertEquals(null, done.getResult());
//
//        // Return results
//        Dtxn.CoordinatorResponse.Builder response = CoordinatorResponse.newBuilder();
//        response.setTransactionId(0);
//        response.setStatus(Dtxn.FragmentResponse.Status.OK);
//        byte[] output = { 0x3, 0x2, 0x1 };
//        response.addResponse(CoordinatorResponse.PartitionResponse.newBuilder()
//                .setPartitionId(0).setOutput(ByteString.copyFrom(output)));
//        dtxnCoordinator.done.run(response.build());
//        assertArrayEquals(output, done.getResult());
//    }
}
