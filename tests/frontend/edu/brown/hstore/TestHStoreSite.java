package edu.brown.hstore;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.hstore.HStoreSite;

import org.junit.Before;
import org.junit.Test;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.callbacks.MockClientCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;

import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.messaging.*;

import edu.brown.protorpc.StoreResultCallback;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class TestHStoreSite extends BaseTestCase {
    
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = GetNewDestination.class;
    private static final long CLIENT_HANDLE = 1l;
    private static final int NUM_PARTITIONS = 10;
    private static final int BASE_PARTITION = 0;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    
    private LocalTransaction ts;
    private StoredProcedureInvocation invocation;
    private MockClientCallback callback;

    private static final Object PARAMS[] = {
        new Long(0), // S_ID
        new Long(1), // SF_TYPE
        new Long(2), // START_TIME
        new Long(3), // END_TIME
    };

    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_site = new MockHStoreSite(catalog_site, hstore_conf);
        
        this.ts = new LocalTransaction(hstore_site);
        this.invocation = new StoredProcedureInvocation(CLIENT_HANDLE, catalog_proc.getName(), PARAMS);
        this.callback = new MockClientCallback();
        Collection<Integer> predict_touchedPartitions = Collections.singleton(BASE_PARTITION);
        boolean predict_readOnly = true;
        boolean predict_canAbort = true;
        
        
        ts.init(1000l, CLIENT_HANDLE, BASE_PARTITION,
                predict_touchedPartitions, predict_readOnly, predict_canAbort,
                catalog_proc, this.invocation, this.callback);
    }
    
    @Override
    protected void tearDown() throws Exception {
//        hstore_site.shutdown();
    }
    
    /**
     * testSendClientResponse
     */
    @Test
    public void testSendClientResponse() throws Exception {
        ClientResponseImpl cresponse = new ClientResponseImpl(ts.getTransactionId(),
                                                              ts.getClientHandle(),
                                                              ts.getBasePartition(),
                                                              Status.OK,
                                                              HStoreConstants.EMPTY_RESULT,
                                                              "");
        hstore_site.sendClientResponse(ts, cresponse);
        
        // Check to make sure our callback got the ClientResponse
        assertEquals(this.callback, ts.getClientCallback());
        byte serialized[] = this.callback.getResponse();
        assertNotNull(serialized);
        
        // And just make sure that they're the same
        ClientResponseImpl clone = FastDeserializer.deserialize(serialized, ClientResponseImpl.class);
        assertNotNull(clone);
        assertEquals(cresponse.getTransactionId(), clone.getTransactionId());
        assertEquals(cresponse.getClientHandle(), clone.getClientHandle());
    }
    
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
    
  
    
    @Test
    public void testSinglePartitionPassThrough() {
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
    }
}
